"""Step 3 - daily FX rates from frankfurter.dev into public.fx_rates.

Frankfurter republishes the ECB reference rates: EUR base, working days only,
no weekends and no TARGET holidays. The pipeline therefore always asks for a
date *range* (never a single day) and lets the SQL layer carry the last known
rate forward - see the FX rule in sql/03_marts.sql.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import psycopg
import requests

from .config import Settings
from .db import RunLog, fetch_one
from .http_client import get_json

log = logging.getLogger(__name__)

# Which currencies and which window do the orders actually need? Reading it from
# the data means a new currency in the source is picked up automatically.
_NEEDED_SQL = """
with refs as (
    select ops.safe_date(fx_reference_date) as ref_date,
           upper(btrim(currency))           as currency
      from public.orders_raw
)
select coalesce(min(ref_date), current_date)                              as min_ref_date,
       coalesce(array_agg(distinct currency) filter (where currency <> 'EUR'
                                                       and currency ~ '^[A-Z]{3}$'),
                array[]::text[])                                          as currencies
  from refs
"""


def _requirements(conn: psycopg.Connection, settings: Settings) -> tuple[date, list[str]]:
    row = fetch_one(conn, _NEEDED_SQL)
    if row is None:
        return date.today() - timedelta(days=settings.fx_lookback_buffer_days), []
    start = row["min_ref_date"] - timedelta(days=settings.fx_lookback_buffer_days)
    return start, sorted(row["currencies"])


def fetch_rates(
    session: requests.Session,
    settings: Settings,
    start: date,
    end: date,
    currencies: list[str],
) -> dict[str, dict[str, Any]]:
    """Return ``{"2026-08-24": {"RON": 5.2504}, ...}`` for the requested window."""
    payload, _ = get_json(
        session,
        f"{settings.fx_api_base}/{start.isoformat()}..{end.isoformat()}",
        params={"base": settings.fx_base_currency, "symbols": ",".join(currencies)},
        timeout=settings.http_timeout_seconds,
        max_retries=settings.http_max_retries,
    )
    rates = payload.get("rates") or {}
    if not rates:
        raise RuntimeError(f"no FX rates returned for {start}..{end} ({', '.join(currencies)})")
    return rates


def ingest(conn: psycopg.Connection, settings: Settings) -> int:
    """Upsert every rate in the required window. Returns the number of rows written."""
    with RunLog(conn, "ingest_fx", settings.run_trigger) as run, requests.Session() as session:
        start, currencies = _requirements(conn, settings)
        if not currencies:
            log.info("no non-EUR currency in the orders - nothing to fetch")
            run.rows_out = 0
            run.message = "no non-EUR currency in the source data"
            return 0

        # Rates only exist up to today; reference dates in the future are served
        # by carrying the latest rate forward, and self-correct on a later run.
        end = date.today()
        log.info("fetching %s from %s to %s", ",".join(currencies), start, end)
        rates = fetch_rates(session, settings, start, end, currencies)

        rows = [
            (rate_date, settings.fx_base_currency, quote, value)
            for rate_date, quotes in rates.items()
            for quote, value in quotes.items()
        ]

        with conn.cursor() as cur:
            cur.executemany(
                """
                insert into public.fx_rates (rate_date, base_currency, quote_currency, rate, source, fetched_at)
                values (%s, %s, %s, %s, 'frankfurter.dev', now())
                on conflict (rate_date, base_currency, quote_currency)
                do update set rate = excluded.rate, fetched_at = excluded.fetched_at
                """,
                rows,
            )
        conn.commit()

        run.rows_in = len(rates)
        run.rows_out = len(rows)
        run.message = f"{len(rows)} rates for {', '.join(currencies)} covering {start}..{end}"
        log.info("stored %s rates", len(rows))

    return len(rows)
