"""Step 1 - pull orders_raw from the REST endpoint into Postgres.

Two things make this more than a single GET:

* the endpoint pages at 1000 rows, and ``order_id`` is **not** unique (an order
  has one row per SKU, and the source also contains duplicated lines), so
  ``limit/offset`` paging can drop or repeat rows when a page boundary falls in
  the middle of an order.  Keyset paging on ``order_id`` plus trimming the
  partial group at the end of each page is stable regardless of duplicates.
* the load is verified against the row count reported by PostgREST, so a
  half-downloaded snapshot fails the run instead of silently shrinking the marts.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import requests
from psycopg.types.json import Json

from .config import Settings
from .db import RunLog
from .http_client import get_json

log = logging.getLogger(__name__)


def _headers(settings: Settings) -> dict[str, str]:
    return {"apikey": settings.orders_api_key, "Accept": "application/json"}


def fetch_expected_count(session: requests.Session, settings: Settings) -> int:
    """Ask PostgREST how many rows the source table has (Content-Range header)."""
    _, headers = get_json(
        session,
        settings.orders_url,
        params={"select": "order_id", "limit": 1},
        headers={**_headers(settings), "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"},
        timeout=settings.http_timeout_seconds,
        max_retries=settings.http_max_retries,
    )
    content_range = headers.get("content-range", "")
    total = content_range.split("/")[-1]
    if not total.isdigit():
        raise RuntimeError(f"could not read a row count from Content-Range: {content_range!r}")
    return int(total)


def iter_pages(session: requests.Session, settings: Settings) -> Iterator[list[dict[str, Any]]]:
    """Yield pages of source rows using keyset pagination on order_id."""
    cursor: str | None = None

    while True:
        params: dict[str, Any] = {"select": "*", "order": "order_id.asc", "limit": settings.page_size}
        if cursor is not None:
            params["order_id"] = f"gt.{cursor}"

        rows, _ = get_json(
            session,
            settings.orders_url,
            params=params,
            headers=_headers(settings),
            timeout=settings.http_timeout_seconds,
            max_retries=settings.http_max_retries,
        )
        if not rows:
            return

        if len(rows) < settings.page_size:  # last page
            yield rows
            return

        last_id = rows[-1]["order_id"]
        complete = [row for row in rows if row["order_id"] != last_id]
        if complete:
            yield complete
            cursor = complete[-1]["order_id"]
        else:
            # A single order_id filled the whole page: the rest of that order
            # would be skipped, so fail instead of silently losing lines.
            raise RuntimeError(
                f"order_id {last_id!r} has at least {settings.page_size} lines - "
                "increase PAGE_SIZE so a page can never hold a single order"
            )


def ingest(conn: psycopg.Connection, settings: Settings) -> int:
    """Replace orders_raw with a fresh snapshot. Returns the number of rows."""
    batch_id = uuid.uuid4()

    with RunLog(conn, "ingest_orders", settings.run_trigger) as run, requests.Session() as session:
        expected = fetch_expected_count(session, settings)
        run.rows_in = expected
        log.info("source reports %s rows", expected)

        loaded = 0
        with conn.cursor() as cur:
            # The source is a full snapshot, so the landing zone is replaced
            # atomically: readers never see a half-loaded table.
            cur.execute("delete from public.orders_raw")
            for page in iter_pages(session, settings):
                cur.executemany(
                    "insert into public.orders_raw (batch_id, payload) values (%s, %s)",
                    [(batch_id, Json(row)) for row in page],
                )
                loaded += len(page)
                log.info("loaded %s/%s rows", loaded, expected)

        if loaded != expected:
            raise RuntimeError(
                f"incomplete ingest: loaded {loaded} rows but the source reports {expected}"
            )

        conn.commit()
        run.rows_out = loaded
        run.message = f"batch {batch_id} - {loaded} rows"

    return loaded
