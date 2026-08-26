"""Steps 2, 4 and 5 - run the SQL transformations and the quality gate.

All the business logic lives in SQL (sql/02..04); this module only calls
``ops.run_daily()``, which rebuilds orders_clean, refreshes both marts, records
the quality checks and raises if an error-level check fails.
"""

from __future__ import annotations

import logging

import psycopg

from .config import Settings
from .db import fetch_all, fetch_one

log = logging.getLogger(__name__)


def run(conn: psycopg.Connection, settings: Settings) -> int:
    """Rebuild the clean layer and both marts. Returns the pipeline run_id."""
    try:
        row = fetch_one(conn, "select ops.run_daily(%s) as run_id", (settings.run_trigger,))
        conn.commit()
    except psycopg.errors.RaiseException as exc:
        conn.rollback()
        for issue in failed_checks(conn):
            log.error(
                "quality check failed: %s (observed=%s threshold=%s) - %s",
                issue["check_name"], issue["observed"], issue["threshold"], issue["details"],
            )
        raise RuntimeError(str(exc).strip()) from exc

    run_id = row["run_id"] if row else -1
    summary = fetch_one(conn, "select message from ops.pipeline_run where run_id = %s", (run_id,))
    log.info("transform finished (run_id=%s): %s", run_id, summary["message"] if summary else "")
    for issue in failed_checks(conn):
        log.warning(
            "quality warning: %s (observed=%s threshold=%s) - %s",
            issue["check_name"], issue["observed"], issue["threshold"], issue["details"],
        )
    return run_id


def failed_checks(conn: psycopg.Connection) -> list[dict]:
    return fetch_all(conn, "select * from ops.latest_quality_issues")


def summary(conn: psycopg.Connection) -> str:
    """A short human-readable report of what the marts currently contain."""
    lines: list[str] = []

    totals = fetch_one(
        conn,
        """
        select (select count(*) from public.orders_raw)                    as raw_rows,
               (select count(*) from public.orders_clean)                  as clean_rows,
               (select count(*) from public.orders_rejected)               as rejected_rows,
               (select count(*) from public.fx_rates)                      as fx_rows,
               (select count(*) from public.customer_spend_eur)            as customers,
               (select round(sum(total_spend_eur), 2) from public.customer_spend_eur) as total_eur
        """,
    )
    if totals:
        lines.append(
            "orders_raw={raw_rows}  orders_clean={clean_rows}  rejected={rejected_rows}  "
            "fx_rates={fx_rows}  customers={customers}  total_spend_eur={total_eur}".format(**totals)
        )

    lines.append("\nrejections by reason:")
    for row in fetch_all(
        conn,
        "select reason, count(*) as n from public.orders_rejected group by 1 order by 2 desc",
    ):
        lines.append(f"  {row['reason']:<28} {row['n']:>6}")

    lines.append("\nrevenue_by_country_category (Books + Electronics, > threshold):")
    for row in fetch_all(
        conn,
        """
        select revenue_rank, country, revenue_eur, books_revenue_eur, electronics_revenue_eur, orders_count
          from public.revenue_by_country_category
         order by revenue_rank
        """,
    ):
        lines.append(
            "  #{revenue_rank} {country}  {revenue_eur:>12} EUR  "
            "(books {books_revenue_eur}, electronics {electronics_revenue_eur}, "
            "{orders_count} orders)".format(**row)
        )

    lines.append("\ntop customers by spend:")
    for row in fetch_all(
        conn,
        """
        select customer_id, customer_email, country, total_spend_eur, orders_count
          from public.customer_spend_eur
         order by total_spend_eur desc
         limit 5
        """,
    ):
        lines.append(
            "  {customer_id:>6} {customer_email:<32} {country}  "
            "{total_spend_eur:>10} EUR  ({orders_count} orders)".format(**row)
        )

    return "\n".join(lines)
