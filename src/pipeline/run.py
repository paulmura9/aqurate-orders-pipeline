"""Command line entry point.

    python -m pipeline.run setup      # create/refresh schema, functions, views
    python -m pipeline.run ingest     # step 1: orders_raw
    python -m pipeline.run fx         # step 3: fx_rates
    python -m pipeline.run transform  # steps 2, 4, 5 + quality gate
    python -m pipeline.run all        # everything, in order (used by the scheduler)
    python -m pipeline.run profile    # data profiling report for the write-up
    python -m pipeline.run report     # what the marts currently contain
    python -m pipeline.run health     # last run per step + open quality issues
"""

from __future__ import annotations

import argparse
import logging
import sys

import requests

from . import ingest_fx, ingest_orders, profile_raw, transform
from .config import Settings
from .db import apply_sql_file, connect, fetch_all

log = logging.getLogger("pipeline")


def _setup(conn, settings: Settings) -> None:
    from .config import SQL_DIR

    for name in settings.sql_files:
        apply_sql_file(conn, SQL_DIR / name)
    conn.commit()
    log.info("schema and functions are up to date")


def _health(conn) -> str:
    lines = ["pipeline health", "=" * 78]
    for row in fetch_all(conn, "select * from ops.pipeline_health order by step"):
        flag = "!!" if row["needs_attention"] else "ok"
        lines.append(
            "[{flag}] {step:<16} {status:<8} {hours_since:>6}h ago  rows_out={rows_out}  {message}".format(
                flag=flag, **row
            )
        )
    issues = fetch_all(conn, "select * from ops.latest_quality_issues")
    lines.append("")
    lines.append("open quality issues (48h): " + (str(len(issues)) if issues else "none"))
    for issue in issues:
        lines.append(
            "  [{severity}] {check_name}: observed={observed} threshold={threshold} - {details}".format(**issue)
        )
    return "\n".join(lines)


def _heartbeat(settings: Settings) -> None:
    """Ping the dead man's switch - only reached when the whole run succeeded."""
    if not settings.heartbeat_url:
        return
    try:
        requests.get(settings.heartbeat_url, timeout=10).raise_for_status()
        log.info("heartbeat sent")
    except requests.RequestException as exc:  # never fail the run over the ping
        log.warning("heartbeat ping failed: %s", exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Aqurate orders pipeline")
    parser.add_argument(
        "command",
        choices=["setup", "ingest", "fx", "transform", "all", "profile", "report", "health"],
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    settings = Settings.from_env()

    with connect(settings.database_url) as conn:
        if args.command == "setup":
            _setup(conn, settings)
        elif args.command == "ingest":
            ingest_orders.ingest(conn, settings)
        elif args.command == "fx":
            ingest_fx.ingest(conn, settings)
        elif args.command == "transform":
            transform.run(conn, settings)
            print(transform.summary(conn))
        elif args.command == "all":
            _setup(conn, settings)
            ingest_orders.ingest(conn, settings)
            ingest_fx.ingest(conn, settings)
            transform.run(conn, settings)
            print(transform.summary(conn))
            _heartbeat(settings)
        elif args.command == "profile":
            print(profile_raw.report(conn))
        elif args.command == "report":
            print(transform.summary(conn))
        elif args.command == "health":
            print(_health(conn))

    return 0


if __name__ == "__main__":
    sys.exit(main())
