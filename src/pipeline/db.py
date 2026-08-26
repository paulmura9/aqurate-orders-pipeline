"""Database access helpers: connections, SQL files and the run log."""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger(__name__)


@contextlib.contextmanager
def connect(database_url: str) -> Iterator[psycopg.Connection]:
    """Open a connection with autocommit off - each step is one transaction."""
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        yield conn


def apply_sql_file(conn: psycopg.Connection, path: Path) -> None:
    """Apply one migration file. The files are written to be idempotent."""
    log.info("applying %s", path.name)
    with conn.cursor() as cur:
        cur.execute(path.read_text(encoding="utf-8"))  # type: ignore[arg-type]


def fetch_one(conn: psycopg.Connection, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(sql, params)  # type: ignore[arg-type]
        return cur.fetchone()


def fetch_all(conn: psycopg.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, params)  # type: ignore[arg-type]
        return cur.fetchall()


class RunLog:
    """Writes one row per pipeline step into ops.pipeline_run.

    Used as a context manager so a crash is recorded as a failed run instead of
    leaving the previous 'success' row as the newest state - a silent failure is
    exactly what the monitoring has to catch.
    """

    def __init__(self, conn: psycopg.Connection, step: str, triggered_by: str = "manual") -> None:
        self.conn = conn
        self.step = step
        self.triggered_by = triggered_by
        self.run_id: int | None = None
        self.rows_in: int | None = None
        self.rows_out: int | None = None
        self.message: str | None = None
        self._started = 0.0

    def __enter__(self) -> "RunLog":
        self._started = time.monotonic()
        row = fetch_one(
            self.conn,
            """
            insert into ops.pipeline_run (step, status, triggered_by)
            values (%s, 'running', %s)
            returning run_id
            """,
            (self.step, self.triggered_by),
        )
        self.run_id = row["run_id"] if row else None
        self.conn.commit()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        duration_ms = int((time.monotonic() - self._started) * 1000)
        status = "success" if exc_type is None else "failed"
        message = self.message if exc_type is None else f"{exc_type.__name__}: {exc}"
        if exc_type is not None:
            self.conn.rollback()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                update ops.pipeline_run
                   set status = %s, finished_at = now(), duration_ms = %s,
                       rows_in = %s, rows_out = %s, message = %s
                 where run_id = %s
                """,
                (status, duration_ms, self.rows_in, self.rows_out, message, self.run_id),
            )
        self.conn.commit()
        log.info("step=%s status=%s duration_ms=%s rows_out=%s", self.step, status, duration_ms, self.rows_out)
        return False  # never swallow the exception
