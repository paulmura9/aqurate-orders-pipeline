"""Configuration, read once from the environment.

Everything the pipeline needs is an environment variable so the same code runs
locally (.env), in GitHub Actions (secrets) or in any other scheduler.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:  
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:  
    pass

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = PROJECT_ROOT / "sql"

SQL_FILES = (
    "01_schema.sql",
    "02_orders_clean.sql",
    "03_marts.sql",
    "04_quality_checks.sql",
)


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"Environment variable {name} is not set. Copy .env.example to .env and fill it in."
        )
    return value


@dataclass(frozen=True)
class Settings:
    """Runtime settings for one invocation of the pipeline."""

    database_url: str
    orders_url: str
    orders_api_key: str
    fx_api_base: str = "https://api.frankfurter.dev/v1"
    fx_base_currency: str = "EUR"
    fx_lookback_buffer_days: int = 10
    page_size: int = 1000
    http_timeout_seconds: int = 30
    http_max_retries: int = 5
    heartbeat_url: str | None = None
    run_trigger: str = "manual"
    sql_files: tuple[str, ...] = field(default=SQL_FILES)

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=_require("DATABASE_URL"),
            orders_url=(
                os.environ.get("ORDERS_SOURCE_URL")
                or "https://jzozteoirwfczccltcdr.supabase.co/rest/v1/orders_raw"
            ).strip(),
            orders_api_key=_require("ORDERS_SOURCE_API_KEY"),
            fx_api_base=(os.environ.get("FX_API_BASE") or "https://api.frankfurter.dev/v1").strip(),
            fx_lookback_buffer_days=int(os.environ.get("FX_LOOKBACK_BUFFER_DAYS") or 10),
            page_size=int(os.environ.get("PAGE_SIZE") or 1000),
            heartbeat_url=os.environ.get("HEARTBEAT_URL") or None,
            run_trigger=os.environ.get("RUN_TRIGGER") or "manual",
        )
