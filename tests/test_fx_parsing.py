"""The FX loader flattens Frankfurter's nested response into fx_rates rows."""

from __future__ import annotations

from datetime import date

import pytest

from pipeline.config import Settings
from pipeline.ingest_fx import fetch_rates


class FakeFrankfurter:
    payload = {
        "amount": 1.0,
        "base": "EUR",
        "start_date": "2026-08-21",
        "end_date": "2026-08-26",
        "rates": {
            "2026-08-21": {"RON": 5.2563},
            "2026-08-24": {"RON": 5.2504},
            "2026-08-25": {"RON": 5.2537},
            "2026-08-26": {"RON": 5.2568},
        },
    }

    def get(self, url, params=None, headers=None, timeout=None): 
        return _FakeResponse(self.payload)


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.status_code = 200
        self.headers = {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def _settings() -> Settings:
    return Settings(
        database_url="postgresql://unused",
        orders_url="https://example.invalid/orders_raw",
        orders_api_key="key",
    )


def test_weekend_gaps_are_preserved_not_invented() -> None:
    rates = fetch_rates(FakeFrankfurter(), _settings(), date(2026, 8, 21), date(2026, 8, 26), ["RON"])

    assert set(rates) == {"2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26"}
    assert "2026-08-22" not in rates


def test_empty_response_is_an_error() -> None:
    class Empty(FakeFrankfurter):
        payload = {"rates": {}}

    with pytest.raises(RuntimeError):
        fetch_rates(Empty(), _settings(), date(2026, 8, 21), date(2026, 8, 26), ["RON"])
