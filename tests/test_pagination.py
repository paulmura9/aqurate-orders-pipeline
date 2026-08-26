"""The paginator must not lose or duplicate rows when order_id is not unique.

orders_raw has one row per order line, so a single order_id can span several
rows - and the source also contains duplicated lines. That is exactly the case
where limit/offset paging goes wrong, so it is the case worth testing.
"""

from __future__ import annotations

from collections import Counter

import pytest

from pipeline.config import Settings
from pipeline.ingest_orders import iter_pages


class FakePostgrest:
    """A minimal stand-in for the REST endpoint: ordering, gt filter and limit."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = sorted(rows, key=lambda r: r["order_id"])
        self.calls = 0

    def get(self, url, params=None, headers=None, timeout=None): 
        self.calls += 1
        rows = self.rows
        cursor = (params or {}).get("order_id")
        if cursor:
            assert cursor.startswith("gt.")
            rows = [r for r in rows if r["order_id"] > cursor[3:]]
        limit = (params or {}).get("limit", len(rows))
        return _FakeResponse(rows[:limit])

    def __enter__(self):
        return self

    def __exit__(self, *exc): 
        return False


class _FakeResponse:
    def __init__(self, payload: list[dict]) -> None:
        self._payload = payload
        self.status_code = 200
        self.headers = {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list[dict]:
        return self._payload


def _settings(page_size: int) -> Settings:
    return Settings(
        database_url="postgresql://unused",
        orders_url="https://example.invalid/orders_raw",
        orders_api_key="key",
        page_size=page_size,
    )


def _dataset() -> list[dict]:
    rows: list[dict] = []
    for i in range(1, 41):
        order_id = f"ORD{10000 + i}"
        for line in range(1 + i % 3): 
            rows.append({"order_id": order_id, "sku": f"SKU-{line}"})
    rows.append(dict(rows[0]))  
    return rows


@pytest.mark.parametrize("page_size", [5, 7, 10, 999, 1000])
def test_every_row_is_returned_exactly_once(page_size: int) -> None:
    rows = _dataset()
    session = FakePostgrest(rows)

    collected = [row for page in iter_pages(session, _settings(page_size)) for row in page]

    def key(row: dict) -> tuple[str, str]:
        return row["order_id"], row["sku"]

    assert Counter(map(key, collected)) == Counter(map(key, rows))


def test_pages_never_split_an_order() -> None:
    """A page boundary must never fall inside one order_id."""
    session = FakePostgrest(_dataset())
    seen: set[str] = set()

    for page in iter_pages(session, _settings(5)):
        ids_in_page = {row["order_id"] for row in page}
        assert not (ids_in_page & seen), "an order_id was split across two pages"
        seen |= ids_in_page


def test_a_page_too_small_to_hold_an_order_fails_loudly() -> None:
    """Better a hard error than a snapshot that quietly misses order lines."""
    session = FakePostgrest([{"order_id": "ORD10001", "sku": f"SKU-{i}"} for i in range(3)])

    with pytest.raises(RuntimeError, match="increase PAGE_SIZE"):
        list(iter_pages(session, _settings(2)))
