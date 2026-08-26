"""Data profiling for orders_raw.

This is the script that produced the "data issues" section of the README: it
counts every problem the cleaning layer knows about, straight from the landing
zone, so the claims in the write-up can be reproduced instead of trusted.

    python -m pipeline.run profile
"""

from __future__ import annotations

import psycopg

from .db import fetch_all

PROBES: tuple[tuple[str, str], ...] = (
    ("rows in the latest raw batch", "select count(*) from raw"),
    ("distinct order_id (raw spelling)", "select count(distinct order_id) from raw"),
    ("distinct order_id (trimmed + upper)", "select count(distinct upper(btrim(order_id))) from raw"),
    ("order_id with leading/trailing spaces", "select count(*) from raw where order_id <> btrim(order_id)"),
    ("order_id in lower case", "select count(*) from raw where order_id <> upper(order_id)"),
    ("exact duplicate rows", """
        select coalesce(sum(n - 1), 0) from (
            select count(*) as n from raw group by payload having count(*) > 1
        ) d"""),
    ("duplicate (order_id, sku) after normalisation", """
        select coalesce(sum(n - 1), 0) from (
            select count(*) as n from raw
             group by ops.normalise_order_id(order_id), ops.normalise_sku(sku)
            having count(*) > 1
        ) d"""),
    ("order_ts as ISO 8601", "select count(*) from raw where order_ts ~ '^\\d{4}-\\d{2}-\\d{2}T'"),
    ("order_ts as day-first d/m/Y", "select count(*) from raw where order_ts ~ '^\\d{1,2}/\\d{1,2}/\\d{4}'"),
    ("order_ts as unix epoch", "select count(*) from raw where order_ts ~ '^\\d+$'"),
    ("order_ts unparseable", "select count(*) from raw where ops.parse_order_ts(order_ts) is null"),
    ("category NULL", "select count(*) from raw where nullif(btrim(category), '') is null"),
    ("customer_id NULL", "select count(*) from raw where customer_id is null"),
    ("customer_id NULL but recoverable from e-mail", """
        select count(*) from raw
         where customer_id is null and customer_email ~ '^customer\\d+@'"""),
    ("negative qty", "select count(*) from raw where ops.safe_numeric(qty) < 0"),
    ("zero qty", "select count(*) from raw where ops.safe_numeric(qty) = 0"),
    ("unit_price = 0", "select count(*) from raw where ops.safe_numeric(unit_price) = 0"),
    ("unit_price far above the SKU median (sentinel)", """
        select count(*) from raw r
          join (
            select ops.normalise_sku(sku) as k,
                   (percentile_cont(0.5) within group (order by ops.safe_numeric(unit_price)))::numeric as med
              from raw where ops.safe_numeric(unit_price) > 0 group by 1
          ) m on m.k = ops.normalise_sku(r.sku)
         where ops.safe_numeric(r.unit_price) > 20 * m.med"""),
    ("internal test orders (status='test')", "select count(*) from raw where lower(btrim(status)) = 'test'"),
    ("internal e-mail domain", "select count(*) from raw where customer_email ilike '%@aqurate.ai'"),
    ("refunded orders", "select count(*) from raw where lower(btrim(status)) = 'refunded'"),
    ("SKU spellings (raw)", "select count(distinct btrim(sku)) from raw"),
    ("SKU spellings (normalised)", "select count(distinct ops.normalise_sku(sku)) from raw"),
    ("fx_reference_date in the future", """
        select count(*) from raw where ops.safe_date(fx_reference_date) > current_date"""),
    ("fx_reference_date on a weekend (no ECB rate)", """
        select count(*) from raw
         where extract(isodow from ops.safe_date(fx_reference_date)) in (6, 7)"""),
    ("non-EUR lines needing conversion", "select count(*) from raw where upper(btrim(currency)) <> 'EUR'"),
)

_HEADER = """
with raw as (
    select * from public.orders_raw
     where batch_id = (select batch_id from public.orders_raw order by ingested_at desc, raw_id desc limit 1)
)
"""


def report(conn: psycopg.Connection) -> str:
    lines = ["orders_raw profile", "=" * 62]
    with conn.cursor() as cur:
        for label, probe in PROBES:
            cur.execute(f"{_HEADER} {probe}")  # type: ignore[arg-type]
            row = cur.fetchone()
            value = list(row.values())[0] if row else None
            lines.append(f"{label:<50} {value:>10}")

    lines += ["", "value distributions", "=" * 62]
    for column in ("status", "channel", "country", "currency", "category"):
        lines.append(f"\n{column}:")
        for row in fetch_all(
            conn,
            f"""
            {_HEADER}
            select coalesce(nullif(btrim({column}), ''), '<null>') as value, count(*) as n
              from raw group by 1 order by 2 desc
            """,
        ):
            lines.append(f"  {row['value']:<24} {row['n']:>8}")

    lines += ["", "fx_reference_date coverage", "=" * 62]
    for row in fetch_all(
        conn,
        f"""
        {_HEADER}
        select ops.safe_date(fx_reference_date) as ref_date,
               to_char(ops.safe_date(fx_reference_date), 'Dy') as weekday,
               count(*) as n,
               count(*) filter (where upper(btrim(currency)) <> 'EUR') as non_eur
          from raw group by 1, 2 order by 1
        """,
    ):
        lines.append(
            "  {ref_date}  {weekday}  {n:>6} lines ({non_eur} non-EUR)".format(**row)
        )

    return "\n".join(lines)
