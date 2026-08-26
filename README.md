# Orders pipeline - Aqurate junior data engineer challenge

An end-to-end ETL around the `orders_raw` endpoint from the brief: ingest into
Postgres (Supabase), clean, pull daily ECB FX rates, convert to EUR, publish two
reporting tables, refresh them daily and check them on every run.

**Python** does the moving of data (paged API extraction, FX, orchestration,
monitoring); **SQL** does the modelling (cleaning rules, FX join, aggregation,
assertions). Everything is idempotent - the whole pipeline can be re-run at any
time and produces the same result.

```
 orders_raw REST endpoint          frankfurter.dev (ECB)
            |                                |
            | keyset paging, row-count check | date range, weekends included
            v                                v
   public.orders_raw  (text + jsonb)   public.fx_rates
            |                                |
            |  ops.rebuild_orders_clean()    |
            v                                |
   public.orders_clean  ---------------------+
            |            public.orders_clean_eur (view: amount_eur + rate used)
            |                    |
            |   ops.refresh_marts()
            +---------> public.customer_spend_eur            (step 4)
            +---------> public.revenue_by_country_category   (step 5)
   public.orders_rejected (quarantine, with a reason per row)

   every run: ops.pipeline_run + ops.quality_check_result  ->  ops.pipeline_health
```

| Brief | Where |
|---|---|
| 1. Ingest | `src/pipeline/ingest_orders.py` -> `public.orders_raw` |
| 2. Clean | `sql/02_orders_clean.sql` -> `public.orders_clean`, `public.orders_rejected` |
| 3. FX rates | `src/pipeline/ingest_fx.py` -> `public.fx_rates` |
| 4. Customer spend in EUR | `sql/03_marts.sql` -> `public.customer_spend_eur` |
| 5. Country/category breakdown | `sql/03_marts.sql` -> `public.revenue_by_country_category` |
| 6. Daily refresh | `.github/workflows/daily.yml` (+ `sql/05_pg_cron_optional.sql`) |
| 7. Write-up | this file |

---

## Quickstart

```bash
git clone <this repo> && cd aqurate-orders-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env      # fill in DATABASE_URL (Supabase -> Connect -> URI)

export PYTHONPATH=src
python -m pipeline.run setup      # create schema, functions, views
python -m pipeline.run all        # ingest + FX + clean + marts + checks
python -m pipeline.run report     # what the marts contain
python -m pipeline.run profile    # the data-issue report this write-up is based on
python -m pipeline.run health     # last run per step + open quality issues
pytest -q                         # unit tests, no database needed
```

`make setup|ingest|fx|transform|all|profile|report|health|test` do the same.

---

## 1. Data issues in `orders_raw`, and what the pipeline does with them

9,268 source rows. The grain is **one row per order line**, not per order: an
order has one row per SKU (6,000 distinct orders after normalisation), which is
what makes several of the issues below tricky.

Every decision below is reproducible with `python -m pipeline.run profile`, and
nothing is deleted silently: every row that does not reach `orders_clean` is
written to `public.orders_rejected` with a reason, and a quality check asserts
`orders_clean + orders_rejected = orders_raw`.

| # | Issue | Rows | Decision | Why |
|---|---|---|---|---|
| 1 | Duplicated order lines | 263 | keep one copy per `(order_id, sku)` | 183 are byte-identical; the other 80 differ **only** in `order_id` formatting and `fx_reference_date`. Nothing else differs, so they are re-exports of the same line, not genuine second lines. |
| 2 | `order_id` formatting: 49 with a leading space, 31 lower-case | 80 | trim + upper-case | All 80 turned out to be duplicates of a correctly formatted row - which is exactly why they must be normalised *before* de-duplicating, otherwise 80 phantom orders survive. |
| 3 | Three different `order_ts` formats: ISO (5,592), day-first `dd/mm/yyyy` (2,270), Unix epoch seconds (1,406) | 9,268 | parse all three into `timestamp` | Day-first is unambiguous here: the first component goes up to 31 and the second never exceeds 12. Epoch values decode to the same Jan-Jun 2026 window as the other rows, which confirms the reading. |
| 4 | `category` is NULL | 79 | back-fill from the SKU's own most frequent category | `sku`/`product_name` determine the category everywhere else in the data, so the value is recoverable; dropping the rows would lose real revenue. |
| 5 | SKU spelled 3 different ways: `SKU-FA-O03` (letter O for zero), `SKUEL001`, `SKU HK 003` | 221 lines, 19 spellings for 16 products | canonical SKU = the most frequent spelling for that `product_name` | Data-driven instead of a hard-coded lookup: a new typo is absorbed automatically. Without it, `SKU-FA-003` revenue is split across two "products" and the de-duplication key is wrong. |
| 6 | `customer_id` NULL | 103 | recover the id from `customer_email` (`customer<id>@example.com`) | `customer_id` <-> e-mail is 1:1 across the whole dataset, so the id is derivable. 100 rows are recovered; the other 3 are internal test accounts, dropped by rule 8. |
| 7 | Negative `qty` (-1 to -3), all on `completed` orders | 167 | take the absolute value, flag `qty_sign_flipped` | Refunds have their own status, and these rows carry a normal positive price, so a negative quantity on a completed order reads as a sign error on export. Treating them as returns instead would only change spend by ~1%; the flag makes the assumption visible and reversible. |
| 8 | Internal test orders: `status = 'test'`, all with `@aqurate.ai` e-mails | 101 | quarantine | The two signals agree exactly (101 rows both ways), so this is unambiguous - test traffic is not revenue. |
| 9 | `unit_price = 999999` sentinel | 13 | quarantine as `implausible_unit_price` | Roughly 5,000x the median price of the same SKU (rule: > 20x the SKU median). Keeping them would add ~€13M of fictional revenue and put every country over the €40k threshold. Imputing a price would invent revenue that never existed, so they are quarantined for the data owner instead. |
| 10 | `unit_price = 0` | 24 | keep, flag `zero_unit_price` | Plausible as a giveaway/replacement line: it contributes €0, so it cannot distort a total, and dropping it would understate order counts. |
| 11 | `status = 'refunded'` | 403 | keep in `orders_clean`, exclude from spend/revenue | They are real orders and belong in the clean layer, but "total amount spent" should not include money that was given back. They are standalone orders with positive quantities, not reversal lines, so netting them off another order would be wrong. |
| 12 | `fx_reference_date` on weekends, and 6,131 rows dated in the future | 9,268 | see the FX section below | |

Two observations that are **not** treated as errors, and why:

* **RON prices are not RON-scaled.** A `RON` line for the same SKU has roughly
  the same numeric price as an `EUR` line (~80 for the earbuds, not ~420). The
  brief says to convert by `currency`, so the pipeline does exactly that, but in
  a real project this is the first question to the data owner - either the
  currency label or the price column is wrong. It is flagged here rather than
  silently "fixed", because guessing would change revenue by a factor of five.
* **Country/currency mixes** (RON orders from DE/HU/BG) are left alone: paying
  in a different currency than your country of residence is normal.

## 2. FX: one rule, three real cases

Rates come from Frankfurter (ECB reference rates, EUR base). The ECB publishes
**working days only** - no weekends, no TARGET holidays - and `fx_reference_date`
in this dataset runs 2026-08-23 .. 2026-09-03, so it covers weekends *and* dates
that have not happened yet.

The lookup is a single rule: **the most recent rate published on or before
`fx_reference_date`** (a `LATERAL` join in `public.orders_clean_eur`). It covers
all three cases at once, and every converted row also stores the rate and the
date it came from, so any figure can be explained afterwards:

| `fx_status` | Case | Behaviour |
|---|---|---|
| `exact` | reference date is a working day | that day's rate |
| `carried_forward` | weekend / holiday | previous working day's rate |
| `provisional` | reference date is in the future | latest published rate, flagged |
| `missing` | no rate at all | row excluded from the marts **and the run fails** |

Conversion is `amount_eur = qty * unit_price / rate`, since the stored rate is
"quote units per 1 EUR".

This is also why the daily refresh in step 6 actually does something: as the
future reference dates arrive, `provisional` rows are recomputed with the real
published rate. It is not cosmetic - on the current data **DE sits at €39,842**
against a €40,000 threshold, so a few days of rate movement can add a country to
`revenue_by_country_category`.

## 3. Results (run of 2026-08-26)

```
orders_raw      9,268 lines
orders_clean    8,895 lines / 5,932 orders   (373 quarantined: 259 duplicates,
                                              101 internal test, 13 sentinel prices)
customer_spend_eur   1,873 customers, €738,227.47 total
revenue_by_country_category
  #1  RO   €150,736.75   (books €23,252.54 / electronics €127,484.21)
  #2  HU    €41,981.36   (books  €6,317.74 / electronics  €35,663.62)
  below the €40k threshold: DE €39,842.18, BG €34,391.80
```

FX status of the converted lines: 506 `exact`, 137 `carried_forward` (weekend),
1,202 `provisional` (future reference date, will self-correct).

## 4. Monitoring - how I would find out if the daily job silently failed

The failure that matters is not a crash, it is a run that "succeeds" while
publishing stale or wrong numbers. Four layers, cheapest first:

1. **The job fails loudly instead of publishing.** `ops.run_daily()` runs the
   clean + marts rebuild *and* the assertions in one transaction and raises if an
   error-level check fails, so a bad run leaves the previous good tables in place
   rather than replacing them with garbage. The ingest is verified against the
   row count the API reports (`Content-Range`), so a half-downloaded snapshot
   fails instead of quietly shrinking every total.
2. **Assertions, not eyeballs** (`ops.quality_check_result`, 10 checks): clean
   layer not empty; `clean + rejected = raw`; rejection rate <= 10%; every
   non-EUR line found a rate; FX table no older than 5 days; marts refreshed
   within 23h; mart total equals a straight recomputation from the clean layer;
   no negative spend; the €40k threshold respected; and a warning counting rows
   still converted with a provisional rate. Results are stored per run, so
   "since when is this wrong?" is answerable.
3. **Liveness, not just correctness.** A job that stops running produces no
   errors at all. `ops.pipeline_health` flags any step whose last *successful*
   run is older than 26 hours (`needs_attention`), which catches a disabled
   schedule, an expired credential or a paused Supabase project. On top of that,
   `HEARTBEAT_URL` (healthchecks.io / Better Stack / Cronitor, free tier) is
   pinged **only** after a fully successful run - if the run never happens, the
   dead man's switch alerts. That is the one check that survives the pipeline
   being dead.
4. **Somewhere a human looks.** GitHub Actions e-mails the repo owner when a
   scheduled workflow fails, the run summary is written to the job page, and the
   failure step prints `ops.pipeline_health`. In a production setup I would route
   the same signal to the team's Slack/PagerDuty instead of one person's inbox,
   and add freshness monitoring on the mart tables themselves (a
   `max(refreshed_at)` check from the BI tool) so consumers detect staleness
   independently of the pipeline that produces it.

Deliberately *not* alerted on: individual quarantined rows. They are expected
(4.02% here); what is alertable is the *rate* changing, which is check 3.

## 5. Automation (step 6)

`.github/workflows/daily.yml` runs the full pipeline at 05:15 UTC - after the
ECB rates for the previous working day are published, and early enough that a
failure is visible at the start of the day. It needs two repository secrets:
`DATABASE_URL` and `ORDERS_SOURCE_API_KEY` (optionally `HEARTBEAT_URL`).

`sql/05_pg_cron_optional.sql` is the alternative with no external infrastructure
at all: `pg_cron` + the `http` extension fetch the rates and refresh the marts
from inside Supabase. Both are free.

**Teardown after the 3-5 day observation window** (nothing here costs money, but
so it does not run forever):

```bash
# GitHub Actions: disable the workflow (Actions tab -> daily-pipeline -> Disable)
# or delete .github/workflows/daily.yml
```
```sql
-- if the in-database schedule was used
select cron.unschedule('aqurate-daily-refresh');
```

## 6. AI usage

Tool: **Claude (Claude Code)**, used as a pair programmer for the whole project.

* **Profiling first, code second.** Rather than guessing at the data issues, the
  9,268 rows were loaded into a scratch schema and profiled with SQL - every
  number in section 1 comes from that pass, and the probes were kept as
  `src/pipeline/profile_raw.py` so the claims stay reproducible.
* **Kept:** the layered structure (landing zone as text -> clean -> marts), the
  quarantine table, the `LATERAL` FX lookup with the rate stored next to the
  amount, and the quality-check harness. The whole SQL layer was executed against
  the real data in a scratch schema before being committed - which is how the
  `percentile_cont` type mismatch and a `create or replace view` incompatibility
  were caught rather than shipped.
* **Changed / rejected:** the first ingest used `limit/offset` paging, which
  silently loses and repeats rows when `order_id` is not unique - replaced with
  keyset paging plus a row-count assertion, and a unit test that reproduces the
  failure. It was not theoretical: the offset-paged snapshot reported 185
  byte-identical duplicates against the keyset snapshot's 183, i.e. it invented
  two rows at a page boundary. Suggestions to impute the `999999` prices and to drop the
  negative-quantity rows were rejected in favour of quarantining and flagging:
  inventing revenue and discarding it are both worse than making the assumption
  visible. The dedupe tie-break (prefer the well-formatted `order_id`, then the
  earlier `fx_reference_date`) is a judgement call, documented as such.
* **Not delegated:** which rows count as revenue (refunded/test/zero-price), how
  much of a price outlier is "too much", and the RON-scaling question above -
  those are data-owner decisions, and the pipeline is written so each one is a
  single visible line rather than a hidden default.

## 7. Repository layout

```
sql/01_schema.sql              tables, indexes, ops.settings, RLS
sql/02_orders_clean.sql        cast helpers + ops.orders_judged + rebuild function
sql/03_marts.sql               orders_clean_eur view + ops.refresh_marts()
sql/04_quality_checks.sql      assertions, ops.run_daily(), health views
sql/05_pg_cron_optional.sql    in-database daily schedule (optional)
src/pipeline/ingest_orders.py  keyset-paged extraction + row-count verification
src/pipeline/ingest_fx.py      Frankfurter -> fx_rates
src/pipeline/transform.py      runs the SQL, surfaces failed checks
src/pipeline/profile_raw.py    the data-issue report
src/pipeline/run.py            CLI
tests/                         pagination + FX parsing (no database required)
.github/workflows/daily.yml    the daily schedule
```

### Notes on the database

All tables live in `public` (the names the brief asks for) with RLS enabled and
no policies, so nothing is exposed through Supabase's REST API; the pipeline
connects with the Postgres/service role, which bypasses RLS. Operational objects
(run log, checks, settings, functions) live in `ops`.
