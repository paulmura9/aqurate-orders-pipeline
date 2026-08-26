# Orders pipeline

My solution to the Aqurate junior data engineer challenge. It pulls `orders_raw`
from the REST endpoint into Postgres (Supabase), cleans it, fetches daily ECB
exchange rates, converts everything to EUR, and builds the two reporting tables
the brief asks for. It reruns itself every morning through GitHub Actions.

Python handles moving data around: paging the API, fetching rates, orchestration,
monitoring. SQL does the actual modelling — the cleaning rules, the FX join, the
aggregations, the assertions. I kept it that way on purpose; the transformation
logic is the part a reviewer (or a future me) will want to read, and it reads
better as SQL than as pandas.

Everything is idempotent. You can run the whole thing twice in a row and get the
same tables.

```
 orders_raw endpoint                frankfurter.dev (ECB rates)
         |                                   |
         v                                   v
   public.orders_raw                   public.fx_rates
   (everything as text + the raw jsonb)      |
         |                                   |
         |  ops.rebuild_orders_clean()       |
         v                                   |
   public.orders_clean  --------------------+
   public.orders_rejected                    |
                          public.orders_clean_eur  (view: EUR amount + which rate was used)
                                   |
                          ops.refresh_marts()
                                   |
              +--------------------+--------------------+
              v                                         v
   public.customer_spend_eur              public.revenue_by_country_category
```

Every run writes to `ops.pipeline_run` and `ops.quality_check_result`.

## Running it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # DATABASE_URL from Supabase -> Connect -> URI
export PYTHONPATH=src

python -m pipeline.run setup  # creates the schema, functions and views
python -m pipeline.run all    # ingest + FX + clean + marts + checks
```

Other commands: `report` (what's in the marts), `profile` (the data-issue report
below), `health` (last run per step, open quality issues). `pytest -q` runs the
unit tests, which don't need a database. There's a `Makefile` if you prefer
`make all`. `SETUP.md` has the full Supabase and GitHub walkthrough.

## What was wrong with the data

The first thing I did was load all 9,268 rows into a scratch schema and count
things, before writing any cleaning code. `python -m pipeline.run profile`
reproduces that report, so every number below can be checked.

The detail that shapes everything else: **a row is an order line, not an order.**
One order has one row per SKU. There are 9,268 rows but only 6,000 real orders.
Miss that and the duplicate handling goes wrong in both directions.

Nothing gets dropped quietly. Every row that doesn't make it into `orders_clean`
lands in `orders_rejected` with a reason attached, and one of the quality checks
asserts that `clean + rejected = raw` on every run.

**Duplicates (263 rows).** 183 are byte-for-byte identical. The other 80 are the
interesting ones: they differ *only* in how `order_id` is written and in
`fx_reference_date`. Same customer, same SKU, same quantity, same price, same
timestamp. That's a re-export of the same line, not a second purchase, so I keep
one copy per `(order_id, sku)`. Which copy? I take the one whose `order_id` was
already formatted correctly, and break remaining ties on the earlier
`fx_reference_date`. That second part is a coin flip and I've flagged it as such
— nothing in the data says which version is newer.

**Messy `order_id` (80 rows).** 49 have a leading space, 31 are lower-case.
Trimming and upper-casing is obvious enough, but the ordering matters: normalise
*first*, then de-duplicate. All 80 turned out to be the duplicates described
above, so if you de-duplicate on the raw string you end up with 6,080 orders
instead of 6,000 and 80 phantom orders in your customer totals.

**Three timestamp formats (all 9,268 rows).** 5,592 ISO 8601, 2,270 in
`dd/mm/yyyy`, 1,406 as Unix epoch seconds. Day-first rather than month-first
isn't a guess: the first component goes up to 31 and the second never exceeds 12.
The epoch values decode into the same Jan–Jun 2026 window as everything else,
which was a nice confirmation that I'd read them correctly.

**SKUs spelled three different ways (221 rows).** `SKU-FA-O03` with a capital O
instead of a zero, `SKUEL001` with no separators, `SKU HK 003` with spaces. 19
spellings for 16 actual products. Instead of hard-coding a mapping I let the data
decide: the canonical SKU for a product is whichever spelling appears most often
for that `product_name`. A new typo gets absorbed automatically. This one isn't
cosmetic either — without it `SKU-FA-003` revenue splits across two "products"
and the de-duplication key is wrong.

**Missing `category` (79 rows).** Filled in from the SKU's own most common
category. The SKU determines the category everywhere else in the dataset, so the
value is genuinely recoverable and throwing the rows away would lose real
revenue.

**Missing `customer_id` (103 rows).** The e-mail encodes it
(`customer1714@example.com`), and the id-to-e-mail mapping is 1:1 across the whole
dataset, so 100 of them are recoverable. The remaining 3 belong to internal test
accounts and get dropped anyway.

**Negative quantities (167 rows).** All between -1 and -3, all on `completed`
orders, all with a normal positive price. Refunds have their own status in this
data, so I read these as a sign error somewhere in the export rather than as
returns, and take the absolute value. I'm not certain about this one. Treating
them as returns instead moves total spend by about 1%, which is why every
affected row carries a `qty_sign_flipped` flag — if the data owner says otherwise,
the rows are one query away.

**Internal test orders (101 rows).** `status = 'test'` and an `@aqurate.ai`
e-mail address. Both signals pick out exactly the same 101 rows, so there's no
ambiguity here. Test traffic isn't revenue; quarantined.

**Sentinel prices (13 rows).** `unit_price = 999999`, roughly five thousand times
the median price of the same SKU. The rule I settled on is "more than 20x the
median for that SKU", which catches these and nothing else (the widest genuine
spread is under 2x). I quarantine them rather than imputing a price. Keeping
them adds about €13M of imaginary revenue and pushes every country over the €40k
threshold; imputing invents revenue that never existed. Neither is mine to
decide, so they go in the reject table with the reason spelled out.

**Zero prices (24 rows).** Kept, with a flag. A €0 line is plausible as a
giveaway or a replacement, it contributes nothing to a total so it can't distort
anything, and dropping it would understate the order count.

**Refunded orders (403 rows).** These stay in `orders_clean` — they're real
orders — but they're excluded from spend and revenue. "Total amount spent"
shouldn't include money that went back to the customer. Worth noting they're
standalone orders with positive quantities, not reversal lines pointing at
another order, so netting them off something else would be wrong.

Two things I noticed and deliberately did *not* "fix":

A RON line and an EUR line for the same SKU have roughly the same number in
`unit_price` — about 80 for the earbuds, not about 420. If those RON prices were
really in RON, that product costs €15. Either the currency label or the price
column is wrong upstream. The brief says convert by `currency`, so that's what
the pipeline does, but in a real project this is my first message to whoever owns
the data, because guessing wrong changes revenue by a factor of five.

Orders paid in RON from Germany, Hungary and Bulgaria look odd at first glance
but are perfectly normal. People pay in currencies other than their country's.
Left alone.

## Exchange rates

Rates come from Frankfurter, which republishes the ECB reference rates with EUR
as the base. The ECB only publishes on working days — no weekends, no TARGET
holidays — and `fx_reference_date` in this dataset runs from 2026-08-23 to
2026-09-03. So it covers weekends *and* dates that haven't happened yet.

Rather than special-casing each situation I use one rule: take the most recent
rate published on or before `fx_reference_date`. It's a `LATERAL` join in
`public.orders_clean_eur` and it handles all three cases by itself. Every
converted row also stores the rate and the date that rate came from, so any
number in the marts can be traced back afterwards:

- `exact` — the reference date was a working day
- `carried_forward` — weekend or holiday, so the previous working day's rate
- `provisional` — the reference date is in the future, so the latest rate we have
- `missing` — no rate at all, which excludes the row from the marts *and* fails
  the run

The conversion is `qty * unit_price / rate`, since the stored rate is quote units
per one EUR.

This is also what makes the daily refresh in step 6 do something real rather than
recompute the same numbers. As those future reference dates arrive, the
`provisional` rows get recalculated with the rate that actually got published.
And it's not academic: Germany currently sits at €39,842 against a €40,000
threshold, so a few days of rate movement could add a whole country to the
breakdown table.

## Results

From the run on 2026-08-26:

```
orders_raw            9,268 lines
orders_clean          8,895 lines / 5,932 orders
orders_rejected         373 lines  (259 duplicates, 101 internal test, 13 sentinel prices)
customer_spend_eur    1,873 customers, €738,227.47 total

revenue_by_country_category (Books + Electronics, above €40,000)
  1. RO   €150,736.75   (books €23,252.54, electronics €127,484.21)
  2. HU    €41,981.36   (books  €6,317.74, electronics  €35,663.62)

  just below the line: DE €39,842.18, BG €34,391.80
```

Of the converted lines, 506 used an exact-date rate, 137 carried a rate forward
over a weekend, and 1,202 are still provisional and will settle over the next
week.

## How I'd know if this broke

The failure I actually worry about isn't a crash. It's a run that reports success
while quietly publishing stale or wrong numbers, because nobody looks at a green
job.

So the first line of defence is that the job refuses to publish garbage.
`ops.run_daily()` rebuilds the clean layer, refreshes both marts and runs the
assertions inside a single transaction, and raises if any error-level check
fails. A bad run leaves yesterday's good tables in place instead of replacing
them. Same idea on the way in: the ingest compares what it loaded against the row
count PostgREST reports in `Content-Range`, so a half-downloaded snapshot fails
loudly instead of silently shrinking every total.

Then there are the assertions themselves, ten of them, stored per run in
`ops.quality_check_result` so "since when has this been wrong?" is a question you
can answer. The clean table isn't empty. `clean + rejected = raw`. The rejection
rate is under 10%. Every non-EUR line found a rate. The FX table isn't more than
five days stale. The marts were refreshed in the last 23 hours. The mart total
matches a straight recomputation from the clean layer. No negative spend. Nothing
in the country table sits below its own threshold. Plus a warning that counts how
many lines are still converted at a provisional rate.

None of that helps if the job stops running altogether, which produces no errors
at all. Two things cover that. `ops.pipeline_health` flags any step whose last
successful run is more than 26 hours old, which catches a disabled schedule, an
expired password or a paused Supabase project. And `HEARTBEAT_URL` — a
healthchecks.io ping, free — is only hit after a fully successful run, so if the
run never happens, something external notices and e-mails me. That's the one
check that survives the pipeline being completely dead.

Finally, someone has to actually see it. GitHub e-mails the repo owner when a
scheduled workflow fails, the run summary is written to the job page, and the
failure step dumps `ops.pipeline_health` into the log. In a real team I'd send
that to Slack or PagerDuty instead of one person's inbox, and I'd also put a
freshness check on the mart tables from whatever BI tool reads them — so the
consumers notice staleness on their own, independently of the pipeline that's
supposed to be producing it.

One thing I chose *not* to alert on: individual rejected rows. About 4% of rows
get quarantined and that's expected and stable. What's worth waking someone up
for is that percentage moving, which is what the rejection-rate check watches.

## Automation

`.github/workflows/daily.yml` runs the whole pipeline at 05:15 UTC. That time is
deliberate — it's after the ECB rates for the previous working day are out, and
early enough that a failure is visible at the start of the day. It needs two
repository secrets, `DATABASE_URL` and `ORDERS_SOURCE_API_KEY`, plus
`HEARTBEAT_URL` if you want the dead man's switch.

`sql/05_pg_cron_optional.sql` is the same thing with no external infrastructure
at all: `pg_cron` plus the `http` extension fetch the rates and refresh the marts
from inside Supabase. Both options are free.

To tear it down after a few days, disable the workflow in the Actions tab (or
delete the file), and if the in-database schedule was used,
`select cron.unschedule('aqurate-daily-refresh');`.

## AI usage

I used Claude (Claude Code) throughout, as a pair programmer rather than a code
generator.

What worked well was insisting on profiling before writing anything. It's very
easy to get plausible-looking cleaning code for data nobody has looked at, so the
first pass was loading the rows into a scratch schema and counting problems.
Every number in the data-issues section comes from that, and I kept the queries
as `src/pipeline/profile_raw.py` so the claims stay checkable instead of being
prose I'd have to trust.

I kept the overall shape: text-only landing zone, then clean, then marts; the
quarantine table instead of deleting rows; the `LATERAL` FX lookup that stores
the rate next to the amount; the quality-check harness. I also ran the entire SQL
layer against the real data in a scratch schema before committing any of it,
which is how a `percentile_cont` type mismatch and a `create or replace view`
incompatibility got caught in development rather than in the first real run.

The thing I changed and am happiest about: the first version of the ingest used
`limit`/`offset` paging. That silently loses and repeats rows when the ordering
column isn't unique, which `order_id` very much isn't here. It wasn't
theoretical — the offset-paged snapshot reported 185 byte-identical duplicates
where the correct one has 183, so it had invented two rows at a page boundary. I
replaced it with keyset paging over `order_id`, a row-count assertion against the
API, and a unit test that reproduces the failure with a deliberately small page
size. That's my favourite part of this repo, mostly because the bug found itself
through a check rather than through luck.

Two suggestions I rejected: imputing a sensible price over the `999999` sentinel,
and dropping the negative-quantity rows. Inventing revenue and throwing revenue
away are both worse than making the assumption visible, so those rows are
quarantined and flagged instead.

And a few things I didn't delegate at all, because they aren't code decisions:
which statuses count as revenue, how far from the median a price has to be before
it stops being a price, and the RON-scaling question above. Those belong to
whoever owns the data. The pipeline is written so each one is a single readable
line rather than a default buried three functions deep.

## Layout

```
sql/01_schema.sql             tables, indexes, ops.settings, RLS
sql/02_orders_clean.sql       cast helpers, the ops.orders_judged view, the rebuild function
sql/03_marts.sql              orders_clean_eur + ops.refresh_marts()
sql/04_quality_checks.sql     assertions, ops.run_daily(), the health views
sql/05_pg_cron_optional.sql   in-database schedule (optional)

src/pipeline/ingest_orders.py keyset paging + row-count verification
src/pipeline/ingest_fx.py     Frankfurter -> fx_rates
src/pipeline/transform.py     runs the SQL, surfaces failed checks
src/pipeline/profile_raw.py   the data-issue report
src/pipeline/run.py           CLI

tests/                        pagination and FX parsing, no database needed
.github/workflows/daily.yml   the daily schedule
```

Tunables that shouldn't need a code change live in `ops.settings` — the €40,000
threshold, which categories go in the breakdown, which statuses count as revenue,
the price-outlier factor.

All the tables the brief asks for are in `public` with RLS enabled and no
policies, so nothing is readable through Supabase's REST API; the pipeline
connects as the Postgres role, which bypasses RLS. Everything operational — the
run log, the checks, the settings, the functions — lives in `ops`.
