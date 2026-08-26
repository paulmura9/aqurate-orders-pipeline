# Setup checklist

Everything below is free tier. Roughly 20 minutes end to end.

## 1. Supabase project

1. https://supabase.com/dashboard -> **New project** (region `eu-central-1` or
   `eu-west-1`). Save the database password - it is shown once.
2. Wait for the project to finish provisioning (~2 min).
3. **Connect** (top bar) -> **Connection string** -> **URI**, and copy the
   *Session pooler* string. It looks like:
   `postgresql://postgres.<ref>:<password>@aws-1-<region>.pooler.supabase.com:5432/postgres`
   Replace `<password>` with the real password (URL-encode any `@ : / ?` in it).

Nothing has to be created by hand in the dashboard: `python -m pipeline.run setup`
creates every table, view and function from `sql/`.

## 2. Run it locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # paste DATABASE_URL, keep the rest as-is
export PYTHONPATH=src

python -m pipeline.run setup  # schema + functions
python -m pipeline.run all    # ingest + FX + clean + marts + quality checks
python -m pipeline.run report
```

Expected on the current data: `orders_raw` 9,268 rows, `orders_clean` 8,893,
`orders_rejected` 375, `customer_spend_eur` 1,873 customers,
`revenue_by_country_category` 2 rows (RO, HU).

If `python -m pipeline.run all` fails with a quality-check error, run
`python -m pipeline.run health` - it prints which assertion failed and why.

### Checking the result in the Supabase SQL editor

```sql
select * from public.revenue_by_country_category order by revenue_rank;
select * from public.customer_spend_eur order by total_spend_eur desc limit 20;
select reason, count(*) from public.orders_rejected group by 1 order by 2 desc;
select * from ops.pipeline_health;
select * from ops.latest_quality_issues;
```

## 3. GitHub

1. Create a **new empty repository** (private is fine), then:

   ```bash
   git init && git add . && git commit -m "Orders pipeline"
   git branch -M main
   git remote add origin git@github.com:<you>/<repo>.git
   git push -u origin main
   ```

   `.env` is git-ignored - the database password must never be committed.

2. **Settings -> Collaborators -> Add people -> `aqurate-careers`** (required by
   the brief).

3. **Settings -> Secrets and variables -> Actions -> New repository secret**:

   | Name | Value |
   |---|---|
   | `DATABASE_URL` | the connection string from step 1 |
   | `ORDERS_SOURCE_API_KEY` | `sb_publishable_Xwjiw--qkKcbMuSbKd6I2w_wN9mpNTv` |
   | `HEARTBEAT_URL` | optional, see below |

   And under the **Variables** tab:

   | Name | Value |
   |---|---|
   | `ORDERS_SOURCE_URL` | `https://jzozteoirwfczccltcdr.supabase.co/rest/v1/orders_raw` |

4. **Actions** tab -> `daily-pipeline` -> **Run workflow** once, to prove the
   schedule works before leaving it to run on its own at 05:15 UTC.

   > Scheduled workflows only run on the repository's default branch, and GitHub
   > disables them after 60 days without repository activity - neither matters
   > for a 3-5 day window.

## 4. Optional: dead man's switch

https://healthchecks.io -> new check, period 1 day, grace 2 hours -> copy the
ping URL into the `HEARTBEAT_URL` secret. It is pinged only after a fully
successful run, so it alerts by e-mail if the pipeline stops running at all -
the one failure mode a pipeline cannot report about itself.

## 5. Optional: run the schedule inside Supabase instead

If you would rather not depend on GitHub Actions, paste `sql/05_pg_cron_optional.sql`
into the Supabase SQL editor. It enables `pg_cron` + `http`, fetches the FX rates
and refreshes the marts from inside the database, every day at 05:15 UTC.
(`orders_raw` is static for this exercise, so the in-database job does not
re-ingest it.)

## 6. Teardown after 3-5 days

* GitHub: **Actions -> daily-pipeline -> ... -> Disable workflow**
* Supabase (only if `05_pg_cron_optional.sql` was applied):
  `select cron.unschedule('aqurate-daily-refresh');`
* The Supabase project itself can stay - a free project costs nothing (it just
  pauses after a week of inactivity).
