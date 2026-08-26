-- =============================================================================
-- 05_pg_cron_optional.sql - daily refresh without any external infrastructure
--
-- The primary scheduler for this project is GitHub Actions (see
-- .github/workflows/daily.yml), which also re-ingests orders_raw and writes a
-- run log outside the database. This file is the "no external infra" variant:
-- Supabase can fetch the FX rates and refresh the marts entirely on its own.
--
-- Run this file only if you want the in-database schedule. Both extensions are
-- available on Supabase's free tier.
-- =============================================================================

create extension if not exists pg_cron;
create extension if not exists http with schema extensions;

-- -----------------------------------------------------------------------------
-- Pull the daily ECB rates straight from Frankfurter into public.fx_rates.
-- Re-running is safe: rates are upserted per (date, base, quote).
-- -----------------------------------------------------------------------------
create or replace function ops.fetch_fx_rates(
    p_days_back  integer default 10,
    p_currencies text[]  default null
)
returns integer language plpgsql as $$
declare
    v_currencies text[] := coalesce(
        p_currencies,
        (select array_agg(distinct currency) from public.orders_clean where currency <> 'EUR'),
        array['RON']
    );
    v_start   date := current_date - p_days_back;
    v_url     text;
    v_status  integer;
    v_body    text;
    v_rows    integer;
begin
    v_url := format('https://api.frankfurter.dev/v1/%s..%s?base=EUR&symbols=%s',
                    v_start, current_date, array_to_string(v_currencies, ','));

    select r.status, r.content into v_status, v_body
      from extensions.http_get(v_url) r;

    if v_status <> 200 then
        raise exception 'Frankfurter returned HTTP % for %', v_status, v_url;
    end if;

    insert into public.fx_rates (rate_date, base_currency, quote_currency, rate, source, fetched_at)
    select d.key::date, 'EUR', c.key, c.value::numeric, 'frankfurter.dev', now()
      from jsonb_each((v_body::jsonb) -> 'rates') d,
           lateral jsonb_each_text(d.value) c
    on conflict (rate_date, base_currency, quote_currency)
    do update set rate = excluded.rate, fetched_at = excluded.fetched_at;

    get diagnostics v_rows = row_count;
    return v_rows;
end;
$$;

-- -----------------------------------------------------------------------------
-- One job: refresh the rates, then rebuild the clean layer and both marts.
-- 05:15 UTC is after the ECB reference rates for the day are published (~16:00
-- CET the previous working day), so the run always sees the newest rate.
-- -----------------------------------------------------------------------------
select cron.schedule(
    'aqurate-daily-refresh',
    '15 5 * * *',
    $job$
    select ops.fetch_fx_rates(10);
    select ops.run_daily('pg_cron');
    $job$
);

-- Inspect:
--   select * from cron.job;
--   select * from cron.job_run_details order by start_time desc limit 10;
--   select * from ops.pipeline_health;
--
-- Tear the automation down after the 3-5 day observation window:
--   select cron.unschedule('aqurate-daily-refresh');
