-- =============================================================================
-- 04_quality_checks.sql - assertions + the daily orchestrator + health views
--
-- The checks are the answer to "how would I know if the daily job silently
-- failed?": every run writes its result, and a run that produces no rows, loses
-- rows, or converts with a stale rate fails loudly instead of quietly
-- publishing a wrong number.
-- =============================================================================

create or replace function ops.run_quality_checks(p_run_id bigint default null)
returns integer language plpgsql as $$
declare
    v_failed  integer;
    v_max_lag integer := coalesce(ops.setting('fx_max_staleness_days')::integer, 7);
begin
    insert into ops.quality_check_result (run_id, check_name, severity, passed, observed, threshold, details)

    -- 1. the cleaned table is not empty
    select p_run_id, 'orders_clean_not_empty', 'error', count(*) > 0, count(*), 1,
           'rows in orders_clean'
      from public.orders_clean

    -- 2. nothing vanished between raw and clean: kept + rejected = raw
    union all
    select p_run_id, 'raw_rows_reconciled', 'error',
           (select count(*) from public.orders_clean) + (select count(*) from public.orders_rejected)
               = (select count(*) from public.orders_raw r
                   where r.batch_id = (select batch_id from public.orders_raw
                                        order by ingested_at desc, raw_id desc limit 1)),
           (select count(*) from public.orders_clean) + (select count(*) from public.orders_rejected),
           (select count(*) from public.orders_raw r
             where r.batch_id = (select batch_id from public.orders_raw
                                  order by ingested_at desc, raw_id desc limit 1)),
           'clean + rejected must equal the rows of the latest raw batch'

    -- 3. rejection rate stays in the expected range
    union all
    select p_run_id, 'rejection_rate', 'warn',
           coalesce(count(*)::numeric / nullif((select count(*) from public.orders_raw r
                where r.batch_id = (select batch_id from public.orders_raw
                                     order by ingested_at desc, raw_id desc limit 1)), 0), 0) <= 0.10,
           round(coalesce(count(*)::numeric / nullif((select count(*) from public.orders_raw r
                where r.batch_id = (select batch_id from public.orders_raw
                                     order by ingested_at desc, raw_id desc limit 1)), 0), 0), 4),
           0.10,
           'share of raw rows quarantined'
      from public.orders_rejected

    -- 4. every non-EUR line found a rate
    union all
    select p_run_id, 'fx_rate_available', 'error', count(*) = 0, count(*), 0,
           'order lines without any usable FX rate'
      from public.orders_clean_eur
     where fx_status = 'missing'

    -- 5. FX table itself is fresh (ECB publishes on working days only)
    union all
    select p_run_id, 'fx_rates_fresh', 'error',
           coalesce(max(rate_date), date '1970-01-01') >= current_date - 5,
           coalesce(current_date - max(rate_date), 9999), 5,
           'days since the most recent FX rate'
      from public.fx_rates

    -- 6. the marts were rebuilt today
    union all
    select p_run_id, 'customer_spend_fresh', 'error',
           coalesce(max(refreshed_at), 'epoch'::timestamptz) > now() - interval '23 hours',
           extract(epoch from now() - coalesce(max(refreshed_at), 'epoch'::timestamptz)) / 3600, 23,
           'hours since customer_spend_eur was refreshed'
      from public.customer_spend_eur

    -- 7. the mart total equals a straight recomputation from the clean layer
    union all
    select p_run_id, 'spend_totals_match', 'error',
           abs(coalesce((select sum(total_spend_eur) from public.customer_spend_eur), 0)
             - coalesce((select sum(amount_eur) from public.orders_clean_eur
                          where status = any (string_to_array(coalesce(ops.setting('revenue_statuses'), 'completed'), ','))
                            and amount_eur is not null), 0)) < 1,
           abs(coalesce((select sum(total_spend_eur) from public.customer_spend_eur), 0)
             - coalesce((select sum(amount_eur) from public.orders_clean_eur
                          where status = any (string_to_array(coalesce(ops.setting('revenue_statuses'), 'completed'), ','))
                            and amount_eur is not null), 0)), 1,
           'EUR difference between the mart and orders_clean_eur'

    -- 8. no negative or null spend slipped through
    union all
    select p_run_id, 'spend_non_negative', 'error', count(*) = 0, count(*), 0,
           'customers with negative or missing total spend'
      from public.customer_spend_eur
     where total_spend_eur is null or total_spend_eur < 0

    -- 9. the country breakdown respects its own threshold
    union all
    select p_run_id, 'country_threshold_respected', 'error', count(*) = 0, count(*), 0,
           'rows below the configured revenue threshold'
      from public.revenue_by_country_category
     where revenue_eur <= coalesce(ops.setting('min_country_revenue_eur')::numeric, 40000)

    -- 10. how much of the revenue still rests on a provisional/stale rate
    union all
    select p_run_id, 'fx_conversions_current', 'warn', count(*) = 0, count(*), 0,
           'order lines converted with a provisional or stale rate'
      from public.orders_clean_eur
     where fx_status = 'provisional' or fx_lag_days > v_max_lag;

    select count(*) into v_failed
      from ops.quality_check_result
     where run_id is not distinct from p_run_id
       and severity = 'error'
       and not passed
       and checked_at > now() - interval '1 hour';

    return v_failed;
end;
$$;

-- -----------------------------------------------------------------------------
-- Daily orchestration inside the database: clean -> marts -> checks, all in one
-- transaction and all logged. Called by the Python runner and, optionally, by
-- pg_cron (see 05_pg_cron_optional.sql).
-- -----------------------------------------------------------------------------
create or replace function ops.run_daily(p_triggered_by text default 'manual')
returns bigint language plpgsql as $$
declare
    v_run_id     bigint;
    v_started    timestamptz := clock_timestamp();
    v_clean_rows integer;
    v_customers  integer;
    v_countries  integer;
    v_failed     integer;
begin
    insert into ops.pipeline_run (step, status, triggered_by)
    values ('transform', 'running', p_triggered_by)
    returning run_id into v_run_id;

    v_clean_rows := ops.rebuild_orders_clean();
    select m.customers, m.countries into v_customers, v_countries from ops.refresh_marts() m;
    v_failed := ops.run_quality_checks(v_run_id);

    update ops.pipeline_run
       set status      = case when v_failed = 0 then 'success' else 'failed' end,
           finished_at = now(),
           duration_ms = (extract(epoch from clock_timestamp() - v_started) * 1000)::integer,
           rows_in     = (select count(*) from public.orders_raw
                           where batch_id = (select batch_id from public.orders_raw
                                              order by ingested_at desc, raw_id desc limit 1)),
           rows_out    = v_clean_rows,
           message     = format('orders_clean=%s customers=%s countries=%s failed_checks=%s',
                                v_clean_rows, v_customers, v_countries, v_failed),
           details     = jsonb_build_object('customers', v_customers, 'countries', v_countries,
                                            'failed_checks', v_failed)
     where run_id = v_run_id;

    if v_failed > 0 then
        raise exception 'daily refresh failed % error-level quality check(s) - see ops.quality_check_result for run_id %',
            v_failed, v_run_id;
    end if;

    return v_run_id;
end;
$$;

-- -----------------------------------------------------------------------------
-- Health views - what a human (or an alert) looks at.
-- -----------------------------------------------------------------------------
create or replace view ops.pipeline_health as
select r.step,
       r.status,
       r.finished_at,
       round(extract(epoch from now() - coalesce(r.finished_at, r.started_at)) / 3600, 1) as hours_since,
       r.rows_out,
       r.duration_ms,
       r.triggered_by,
       r.message,
       -- a job that stopped running is as bad as a job that failed
       (r.status <> 'success' or coalesce(r.finished_at, r.started_at) < now() - interval '26 hours')
           as needs_attention
  from (
      select distinct on (step) *
        from ops.pipeline_run
       order by step, started_at desc
  ) r;

create or replace view ops.latest_quality_issues as
select q.check_name, q.severity, q.observed, q.threshold, q.details, q.checked_at
  from ops.quality_check_result q
 where q.checked_at > now() - interval '48 hours'
   and not q.passed
 order by q.checked_at desc, (q.severity = 'error') desc;
