-- =============================================================================
-- 03_marts.sql - FX conversion + the two reporting tables (steps 4 and 5)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- One place where an order line is expressed in EUR.
--
-- FX rule: take the most recent published rate on or before fx_reference_date.
-- One rule covers all three real-world cases:
--   * fx_reference_date is a working day  -> that day's rate            (exact)
--   * weekend / TARGET holiday            -> the previous working day   (carried_forward)
--   * fx_reference_date is in the future  -> the latest rate we have    (provisional)
-- The rate actually used is stored alongside the amount, so any figure can be
-- explained afterwards, and provisional lines self-correct once the real rate
-- is published (the marts are rebuilt daily).
-- -----------------------------------------------------------------------------
drop view if exists public.orders_clean_eur cascade;

create view public.orders_clean_eur as
select o.*,
       f.rate_date                                            as fx_rate_date,
       case when o.currency = 'EUR' then 1 else f.rate end     as fx_rate,
       case when o.currency = 'EUR' then 0
            else (o.fx_reference_date - f.rate_date) end       as fx_lag_days,
       case
           when o.currency = 'EUR'                       then 'base'
           when f.rate is null                           then 'missing'
           when o.fx_reference_date > current_date       then 'provisional'
           when f.rate_date = o.fx_reference_date        then 'exact'
           else 'carried_forward'
       end                                                    as fx_status,
       case
           when o.currency = 'EUR' then o.gross_amount
           when f.rate is null     then null
           else round(o.gross_amount / f.rate, 2)
       end                                                    as amount_eur
  from public.orders_clean o
  left join lateral (
      select r.rate_date, r.rate
        from public.fx_rates r
       where r.base_currency  = 'EUR'
         and r.quote_currency = o.currency
         and r.rate_date     <= o.fx_reference_date
       order by r.rate_date desc
       limit 1
  ) f on o.currency <> 'EUR';

comment on view public.orders_clean_eur is
    'orders_clean with the EUR amount, the FX rate used and how that rate was obtained';

-- -----------------------------------------------------------------------------
-- Step 4 + step 5, rebuilt together so the two tables are always consistent
-- with each other and with the same orders_clean snapshot.
-- -----------------------------------------------------------------------------
create or replace function ops.refresh_marts()
returns table (customers integer, countries integer) language plpgsql as $$
declare
    v_statuses    text[]  := string_to_array(coalesce(ops.setting('revenue_statuses'), 'completed'), ',');
    v_categories  text[]  := string_to_array(coalesce(ops.setting('breakdown_categories'), 'Books,Electronics'), ',');
    v_min_revenue numeric := coalesce(ops.setting('min_country_revenue_eur')::numeric, 40000);
    v_max_lag     integer := coalesce(ops.setting('fx_max_staleness_days')::integer, 7);
    v_customers   integer;
    v_countries   integer;
begin
    -- Refunded and internal-test lines are not spend; test lines never reach
    -- orders_clean, refunded ones do (they are real orders) but are excluded here.
    truncate table public.customer_spend_eur;

    insert into public.customer_spend_eur (
        customer_id, customer_email, country, orders_count, order_lines,
        total_spend_eur, currencies, first_order_ts, last_order_ts,
        lines_with_stale_fx, refreshed_at
    )
    select o.customer_id,
           min(o.customer_email)                                       as customer_email,
           (array_agg(o.country order by o.order_ts desc))[1]          as country,
           count(distinct o.order_id)                                  as orders_count,
           count(*)                                                    as order_lines,
           round(sum(o.amount_eur), 2)                                 as total_spend_eur,
           array_agg(distinct o.currency)                              as currencies,
           min(o.order_ts)                                             as first_order_ts,
           max(o.order_ts)                                             as last_order_ts,
           count(*) filter (
               where o.fx_status = 'provisional' or o.fx_lag_days > v_max_lag
           )                                                           as lines_with_stale_fx,
           now()
      from public.orders_clean_eur o
     where o.status = any (v_statuses)
       and o.amount_eur is not null
     group by o.customer_id;

    get diagnostics v_customers = row_count;

    -- Step 5: Books + Electronics revenue per country, only above the threshold.
    truncate table public.revenue_by_country_category;

    insert into public.revenue_by_country_category (
        country, revenue_eur, books_revenue_eur, electronics_revenue_eur,
        orders_count, order_lines, revenue_rank, refreshed_at
    )
    with per_country as (
        select o.country,
               round(sum(o.amount_eur), 2)                                                as revenue_eur,
               round(sum(o.amount_eur) filter (where o.category = 'Books'), 2)             as books_revenue_eur,
               round(sum(o.amount_eur) filter (where o.category = 'Electronics'), 2)       as electronics_revenue_eur,
               count(distinct o.order_id)                                                 as orders_count,
               count(*)                                                                   as order_lines
          from public.orders_clean_eur o
         where o.status = any (v_statuses)
           and o.category = any (v_categories)
           and o.amount_eur is not null
         group by o.country
    )
    select country,
           revenue_eur,
           coalesce(books_revenue_eur, 0),
           coalesce(electronics_revenue_eur, 0),
           orders_count,
           order_lines,
           rank() over (order by revenue_eur desc),
           now()
      from per_country
     where revenue_eur > v_min_revenue;

    get diagnostics v_countries = row_count;

    return query select v_customers, v_countries;
end;
$$;

-- The view must respect the RLS of its base tables (Postgres defaults to
-- definer rights, which would expose the data through Supabase's REST API).
alter view public.orders_clean_eur set (security_invoker = on);
