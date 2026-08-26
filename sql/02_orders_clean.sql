-- =============================================================================
-- 02_orders_clean.sql - orders_raw -> orders_clean (+ orders_rejected)
--
-- Full rebuild on every run: orders_raw holds a complete snapshot of the source,
-- so a deterministic rebuild is simpler and safer than incremental merging - the
-- same input always produces the same output.
--
-- The transformation lives in the view ops.orders_judged; the function only
-- materialises it into the two output tables.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Cast helpers: the landing zone is text, and one bad value must never abort
-- the whole load.
-- -----------------------------------------------------------------------------
create or replace function ops.safe_numeric(p_value text)
returns numeric language plpgsql immutable as $$
begin
    return p_value::numeric;
exception when others then
    return null;
end;
$$;

create or replace function ops.safe_date(p_value text)
returns date language plpgsql immutable as $$
begin
    return p_value::date;
exception when others then
    return null;
end;
$$;

-- orders_raw.order_ts arrives in three different formats (see README):
--   ISO 8601            2026-04-12T05:12:21
--   European day-first  28/03/2026 03:54
--   Unix epoch seconds  1777564733
create or replace function ops.parse_order_ts(p_value text)
returns timestamp language plpgsql immutable as $$
declare
    v text := btrim(coalesce(p_value, ''));
begin
    if v ~ '^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?)?$' then
        return replace(v, 'T', ' ')::timestamp;
    elsif v ~ '^\d{1,2}/\d{1,2}/\d{4}( \d{1,2}:\d{2}(:\d{2})?)?$' then
        -- day-first: the source contains days > 12 and never a month > 12
        return to_timestamp(v, case when v ~ ':' then 'DD/MM/YYYY HH24:MI' else 'DD/MM/YYYY' end)::timestamp;
    elsif v ~ '^\d{9,11}$' then
        return (to_timestamp(v::bigint) at time zone 'UTC');
    end if;
    return null;
exception when others then
    return null;
end;
$$;

-- SKU spellings differ ("SKU-EL-001", "SKUEL001", "SKU HK 003"): stripping every
-- separator collapses the variants onto one key.
create or replace function ops.normalise_sku(p_value text)
returns text language sql immutable as $$
    select nullif(regexp_replace(upper(coalesce(p_value, '')), '[^A-Z0-9]', '', 'g'), '');
$$;

create or replace function ops.normalise_order_id(p_value text)
returns text language sql immutable as $$
    select nullif(upper(btrim(coalesce(p_value, ''))), '');
$$;

-- -----------------------------------------------------------------------------
-- The transformation. Every row of the newest raw batch comes out of this view
-- exactly once, either with reject_reason set or with dup_rank telling whether
-- it is the surviving copy of an order line.
-- -----------------------------------------------------------------------------
-- dropped first: `create or replace view` cannot change a column's type, so a
-- change to the transformation would otherwise fail on an existing deployment
drop view if exists ops.orders_judged cascade;

create view ops.orders_judged as
with latest_batch as (
    select batch_id
      from public.orders_raw
     order by ingested_at desc, raw_id desc
     limit 1
),
src as (
    select r.raw_id,
           r.payload,
           ops.normalise_order_id(r.order_id)            as order_id,
           r.order_id                                    as order_id_source,
           ops.normalise_sku(r.sku)                      as sku_key,
           btrim(r.sku)                                  as sku_source,
           lower(btrim(r.product_name))                  as product_key,
           btrim(r.product_name)                         as product_name,
           nullif(btrim(r.category), '')                 as category,
           lower(btrim(r.customer_email))                as customer_email,
           ops.safe_numeric(r.customer_id)               as customer_id,
           r.order_ts                                    as order_ts_source,
           ops.parse_order_ts(r.order_ts)                as order_ts,
           lower(btrim(r.status))                        as status,
           lower(btrim(r.channel))                       as channel,
           ops.safe_numeric(r.qty)                       as qty,
           ops.safe_numeric(r.unit_price)                as unit_price,
           upper(btrim(r.currency))                      as currency,
           upper(btrim(r.country))                       as country,
           ops.safe_date(r.fx_reference_date)            as fx_reference_date
      from public.orders_raw r
      join latest_batch b on b.batch_id = r.batch_id
),
-- Product dimension derived from the data itself: the canonical SKU of a
-- product is its most frequent spelling. That also repairs "SKU-FA-O03"
-- (letter O instead of zero) without hard-coding a lookup table.
product_dim as (
    select distinct on (product_key)
           product_key,
           sku_source   as canonical_sku,
           product_name as canonical_product_name
      from (
          select product_key, sku_source, product_name, count(*) as n
            from src
           where product_key is not null
           group by 1, 2, 3
      ) s
     order by product_key, n desc, sku_source
),
category_dim as (
    select distinct on (product_key)
           product_key,
           category as canonical_category
      from (
          select product_key, category, count(*) as n
            from src
           where category is not null
           group by 1, 2
      ) s
     order by product_key, n desc, category
),
resolved as (
    select s.*,
           coalesce(p.canonical_sku, s.sku_source)            as sku_final,
           coalesce(p.canonical_product_name, s.product_name) as product_name_final,
           coalesce(s.category, c.canonical_category)         as category_final,
           -- customer_id is missing on some rows, but the e-mail encodes it
           coalesce(
               s.customer_id,
               nullif(substring(s.customer_email from '^customer(\d+)@'), '')::numeric
           )                                                  as customer_id_final
      from src s
      left join product_dim  p on p.product_key = s.product_key
      left join category_dim c on c.product_key = s.product_key
),
-- Price sanity is judged per SKU: the 999999 sentinel is thousands of times the
-- median price of its own SKU, while genuine prices stay within a factor of ~2.
price_ref as (
    select sku_final,
           -- percentile_cont returns double precision; the rest of the pipeline is numeric
           (percentile_cont(0.5) within group (order by unit_price))::numeric as median_price
      from resolved
     where unit_price is not null
       and unit_price > 0
     group by 1
),
judged as (
    select r.*,
           pr.median_price,
           case
               when r.status = 'test' or r.customer_email like '%@aqurate.ai'
                                                            then 'internal_test_order'
               when r.order_id is null                      then 'missing_order_id'
               when r.sku_final is null                     then 'missing_sku'
               when r.order_ts is null                      then 'unparseable_order_ts'
               when r.customer_id_final is null             then 'missing_customer_id'
               when r.qty is null or r.qty = 0              then 'invalid_qty'
               when r.unit_price is null or r.unit_price < 0 then 'invalid_unit_price'
               when r.currency !~ '^[A-Z]{3}$'              then 'invalid_currency'
               when r.country  !~ '^[A-Z]{2}$'              then 'invalid_country'
               when r.fx_reference_date is null             then 'invalid_fx_reference_date'
               when pr.median_price is not null
                    and r.unit_price >
                        coalesce(ops.setting('price_outlier_factor')::numeric, 20) * pr.median_price
                                                            then 'implausible_unit_price'
           end as reject_reason
      from resolved r
      left join price_ref pr on pr.sku_final = r.sku_final
)
select j.*,
       case when j.reject_reason is null then
           row_number() over (
               partition by j.order_id, j.sku_final
               order by
                   -- prefer the copy whose order_id was already well formatted
                   (j.order_id_source = j.order_id) desc,
                   -- deterministic tie-break: duplicates differ only here
                   j.fx_reference_date,
                   j.raw_id
           )
       end as dup_rank,
       case when j.reject_reason is null then
           count(*) over (partition by j.order_id, j.sku_final)
       end as dup_count
  from judged j;

-- -----------------------------------------------------------------------------
-- Materialise the view into orders_clean / orders_rejected.
-- -----------------------------------------------------------------------------
create or replace function ops.rebuild_orders_clean()
returns integer language plpgsql as $$
declare
    v_rows integer;
begin
    if not exists (select 1 from public.orders_raw) then
        raise exception 'orders_raw is empty - run the ingest step first';
    end if;

    truncate table public.orders_clean;
    truncate table public.orders_rejected;

    insert into public.orders_clean (
        order_line_id, order_id, customer_id, customer_email, order_ts, order_date,
        status, channel, sku, product_name, category, qty, unit_price, currency,
        country, gross_amount, fx_reference_date, quality_flags
    )
    select j.order_id || '::' || j.sku_final,
           j.order_id,
           j.customer_id_final::integer,
           j.customer_email,
           j.order_ts,
           j.order_ts::date,
           j.status,
           j.channel,
           j.sku_final,
           j.product_name_final,
           coalesce(j.category_final, 'Unknown'),
           abs(j.qty)::integer,
           round(j.unit_price, 2),
           j.currency,
           j.country,
           round(abs(j.qty) * j.unit_price, 2),
           j.fx_reference_date,
           array_remove(array[
               case when j.order_id_source is distinct from j.order_id then 'order_id_reformatted'  end,
               case when j.sku_source is distinct from j.sku_final     then 'sku_canonicalised'     end,
               case when j.category is null                            then 'category_backfilled'   end,
               case when j.customer_id is null                         then 'customer_id_recovered' end,
               case when j.qty < 0                                     then 'qty_sign_flipped'      end,
               case when j.unit_price = 0                              then 'zero_unit_price'       end,
               case when j.order_ts_source ~ '^\d+$'                   then 'order_ts_epoch'        end,
               case when j.order_ts_source ~ '^\d{1,2}/'               then 'order_ts_day_first'    end,
               case when j.dup_count > 1                               then 'deduplicated'          end
           ], null)
      from ops.orders_judged j
     where j.reject_reason is null
       and j.dup_rank = 1;

    get diagnostics v_rows = row_count;

    insert into public.orders_rejected (raw_id, order_id, sku, reason, details, payload)
    select j.raw_id,
           coalesce(j.order_id, j.order_id_source),
           coalesce(j.sku_final, j.sku_source),
           coalesce(j.reject_reason, 'duplicate_order_line'),
           case
               when j.reject_reason = 'implausible_unit_price'
                   then format('unit_price %s vs median %s for the same SKU',
                               j.unit_price, round(j.median_price, 2))
               when j.reject_reason = 'internal_test_order'
                   then format('status=%s, email=%s', j.status, j.customer_email)
               when j.reject_reason is null
                   then format('duplicate order line (%s copies kept 1)', j.dup_count)
           end,
           j.payload
      from ops.orders_judged j
     where j.reject_reason is not null
        or j.dup_rank > 1;

    return v_rows;
end;
$$;
