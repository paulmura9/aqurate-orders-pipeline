-- =============================================================================
-- 01_schema.sql - storage layer (idempotent, safe to re-run)
--
-- Layers:
--   public.orders_raw                  landing zone, everything as text
--   public.fx_rates                    daily ECB rates (EUR base) from Frankfurter
--   public.orders_clean                typed + de-duplicated + repaired orders
--   public.orders_rejected             quarantine: rows deliberately not cleaned
--   public.customer_spend_eur          mart, step 4
--   public.revenue_by_country_category mart, step 5
--   ops.*                              run log, quality results, settings
-- =============================================================================

create schema if not exists ops;

-- -----------------------------------------------------------------------------
-- Tunables. Kept in a table so thresholds are auditable and changeable without
-- a redeploy (the €40,000 cut-off from the brief lives here).
-- -----------------------------------------------------------------------------
create table if not exists ops.settings (
    key         text primary key,
    value       text        not null,
    description text,
    updated_at  timestamptz not null default now()
);

insert into ops.settings (key, value, description) values
    ('min_country_revenue_eur', '40000',
     'Step 5: only countries above this combined Books+Electronics revenue are reported'),
    ('breakdown_categories', 'Books,Electronics',
     'Step 5: categories included in the country breakdown'),
    ('revenue_statuses', 'completed',
     'Order statuses that count as revenue (refunded / test are excluded)'),
    ('price_outlier_factor', '20',
     'A unit_price above this many times the median price of the same SKU is treated as a data error'),
    ('fx_max_staleness_days', '7',
     'How far back the FX lookup may carry a rate forward before the conversion is flagged as stale')
on conflict (key) do nothing;

create or replace function ops.setting(p_key text)
returns text language sql stable as $$
    select value from ops.settings where key = p_key;
$$;

-- -----------------------------------------------------------------------------
-- Landing zone. Every column is text: ingestion must never fail on a bad value,
-- that is the cleaning layer's job. The original JSON is kept for auditing.
-- -----------------------------------------------------------------------------
create table if not exists public.orders_raw (
    raw_id            bigint generated always as identity primary key,
    batch_id          uuid        not null,
    ingested_at       timestamptz not null default now(),
    payload           jsonb       not null,
    order_id          text generated always as (payload ->> 'order_id') stored,
    customer_id       text generated always as (payload ->> 'customer_id') stored,
    customer_email    text generated always as (payload ->> 'customer_email') stored,
    order_ts          text generated always as (payload ->> 'order_ts') stored,
    status            text generated always as (payload ->> 'status') stored,
    channel           text generated always as (payload ->> 'channel') stored,
    sku               text generated always as (payload ->> 'sku') stored,
    product_name      text generated always as (payload ->> 'product_name') stored,
    category          text generated always as (payload ->> 'category') stored,
    qty               text generated always as (payload ->> 'qty') stored,
    unit_price        text generated always as (payload ->> 'unit_price') stored,
    currency          text generated always as (payload ->> 'currency') stored,
    country           text generated always as (payload ->> 'country') stored,
    fx_reference_date text generated always as (payload ->> 'fx_reference_date') stored
);

create index if not exists orders_raw_batch_idx on public.orders_raw (batch_id);
create index if not exists orders_raw_order_idx on public.orders_raw (order_id);

-- -----------------------------------------------------------------------------
-- FX. One row per (date, base, quote). Frankfurter publishes ECB reference
-- rates: working days only, no weekends or TARGET holidays.
-- -----------------------------------------------------------------------------
create table if not exists public.fx_rates (
    rate_date      date        not null,
    base_currency  char(3)     not null,
    quote_currency char(3)     not null,
    rate           numeric(20, 10) not null check (rate > 0),
    source         text        not null default 'frankfurter.dev',
    fetched_at     timestamptz not null default now(),
    primary key (rate_date, base_currency, quote_currency)
);

comment on column public.fx_rates.rate is
    'Units of quote_currency for 1 unit of base_currency (base=EUR => rate 5.25 means 1 EUR = 5.25 RON)';

-- -----------------------------------------------------------------------------
-- Cleaned orders. Grain: one row per order line = (order_id, sku).
-- -----------------------------------------------------------------------------
create table if not exists public.orders_clean (
    order_line_id     text        primary key,
    order_id          text        not null,
    customer_id       integer     not null,
    customer_email    text        not null,
    order_ts          timestamp   not null,
    order_date        date        not null,
    status            text        not null,
    channel           text,
    sku               text        not null,
    product_name      text        not null,
    category          text        not null,
    qty               integer     not null check (qty > 0),
    unit_price        numeric(12, 2) not null check (unit_price >= 0),
    currency          char(3)     not null,
    country           char(2)     not null,
    gross_amount      numeric(14, 2) not null,
    fx_reference_date date        not null,
    quality_flags     text[]      not null default '{}',
    cleaned_at        timestamptz not null default now(),
    unique (order_id, sku)
);

create index if not exists orders_clean_customer_idx on public.orders_clean (customer_id);
create index if not exists orders_clean_country_cat_idx on public.orders_clean (country, category);
create index if not exists orders_clean_fx_ref_idx on public.orders_clean (currency, fx_reference_date);

-- -----------------------------------------------------------------------------
-- Quarantine. Rows dropped on purpose, with the reason, so nothing disappears
-- silently and the counts can be reconciled against orders_raw.
-- -----------------------------------------------------------------------------
create table if not exists public.orders_rejected (
    rejected_id  bigint generated always as identity primary key,
    raw_id       bigint,
    order_id     text,
    sku          text,
    reason       text        not null,
    details      text,
    payload      jsonb,
    rejected_at  timestamptz not null default now()
);

create index if not exists orders_rejected_reason_idx on public.orders_rejected (reason);

-- -----------------------------------------------------------------------------
-- Marts
-- -----------------------------------------------------------------------------
create table if not exists public.customer_spend_eur (
    customer_id            integer primary key,
    customer_email         text    not null,
    country                char(2) not null,
    orders_count           integer not null,
    order_lines            integer not null,
    total_spend_eur        numeric(14, 2) not null,
    currencies             text[]  not null,
    first_order_ts         timestamp,
    last_order_ts          timestamp,
    lines_with_stale_fx    integer not null default 0,
    refreshed_at           timestamptz not null default now()
);

create table if not exists public.revenue_by_country_category (
    country              char(2) primary key,
    revenue_eur          numeric(14, 2) not null,
    books_revenue_eur    numeric(14, 2) not null,
    electronics_revenue_eur numeric(14, 2) not null,
    orders_count         integer not null,
    order_lines          integer not null,
    revenue_rank         integer not null,
    refreshed_at         timestamptz not null default now()
);

-- -----------------------------------------------------------------------------
-- Observability
-- -----------------------------------------------------------------------------
create table if not exists ops.pipeline_run (
    run_id       bigint generated always as identity primary key,
    step         text        not null,
    status       text        not null check (status in ('running', 'success', 'failed')),
    started_at   timestamptz not null default now(),
    finished_at  timestamptz,
    duration_ms  integer,
    rows_in      integer,
    rows_out     integer,
    triggered_by text        not null default 'manual',
    message      text,
    details      jsonb
);

create index if not exists pipeline_run_step_started_idx on ops.pipeline_run (step, started_at desc);

create table if not exists ops.quality_check_result (
    check_id    bigint generated always as identity primary key,
    run_id      bigint references ops.pipeline_run (run_id),
    check_name  text        not null,
    severity    text        not null check (severity in ('error', 'warn', 'info')),
    passed      boolean     not null,
    observed    numeric,
    threshold   numeric,
    details     text,
    checked_at  timestamptz not null default now()
);

create index if not exists quality_check_result_checked_idx
    on ops.quality_check_result (checked_at desc);

-- -----------------------------------------------------------------------------
-- Supabase exposes every table in `public` through PostgREST. Enable RLS with
-- no policies: anon/authenticated get nothing, the pipeline connects as
-- service_role / the Postgres user, which bypasses RLS.
-- -----------------------------------------------------------------------------
alter table public.orders_raw                  enable row level security;
alter table public.fx_rates                    enable row level security;
alter table public.orders_clean                enable row level security;
alter table public.orders_rejected             enable row level security;
alter table public.customer_spend_eur          enable row level security;
alter table public.revenue_by_country_category enable row level security;
