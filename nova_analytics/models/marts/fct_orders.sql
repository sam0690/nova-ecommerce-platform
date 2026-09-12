{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    order_id,
    customer_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    amount,
    status,
    order_date
FROM {{ source('shop', 'fact_orders') }}

{% if is_incremental() %}
-- Lookback window. Every run reprocesses the last 7 days instead of only
-- what is strictly new, so a retried task or a late-arriving day lands
-- without a full rebuild. The overlap is harmless because unique_key makes
-- the write an upsert -- the same contract as ON CONFLICT DO NOTHING in
-- ingestion/csv_ingestion/fact_loader.py.
--
-- What this does NOT cover: a backfill older than 7 days. Those rows sit
-- below the window and will never appear here, silently. After replaying
-- any date older than a week, run:
--
--     dbt build --full-refresh --select fct_orders
--
-- COALESCE guards the empty table: MAX() over no rows is NULL, and
-- `order_date >= NULL` is NULL, which filters out everything and leaves the
-- table empty forever without erroring.
WHERE order_date >= COALESCE(
    (SELECT MAX(order_date) FROM {{ this }}) - INTERVAL '7 days',
    '-infinity'::timestamptz
)
{% endif %}
