{{ config(materialized='table') }}

SELECT
    to_char(d, 'YYYYMMDD')::int    AS date_key,
    d::date                        AS full_date,
    extract(year    FROM d)::int   AS year,
    extract(quarter FROM d)::int   AS quarter,
    extract(month   FROM d)::int   AS month,
    to_char(d, 'FMMonth')          AS month_name,
    extract(day     FROM d)::int   AS day_of_month,
    extract(isodow  FROM d)::int   AS day_of_week,
    to_char(d, 'FMDay')            AS day_name,
    extract(isodow  FROM d) >= 6   AS is_weekend,
    extract(week    FROM d)::int   AS iso_week
FROM generate_series(
    DATE '2025-01-01',
    DATE '2026-12-31',
    INTERVAL '1 day'
) d
