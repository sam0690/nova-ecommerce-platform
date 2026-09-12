-- The acceptance experiment, not a shipped model. Compile and run it:
--
--   dbt compile --project-dir nova_analytics --select country_revenue_drift
--   psql -h localhost -p 5434 -U nova -d nova \
--     -f nova_analytics/target/compiled/nova_analytics/analyses/country_revenue_drift.sql
--
-- Run it once, note the numbers. Then change a customer's country, rerun
-- `dbt snapshot` and `dbt build`, and run it again. as_of_revenue must be
-- identical across both runs; current_revenue will have moved. drift is the
-- history that joining to dim_customers silently rewrote.

WITH as_of AS (

    SELECT country, sum(revenue) AS revenue
    FROM {{ ref('revenue_by_country') }}
    WHERE quarter = date_trunc('quarter', DATE '2026-07-01')
    GROUP BY 1

), current_dim AS (

    SELECT c.country, sum(o.amount) AS revenue
    FROM {{ ref('fct_orders') }} o
    JOIN {{ ref('dim_customers') }} c
        ON o.customer_id = c.customer_id
    WHERE o.status = 'completed'
      AND date_trunc('quarter', o.order_date) = date_trunc('quarter', DATE '2026-07-01')
    GROUP BY 1

)

SELECT
    coalesce(a.country, c.country)                          AS country,
    a.revenue                                               AS as_of_revenue,
    c.revenue                                               AS current_revenue,
    coalesce(c.revenue, 0) - coalesce(a.revenue, 0)         AS drift
FROM as_of a
FULL OUTER JOIN current_dim c ON a.country = c.country
ORDER BY 1
