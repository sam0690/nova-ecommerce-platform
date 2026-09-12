-- Revenue by country, as-of the order date.
--
-- The point of this model is what it does NOT do: it never reads a customer's
-- country as it is today. It reads the country that was true when the order was
-- placed, by joining to the version of the customer that was open at that
-- instant. Move a customer to a new country tomorrow and last quarter's numbers
-- here do not change. Join to dim_customers instead and they do -- see
-- analyses/country_revenue_drift.sql for the two side by side.

WITH customer_versions AS (

    SELECT
        id,
        country,
        -- The snapshot's history starts the day the first `dbt snapshot` ran,
        -- so every version-1 row carries that run's timestamp as its
        -- dbt_valid_from. Every order placed before then would fall outside
        -- all versions and vanish from this join. Backdating the first
        -- version to -infinity states the assumption explicitly: before we
        -- started recording, the original attributes are all we know.
        CASE
            WHEN dbt_valid_from = MIN(dbt_valid_from) OVER (PARTITION BY id)
                THEN '-infinity'::timestamptz
            ELSE dbt_valid_from
        END AS valid_from,
        dbt_valid_to AS valid_to
    FROM {{ ref('scd_customers') }}

)

SELECT
    v.country,
    date_trunc('quarter', o.order_date) AS quarter,
    count(*)          AS orders,
    sum(o.amount)     AS revenue
FROM {{ ref('fct_orders') }} o
JOIN customer_versions v
    ON o.customer_id = v.id
    -- Half-open interval, deliberately: [valid_from, valid_to).
    -- BETWEEN is inclusive at both ends, so an order landing on the exact
    -- instant of a country change would match the closing row AND the
    -- opening one and be counted twice.
    AND o.order_date >= v.valid_from
    AND o.order_date <  v.valid_to
WHERE o.status = 'completed'
GROUP BY 1, 2
