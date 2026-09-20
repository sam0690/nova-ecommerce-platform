-- Restated net revenue must reproduce the number the warehouse already had.
--
-- Group the events by order_date and a refunded order nets to zero there
-- (+amount booking, -amount refund, same day), which is exactly what the old
-- `status = 'completed'` filter did by deleting it. So the two must agree,
-- day for day. If they do not, the event model has dropped a refund, counted
-- one twice, or let the wrong statuses in.
--
-- This also catches an operational drift: a newly loaded order arriving with
-- status = 'refunded' but no row in shop.refunds. The backfill is a one-shot
-- script, not part of the daily DAG -- see PROGRESS.md.

WITH restated AS (

    SELECT
        order_date::date     AS day,
        SUM(signed_amount)   AS net
    FROM {{ ref('fct_revenue_events') }}
    GROUP BY 1

),

completed_only AS (

    SELECT
        order_date::date  AS day,
        SUM(amount)       AS revenue
    FROM {{ ref('fct_orders') }}
    WHERE status = 'completed'
    GROUP BY 1

)

SELECT
    day,
    net,
    revenue
FROM restated
FULL OUTER JOIN completed_only USING (day)
WHERE COALESCE(net, 0) <> COALESCE(revenue, 0)
