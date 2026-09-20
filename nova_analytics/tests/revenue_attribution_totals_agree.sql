-- Both attributions must total the same money.
--
-- They are the same rows summed with different GROUP BY keys, so the grand
-- total cannot move -- a refund is a transfer between months, never a change
-- in the amount. Anything else means a UNION ALL leg is dropping rows or
-- double-counting them, and the chart would be a confident lie.
--
-- This is the one assertion the chart cannot make for you: a wrong total looks
-- exactly like a real trend.

WITH totals AS (

    SELECT
        attribution,
        SUM(net_revenue) AS total
    FROM {{ ref('revenue_by_month_attribution') }}
    GROUP BY 1

)

SELECT *
FROM totals
WHERE total <> (SELECT MIN(total) FROM totals)
