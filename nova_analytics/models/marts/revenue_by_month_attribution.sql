-- The same revenue, twice, attributed two different ways.
--
-- This model exists because of a charting constraint, not an analytical one.
-- A chart has ONE x-axis dimension, and the two numbers come from grouping
-- fct_revenue_events by two DIFFERENT date columns. They cannot share an axis
-- while they are two columns on one row.
--
-- So the shape changes: one row per (month, attribution) instead of one row
-- per month with two columns. LONG format rather than WIDE. Long is what every
-- "compare these series" chart wants -- the tagging column becomes the pivot,
-- and the two series land on one shared y-axis. Two separate charts would let
-- each autoscale its own axis, which is how you accidentally hide the gap you
-- built the chart to show.

WITH as_booked AS (

    -- Revenue recognised on the day the event happened. A refund hits the
    -- month it was issued in, so closed months stay closed.
    SELECT
        date_trunc('month', event_date)::date AS month,
        'as_booked'                           AS attribution,
        SUM(signed_amount)                    AS net_revenue
    FROM {{ ref('fct_revenue_events') }}
    GROUP BY 1

),

restated AS (

    -- Revenue attributed back to the month the order was placed. A September
    -- refund reaches back and reduces July. History moves.
    SELECT
        date_trunc('month', order_date)::date AS month,
        'restated'                            AS attribution,
        SUM(signed_amount)                    AS net_revenue
    FROM {{ ref('fct_revenue_events') }}
    GROUP BY 1

)

SELECT * FROM as_booked
UNION ALL
SELECT * FROM restated
