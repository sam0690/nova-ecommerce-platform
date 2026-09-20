-- One row per revenue *event*, not one row per order.
--
-- A booking and a refund are two different things that happened on two
-- different days. Storing the refund as a status on the order flattened them
-- into one row with one date, and that is what made revenue restate itself
-- silently: the only date available was the order's, so a September refund
-- had no choice but to land in July.
--
-- Every row here carries BOTH dates, which is the whole point:
--
--   group by event_date  -> net revenue AS BOOKED  (July keeps July's money,
--                           September takes the refund)
--   group by order_date  -> net revenue RESTATED   (the refund reaches back
--                           and reduces July)
--
-- Same rows, same sum, two different answers to "which day does this belong
-- to". That is not a bug to fix, it is an accounting policy to choose -- see
-- Step 33.

WITH bookings AS (

    -- status IN ('completed','refunded') and not just 'completed': a refunded
    -- order WAS revenue on the day it was booked. That is precisely the fact
    -- the old completed-only filter threw away. 'cancelled' and 'pending'
    -- stay out because they never became revenue at all.
    SELECT
        order_id,
        'booking'  AS event_type,
        order_date AS event_date,
        order_date,
        amount     AS signed_amount
    FROM {{ ref('fct_orders') }}
    WHERE status IN ('completed', 'refunded')

),

refunds AS (

    -- No join back to fct_orders. shop.refunds already carries order_date --
    -- denormalised on purpose in migration 004 so the CHECK constraint could
    -- see it -- and the FK guarantees every refund has a real order behind it.
    SELECT
        order_id,
        'refund'    AS event_type,
        refund_date AS event_date,
        order_date,
        -amount     AS signed_amount
    FROM {{ source('shop', 'refunds') }}

),

events AS (

    SELECT * FROM bookings
    UNION ALL
    SELECT * FROM refunds

)

SELECT
    -- Grain is (order, event_type), so that pair is the key. One refund per
    -- order today; partial refunds would break this, the same way they would
    -- break shop.refunds' PK, and both would change together.
    order_id || ':' || event_type AS event_id,
    order_id,
    event_type,
    event_date,
    order_date,
    signed_amount
FROM events
