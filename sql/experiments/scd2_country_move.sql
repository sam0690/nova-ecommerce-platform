-- SCD2 acceptance experiment: move one customer to another country and watch
-- point-in-time revenue hold still while current-dimension revenue moves.
-- Companion to nova_analytics/analyses/country_revenue_drift.sql.
--
-- This file MUTATES shop.customers. It is not a migration and must not be run
-- as one -- run the statements individually, in order, by hand.


-- ============================================================
-- 1. Pick a customer who actually has Q3 revenue.
--
-- Updating a customer with no completed orders proves nothing: drift would be
-- 0.00 and you would read that as "the point-in-time join worked" when in fact
-- nothing was tested. Run this first and choose from the top of the list.
-- ============================================================

SELECT
    c.id,
    c.name,
    c.country,
    count(*) FILTER (WHERE o.status = 'completed')      AS completed_orders,
    sum(o.amount) FILTER (WHERE o.status = 'completed') AS q3_completed_revenue
FROM shop.customers c
JOIN shop.fact_orders o
    ON o.customer_id = c.id
WHERE date_trunc('quarter', o.order_date) = date_trunc('quarter', DATE '2026-07-01')
GROUP BY c.id, c.name, c.country
HAVING sum(o.amount) FILTER (WHERE o.status = 'completed') > 0
ORDER BY q3_completed_revenue DESC;


-- ============================================================
-- 2. Record the original value BEFORE changing it.
--
-- The snapshot will preserve it, but you want it here in plain sight so the
-- rollback at the bottom is a fact rather than a memory.
--
--     id = ____   name = ____________   original country = ____
--
-- ============================================================


-- ============================================================
-- 3. The move.
--
-- Target must be a country ALREADY IN shop.customers (IN, NG, SE, MX, GB, PK,
-- SG, JP, PT, DE). Then drift reads as a transfer: one country down by exactly
-- what another goes up by, netting to zero. A brand-new country instead gives
-- you a row appearing from nowhere and a column that does not balance.
--
-- Fill in both blanks and run it on its own.
-- ============================================================

-- UPDATE shop.customers SET country = 'SE' WHERE id = 10;


-- ============================================================
-- 4. Rollback, when you are done with the experiment.
--
-- Note what this does NOT do: it does not erase history. The snapshot records
-- a third version, and scd_customers keeps all three. That is the point --
-- SCD2 has no undo, only more history.
-- ============================================================

-- UPDATE shop.customers SET country = '<ORIGINAL_CODE>' WHERE id = <ID>;
