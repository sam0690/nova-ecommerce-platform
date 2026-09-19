SET search_path TO shop;

-- ============================================================
-- REFUNDS
-- A refund is an event, not a flag on the order.
-- Grain: one row per refunded order.
--
-- order_date is denormalised on purpose: a CHECK cannot look at
-- another table, and "the refund cannot precede the order" is the
-- one invariant worth enforcing in the schema rather than in Python.
-- ============================================================

CREATE TABLE IF NOT EXISTS refunds (
    -- ponytail: order_id IS the PK, so the grain is enforced, not merely
    -- intended, and the backfill gets its ON CONFLICT target for free.
    -- Partial refunds arrive -> surrogate refund_id + UNIQUE (order_id, seq).
    order_id TEXT PRIMARY KEY
        REFERENCES fact_orders(order_id),

    order_date TIMESTAMPTZ NOT NULL,
    refund_date TIMESTAMPTZ NOT NULL,

    -- Positive, i.e. "money that came back". Downstream subtracts it.
    amount NUMERIC(12, 2) NOT NULL
        CHECK (amount > 0),

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT refund_not_before_order
        CHECK (refund_date >= order_date)
);
