SET search_path TO shop;

-- ============================================================
-- STAGING
-- Raw-ish representation of the CSV.
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,

    quantity TEXT NOT NULL,
    unit_price TEXT NOT NULL,
    amount TEXT NOT NULL,

    status TEXT NOT NULL,
    order_date TEXT NOT NULL,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_file TEXT NOT NULL
);


-- ============================================================
-- FACT
-- Clean, typed and deduplicated order data.
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT PRIMARY KEY,

    customer_id INT NOT NULL
        REFERENCES customers(id),

    product_id INT NOT NULL
        REFERENCES products(id),

    product_name TEXT NOT NULL,
    category TEXT NOT NULL,

    quantity INT NOT NULL
        CHECK (quantity > 0),

    unit_price NUMERIC(12, 2) NOT NULL
        CHECK (unit_price >= 0),

    amount NUMERIC(12, 2) NOT NULL
        CHECK (amount >= 0),

    status TEXT NOT NULL
        CHECK (
            status IN (
                'pending',
                'completed',
                'cancelled',
                'refunded'
            )
        ),

    order_date TIMESTAMPTZ NOT NULL,

    loaded_at TIMESTAMPTZ NOT NULL,
    source_file TEXT NOT NULL
);
