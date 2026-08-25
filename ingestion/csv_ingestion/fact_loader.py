import psycopg


def load_fact_orders(
    conn: psycopg.Connection,
) -> int:
    """Load staging rows into fact_orders idempotently."""

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO shop.fact_orders (
                order_id,
                customer_id,
                product_id,
                product_name,
                category,
                quantity,
                unit_price,
                amount,
                status,
                order_date,
                loaded_at,
                source_file
            )
            SELECT
                order_id,
                customer_id::INTEGER,
                product_id::INTEGER,
                product_name,
                category,
                quantity::INTEGER,
                unit_price::NUMERIC(12,2),
                amount::NUMERIC(12,2),
                status,
                order_date::TIMESTAMPTZ,
                loaded_at,
                source_file
            FROM shop.stg_orders
            ON CONFLICT (order_id)
            DO NOTHING
            """
        )

        return cur.rowcount
