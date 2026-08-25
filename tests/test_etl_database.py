from ingestion.csv_ingestion.db import get_connection
from ingestion.csv_ingestion.fact_loader import load_fact_orders
from ingestion.csv_ingestion.staging_loader import load_staging


def test_staging_to_fact() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE shop.stg_orders, shop.fact_orders")

        conn.commit()

        # We'll use a small in-memory JSONL input for this test.
        from io import StringIO

        data = """{"order_id":"TEST-001","customer_id":1,"product_id":1,"product_name":"Mechanical Keyboard 75%","category":"peripherals","quantity":1,"unit_price":"129.00","amount":"129.00","status":"completed","order_date":"2026-08-01T10:00:00Z"}
{"order_id":"TEST-002","customer_id":2,"product_id":2,"product_name":"Wireless Mouse","category":"peripherals","quantity":2,"unit_price":"49.50","amount":"99.00","status":"completed","order_date":"2026-08-01T11:00:00Z"}
"""

        loaded = load_staging(
            conn,
            StringIO(data),
            "orders_2026-08-01.csv",
        )

        assert loaded == 2

        fact_loaded = load_fact_orders(conn)

        assert fact_loaded == 2

        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM shop.fact_orders")

            result = cur.fetchone()

            assert result is not None
            assert result[0] == 2
