import pytest

from ingestion.csv_ingestion.db import get_connection
from ingestion.csv_ingestion.staging import copy_to_staging
from ingestion.csv_ingestion.fact_loader import load_fact_orders
from src.models import Order


def test_failed_load_rolls_back() -> None:
    order = Order.model_validate(
        {
            "order_id": "ATOMIC-TEST-001",
            "customer_id": "1",
            "product_id": "1",
            "product_name": "Test Product",
            "category": "test",
            "quantity": "1",
            "unit_price": "10.00",
            "amount": "10.00",
            "status": "completed",
            "order_date": "2026-08-01T10:00:00Z",
        }
    )

    with get_connection() as conn:
        try:
            copy_to_staging(
                conn,
                [order],
                "atomic-test.csv",
            )

            # Simulate a failure after staging.
            raise RuntimeError("Simulated ETL failure")

        except Exception:
            conn.rollback()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM shop.stg_orders
                WHERE order_id = 'ATOMIC-TEST-001'
                """
            )

            result = cur.fetchone()

            assert result is not None
            assert result[0] == 0
