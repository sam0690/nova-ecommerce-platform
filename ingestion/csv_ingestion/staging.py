from collections.abc import Sequence

import psycopg

from src.models import Order


def copy_to_staging(
    conn: psycopg.Connection,
    rows: Sequence[Order],
    source_file: str,
) -> None:
    """Insert a batch of validated orders into staging using COPY."""

    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY shop.stg_orders (
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
                source_file
            )
            FROM STDIN
            """
        ) as copy:

            for order in rows:
                copy.write_row(
                    (
                        order.order_id,
                        order.customer_id,
                        order.product_id,
                        order.product_name,
                        order.category,
                        order.quantity,
                        order.unit_price,
                        order.amount,
                        order.status,
                        order.order_date,
                        source_file,
                    )
                )
