"""Backfill shop.refunds from orders already sitting in shop.fact_orders.

Every order with status = 'refunded' gets exactly one refund row, dated
order_date + a seeded random lag. Rerunnable: ON CONFLICT DO NOTHING against
the order_id primary key, so a second run inserts zero rows.

    python -m scripts.backfill_refunds
"""

import random
from datetime import datetime, timedelta, timezone

from ingestion.csv_ingestion.db import get_connection

SEED = 42


def refund_lag_days() -> int:
    """Days between order and refund.

    Two-speed: most refunds settle inside a fortnight, a long tail drags on.
    The tail is the point of this step -- it is what pushes refunds into a
    later month than their order and creates the restatement problem.
    """

    if random.random() < 0.7:
        return random.randint(1, 10)

    return random.randint(11, 75)


def refund_date_for(order_date: datetime, now: datetime) -> datetime:
    """order_date + a seeded lag, never in the future.

    The order is already marked refunded today, so a refund dated next month
    is a contradiction -- and Step 31's as-of metrics would never see it. A
    recent order whose lag overshoots gets the lag redrawn inside the window
    that actually exists.
    """

    max_lag = max((now - order_date).days, 0)
    lag = refund_lag_days()

    if lag > max_lag:
        lag = random.randint(0, max_lag)

    return order_date + timedelta(days=lag)


def main() -> None:
    random.seed(SEED)
    now = datetime.now(timezone.utc)

    with get_connection() as conn, conn.cursor() as cur:
        # ORDER BY order_id: the RNG is only reproducible if the rows are
        # handed to it in the same sequence every run.
        cur.execute(
            """
            SELECT order_id, order_date, amount
            FROM shop.fact_orders
            WHERE status = 'refunded'
            ORDER BY order_id
            """
        )

        rows = [
            (order_id, order_date, refund_date_for(order_date, now), amount)
            for order_id, order_date, amount in cur.fetchall()
        ]

        cur.executemany(
            """
            INSERT INTO shop.refunds (order_id, order_date, refund_date, amount)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """,
            rows,
        )
        inserted = cur.rowcount

        cur.execute(
            """
            SELECT COUNT(*)
            FROM shop.refunds
            WHERE DATE_TRUNC('month', refund_date)
               <> DATE_TRUNC('month', order_date)
            """
        )
        crossed = cur.fetchone()[0]

    print(f"{len(rows)} refunded orders, {inserted} rows inserted")
    print(f"{crossed} refunds land in a different month than their order")


if __name__ == "__main__":
    main()
