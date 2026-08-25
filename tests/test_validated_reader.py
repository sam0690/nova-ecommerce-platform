import io

from ingestion.csv_ingestion.validated_reader import read_validated_orders


def test_read_validated_orders() -> None:
    data = """{"order_id":"ORD-1","customer_id":1,"product_id":1,"product_name":"Mouse","category":"Electronics","quantity":1,"unit_price":"29.99","amount":"29.99","status":"completed","order_date":"2026-08-01T10:00:00Z"}
{"order_id":"ORD-2","customer_id":2,"product_id":1,"product_name":"Mouse","category":"Electronics","quantity":2,"unit_price":"29.99","amount":"59.98","status":"completed","order_date":"2026-08-01T11:00:00Z"}
"""

    file = io.StringIO(data)

    orders = list(read_validated_orders(file))

    assert len(orders) == 2
    assert orders[0].order_id == "ORD-1"
    assert orders[1].order_id == "ORD-2"
