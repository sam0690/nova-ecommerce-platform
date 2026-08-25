from io import StringIO
from unittest.mock import Mock, patch

from ingestion.csv_ingestion.staging_loader import load_staging


def test_load_staging_in_batches() -> None:
    data = "\n".join(
        [
            '{"order_id":"ORD-1","customer_id":1,"product_id":1,"product_name":"Mouse","category":"Electronics","quantity":1,"unit_price":"29.99","amount":"29.99","status":"completed","order_date":"2026-08-01T10:00:00Z"}',
            '{"order_id":"ORD-2","customer_id":2,"product_id":1,"product_name":"Mouse","category":"Electronics","quantity":1,"unit_price":"29.99","amount":"29.99","status":"completed","order_date":"2026-08-01T11:00:00Z"}',
        ]
    )

    fake_conn = Mock()

    with patch(
        "ingestion.csv_ingestion.staging_loader.copy_to_staging"
    ) as mock_copy:
        count = load_staging(
            fake_conn,
            StringIO(data),
            "orders_2026-08-01.csv",
        )

    assert count == 2

    mock_copy.assert_called_once()

    batch = mock_copy.call_args.args[1]

    assert len(batch) == 2
    assert batch[0].order_id == "ORD-1"
    assert batch[1].order_id == "ORD-2"
