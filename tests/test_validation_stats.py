from pathlib import Path

from ingestion.csv_ingestion.csv_loader import (
    ValidationStats,
    read_csv_rows,
    validate_rows,
)


def test_validation_stats(tmp_path: Path) -> None:
    csv_file = tmp_path / "orders.csv"

    csv_file.write_text(
        """order_id,customer_id,product_id,product_name,category,quantity,unit_price,amount,status,order_date
ORD-1,1,1,Mouse,Electronics,1,29.99,29.99,completed,2026-08-01T10:00:00Z
ORD-2,2,1,Mouse,Electronics,1,-29.99,-29.99,completed,2026-08-01T11:00:00Z
ORD-3,3,1,Mouse,Electronics,1,29.99,29.99,completed,2026-08-01T12:00:00Z
""",
        encoding="utf-8",
    )

    dlq_file = tmp_path / "dead_letter.jsonl"

    stats = ValidationStats()

    orders = list(
        validate_rows(
            read_csv_rows(csv_file),
            dlq_file,
            stats,
        )
    )

    assert stats.total_rows == 3
    assert stats.rejected_rows == 1
    assert stats.valid_rows == 2
    assert stats.rejection_rate == 1 / 3

    assert len(orders) == 2
    assert dlq_file.exists()
