from pathlib import Path

from ingestion.csv_ingestion.csv_loader import validate_to_tempfile


def test_validate_to_tempfile(tmp_path: Path) -> None:
    csv_file = tmp_path / "orders.csv"
    dlq_file = tmp_path / "dead_letter.jsonl"

    csv_file.write_text(
        """order_id,customer_id,product_id,product_name,category,quantity,unit_price,amount,status,order_date
ORD-1,1,1,Mouse,Electronics,1,29.99,29.99,completed,2026-08-01T10:00:00Z
ORD-2,2,1,Mouse,Electronics,1,-29.99,-29.99,completed,2026-08-01T11:00:00Z
ORD-3,3,1,Mouse,Electronics,2,29.99,59.98,completed,2026-08-01T12:00:00Z
""",
        encoding="utf-8",
    )

    temp_file, stats = validate_to_tempfile(
        csv_file,
        dlq_file,
    )

    try:
        assert stats.total_rows == 3
        assert stats.valid_rows == 2
        assert stats.rejected_rows == 1

        lines = temp_file.readlines()

        assert len(lines) == 2

        assert "ORD-1" in lines[0]
        assert "ORD-3" in lines[1]

    finally:
        temp_file.close()
