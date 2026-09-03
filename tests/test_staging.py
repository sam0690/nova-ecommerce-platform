from pathlib import Path

from ingestion.csv_ingestion.batching import batched
from ingestion.csv_ingestion.csv_loader import (
    ValidationStats,
    read_csv_rows,
    validate_rows,
)
from ingestion.csv_ingestion.db import get_connection
from ingestion.csv_ingestion.staging import copy_to_staging

INPUT_PATH = Path("data/orders_2026-08-01.csv")


def test_copy_to_staging_loads_a_batch(tmp_path: Path) -> None:
    rows = read_csv_rows(INPUT_PATH)

    valid_rows = validate_rows(
        rows,
        tmp_path / "dead_letter.jsonl",
        ValidationStats(),
    )

    batch = next(batched(valid_rows, 10))

    assert len(batch) == 10

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE shop.stg_orders")

        copy_to_staging(
            conn,
            batch,
            INPUT_PATH.name,
        )

        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM shop.stg_orders WHERE source_file = %s",
                (INPUT_PATH.name,),
            )

            assert cur.fetchone() == (10,)
