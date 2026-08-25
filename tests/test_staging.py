from pathlib import Path

from ingestion.csv_ingestion.batching import batched
from ingestion.csv_ingestion.csv_loader import (
    read_csv_rows,
    validate_rows,
)
from ingestion.csv_ingestion.db import get_connection
from ingestion.csv_ingestion.staging import copy_to_staging


input_path = Path("data/orders_2026-08-01.csv")
dlq_path = Path("data/dead_letter_2026-08-01.jsonl")


rows = read_csv_rows(input_path)

valid_rows = validate_rows(
    rows,
    dlq_path,
)

batch = next(batched(valid_rows, 10))


with get_connection() as conn:
    copy_to_staging(
        conn,
        batch,
        input_path.name,
    )

    conn.commit()


print(f"Loaded {len(batch)} rows into staging.")
