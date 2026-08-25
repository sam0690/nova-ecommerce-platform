from typing import TextIO

import psycopg

from ingestion.csv_ingestion.batching import batched
from ingestion.csv_ingestion.staging import copy_to_staging
from ingestion.csv_ingestion.validated_reader import read_validated_orders


BATCH_SIZE = 1000


def load_staging(
    conn: psycopg.Connection,
    file: TextIO,
    source_file: str,
) -> int:
    """Load validated orders into staging in batches of 1,000."""

    total_loaded = 0

    orders = read_validated_orders(file)

    for batch in batched(orders, BATCH_SIZE):
        copy_to_staging(
            conn,
            batch,
            source_file,
        )

        total_loaded += len(batch)

    return total_loaded
