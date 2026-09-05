import argparse
import logging
import os
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import httpx
from dotenv import load_dotenv

from ingestion.csv_ingestion.batching import batched
from ingestion.csv_ingestion.csv_loader import (
    ValidationStats,
    check_rejection_rate,
    validate_rows,
)
from ingestion.csv_ingestion.db import get_connection
from ingestion.csv_ingestion.fact_loader import load_fact_orders
from ingestion.csv_ingestion.staging import copy_to_staging
from ingestion.csv_ingestion.staging_loader import BATCH_SIZE

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("NOVA_API_URL", "http://localhost:8088")
TOKEN = os.getenv("NOVA_API_TOKEN", "dev-token")
PAGE_SIZE = 100


def fetch_orders(day: date) -> Iterator[dict[str, str]]:
    """Stream every order for `day`, following the API's cursors.

    Yields one row at a time so the whole day never sits in memory, matching
    read_csv_rows() — the validation and staging path downstream is the same.
    """

    cursor: str | None = None

    with httpx.Client(
        base_url=BASE_URL,
        headers={"Authorization": f"Bearer {TOKEN}"},
    ) as client:
        while True:
            params: dict[str, str | int] = {
                "date": day.isoformat(),
                "limit": PAGE_SIZE,
            }

            # Omit the cursor entirely on the first request. httpx would
            # serialise None as an empty `cursor=`, which this server happens
            # to ignore and a stricter one would reject.
            if cursor is not None:
                params["cursor"] = cursor

            response = client.get("/orders", params=params)
            response.raise_for_status()

            payload = response.json()

            for row in payload["data"]:
                yield {str(key): str(value) for key, value in row.items()}

            # The server is the only thing that knows whether more rows exist.
            # An empty page is not the end of the stream — a null cursor is.
            cursor = payload["next_cursor"]

            if cursor is None:
                return


def run_api_etl(day: date, dlq_path: Path) -> tuple[int, int]:
    """Run the API → staging → fact pipeline for one day.

    The same four steps as the CSV path; only the source of the rows differs.
    Everything is inside one transaction, so a day that fails the rejection
    check rolls back the truncate and the staging load with it.
    """

    logger.info("Starting API extract for %s", day)

    stats = ValidationStats()

    # There is no file. Record where the rows came from anyway — fact_orders
    # keeps this forever, and it is the only way to tell the two sources apart.
    source = f"api:orders:{day.isoformat()}"

    orders = validate_rows(fetch_orders(day), dlq_path, stats)

    staging_count = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE shop.stg_orders")

        for batch in batched(orders, BATCH_SIZE):
            copy_to_staging(conn, batch, source)
            staging_count += len(batch)

        # Only valid now — stats fill in as the generator drains, so this
        # cannot be checked before the loop the way the CSV path does it.
        logger.info("Total rows: %d", stats.total_rows)
        logger.info("Valid rows: %d", stats.valid_rows)
        logger.info("Rejected rows: %d", stats.rejected_rows)

        check_rejection_rate(stats)

        fact_count = load_fact_orders(conn)

        logger.info("Staging rows loaded: %d", staging_count)
        logger.info("New fact rows loaded: %d", fact_count)

        conn.commit()

    return staging_count, fact_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pull one day of orders from the Nova API into PostgreSQL."
    )
    parser.add_argument("date", help="Order date in YYYY-MM-DD format.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    day = date.fromisoformat(args.date)
    run_api_etl(day, Path(f"data/dead_letter_api_{args.date}.jsonl"))


if __name__ == "__main__":
    main()
