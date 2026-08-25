import argparse
from pathlib import Path

from ingestion.csv_ingestion.csv_loader import (
    check_rejection_rate,
    validate_to_tempfile,
)
from ingestion.csv_ingestion.db import get_connection
from ingestion.csv_ingestion.fact_loader import load_fact_orders
from ingestion.csv_ingestion.staging_loader import load_staging

import logging
import time

logger = logging.getLogger(__name__)

def run_etl(
    input_path: Path,
    dlq_path: Path,
) -> tuple[int, int]:
    """Run the complete CSV → staging → fact ETL pipeline."""

    start_time = time.perf_counter()

    logger.info("Starting ETL")
    logger.info("Input file: %s", input_path)

    validation_start = time.perf_counter()

    temp_file, stats = validate_to_tempfile(
        input_path,
        dlq_path,
    )

    validation_time = time.perf_counter() - validation_start

    try:
        logger.info("Total rows: %d", stats.total_rows)
        logger.info("Valid rows: %d", stats.valid_rows)
        logger.info("Rejected rows: %d", stats.rejected_rows)
        logger.info("Validation time: %.3fs", validation_time)

        check_rejection_rate(stats)

        source_file = input_path.name

        db_start = time.perf_counter()

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE shop.stg_orders")
            staging_count = load_staging(
                conn,
                temp_file,
                source_file,
            )

            logger.info(
                "Staging rows loaded: %d",
                staging_count,
            )

            fact_count = load_fact_orders(conn)

            logger.info(
                "New fact rows loaded: %d",
                fact_count,
            )

            conn.commit()

        db_time = time.perf_counter() - db_start

        total_time = time.perf_counter() - start_time

        logger.info("Database load time: %.3fs", db_time)
        logger.info("Total ETL time: %.3fs", total_time)
        logger.info("ETL completed successfully")

        return staging_count, fact_count

    except Exception:
        logger.exception("ETL failed")
        raise

    finally:
        temp_file.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Nova Commerce CSV → PostgreSQL ETL."
    )

    parser.add_argument(
        "date",
        help="Order date in YYYY-MM-DD format.",
    )

    args = parser.parse_args()

    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",)

    input_path = Path(f"data/orders_{args.date}.csv")
    dlq_path = Path(f"data/dead_letter_{args.date}.jsonl")

    run_etl(
        input_path,
        dlq_path,
    )


if __name__ == "__main__":
    main()
