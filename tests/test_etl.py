from pathlib import Path

from ingestion.csv_ingestion.etl import run_etl


def test_run_etl() -> None:
    csv_path = Path("data/orders_2026-08-01.csv")
    dlq_path = Path("data/dead_letter_2026-08-01.jsonl")

    staging_count, fact_count = run_etl(
        csv_path,
        dlq_path,
    )

    assert staging_count > 0
    assert fact_count >= 0
