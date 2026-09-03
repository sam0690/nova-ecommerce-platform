from pathlib import Path

from ingestion.csv_ingestion.batching import batched
from ingestion.csv_ingestion.csv_loader import (
	ValidationStats,
	read_csv_rows,
	validate_rows,
)
from ingestion.csv_ingestion.db import get_connection
from ingestion.csv_ingestion.fact_loader import load_fact_orders
from ingestion.csv_ingestion.staging  import copy_to_staging

INPUT_PATH = Path("data/orders_2026-08-01.csv")

def load_test_data(dlq_path: Path) -> None:
	rows = read_csv_rows(INPUT_PATH)
	valid_rows = validate_rows(rows, dlq_path, ValidationStats())

	with get_connection() as conn:
		for batch in batched(valid_rows, 1000):
			copy_to_staging(conn,batch,INPUT_PATH.name,)

		load_fact_orders(conn)

		conn.commit()


def get_fact_count() -> int:
	with get_connection() as conn:
		with conn.cursor() as cur:
			cur.execute("SELECT COUNT(*) FROM shop.fact_orders")

			result = cur.fetchone()

			assert result is not None

			return int(result[0])


def test_idempotency(tmp_path: Path) -> None:
	dlq_path = tmp_path / "dead_letter.jsonl"

	load_test_data(dlq_path)

	first_count = get_fact_count()

	load_test_data(dlq_path)

	second_count = get_fact_count()

	assert second_count == first_count
