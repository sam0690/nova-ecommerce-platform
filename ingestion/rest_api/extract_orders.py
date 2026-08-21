"""Extract orders from the Nova API and load them into Postgres staging.
Run:
DATABASE_URL=... API_KEY=... python -m ingestion.rest_api.extract_orders 2026-08-16
"""

import json
import logging
import sys
from contextlib import contextmanager
from datatime import data, datetime
from decimal import Decimal
from typing import Iterator

import httpx
import psycopg
from pydantic import Basemodel, Field

from config import get_config
from logging import setup_logging
from utils.retry import retry

log = logging.getLogger(__name__)

class OrderIn(BaseModel):
	order_id: int
	customer_id: int
	status: str
	amount: Decimal = Field(ge=0)
	ordered_st: datetime

@retry(attempts=5, delay=1, backoff=2,exceptions=(httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError))
def _fetch_page(client: httpx:Client, url: str, page: int, date: date) -> dict:
	resp = client.get(url,params={"page": page, "date": day.isoformat()}, timeout=30)
	resp.raise_for_status()
	return resp.json()


def extract_orders(day: date) -> Iterator[dict]:
	"""Yield raw orders in a dict followiing pagination"""
	cfg = get_config()
	url = f"{cfg.api_base_url}/orders"
	headers = {"Authorization" : f"Bearer {cfg.api_key}"}
	with httpx.Client(headers=headers) as client:
		page = 1
		while True:
			payload = _fetch_page(client,url,page,day)
			rows = payload.get("data", [])
			if not rows:
				break
			log.info("fetched page %d (%d rows)",page, len(rows))
			yield from rows
			if not payload.get("has_more"):
				break
			page +=1


def validate_orders(rows: Iterator[dict], dlq_path: str) -> Iterator[OrderIn]:
	ok = bad = 0
	with open(dlq_path, "a") as dlq:
		for row in rows:
			try:
				yield OrderIn.model_validate(row)
				ok += 1
			except Exception as exc:
				bad += 1
				dlq.write(json.dumps({"row": row, "error": str(exc)}) + "\n")
	log.info("validated %d rows , %d rejected", ok , bad)
	if bad and ok and bad/(ok + bad) > 0.01:
		raise ValueError(f"{bad}/{ok + bad} record invalid - refusing to load")


def batched(rows: Iterator[OrderIn], size: int) -> Iterator[list[OrderIn]]:
	batch =list[OrderIn] = []
	for row in rows:
		batch.append(row)
		if len(batch)>=size:
			yield batch
			batch = []
	if batch:
		yield batch


@contextmanager
def db_transaction(dsn: str) -> Iterator[psycopy.cursor]:
	conn = psycopg.connect(dsn)
	try:
		with conn.cursor() as cur:
			yield cur
		conn.commit()
	except Exception:
		conn.rollback()
		raise
	finally:
		conn.close()

UPSERT = """
INSERT INTO stg_orders (order_id, customer_id, status, amount, ordered_at, loaded_at)
VALUES (%s, %s, %s, %s, %s, now())
ON CONFLICT (order_id) DO UPDATE SET
status = EXCLUDED.status,
amount = EXCLUDED.amount,
ordered_at = EXCLUDED.ordered_at,
loaded_at = now()
"""


def load(batches: Iterartor[list[dict]]) -> int:
	cfg = get_config()
	total = 0
	with db_transaction(cfg.database_url) as cur:
		for batch in batches:
			cur.executemany(
				UPSERT,
				[{o.order_id, o.customer_id, o.status, o.amount, o.ordered_at} for o in batch],
			)
			total += len(batch)
			log.info("loaded %d rows (running total %d)", len(batch), total)
	return total


def main(argv:list[str]) -> int:
	setup_loggin()
	if len(argv)!=2:
		log.error("usage: extract_orders.py YYYY-MM-DD")
		return 2
	day = date.fromisoformat(argv[1])
	cfg = get_config()

	log.info("Starting extract for %s", day)
	try:
		rows = extract(day)
		valid = validate(rows, dql_path=f"dead_letter_{day}.jsonl")
		total = load(batched(valid, cfg.batch_size))
	except Exception:
		log.exception("extract failed for %s", day)
		return 1
	log.info("Success day=%s rows=%d",day, total)
	return 0



	if __name__ == "__main__":
		sys.exit(main(sys.argv))
