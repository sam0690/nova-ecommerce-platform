import os
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv

load_dotenv()

CREATE = """
CREATE TABLE IF NOT EXISTS products (
	id          integer PRIMARY KEY,
	title       text NOT NULL,
	price       numeric(10, 2),
	fetched_at  timestamptz NOT NULL
)
"""

# ON CONFLICT needs a unique constraint to collide against -- here, the id PK.
# EXCLUDED is the row we *tried* to insert; that's how you reach the new values.
UPSERT = """
INSERT INTO products (id, title, price, fetched_at)
VALUES (%s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE SET
	title      = EXCLUDED.title,
	price      = EXCLUDED.price,
	fetched_at = EXCLUDED.fetched_at
"""

with psycopg.connect(
	host=os.environ["POSTGRES_HOST"],
	port=os.environ["POSTGRES_PORT"],
	dbname=os.environ["POSTGRES_DB"],
	user=os.environ["POSTGRES_USER"],
	password=os.environ["POSTGRES_PASSWORD"],
) as conn:
	with conn.cursor() as cur:
		cur.execute(CREATE)

		# %s placeholders, never f-strings -- psycopg escapes these for us.
		cur.execute(UPSERT, (1, "Fake Laptop", 999.99, datetime.now(timezone.utc)))

		cur.execute("SELECT id, title, price, fetched_at FROM products ORDER BY id")
		for row in cur.fetchall():
			print(row)

		cur.execute("SELECT count(*) FROM products")
		print(f"rows: {cur.fetchone()[0]}  <- must stay 1 across reruns")
