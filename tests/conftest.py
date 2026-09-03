import os
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

# Load .env, then override the database name *before* any test module imports
# ingestion.csv_ingestion.db. That module calls load_dotenv() itself, and
# load_dotenv() never overwrites a variable that is already set — so this wins
# and get_connection() points at nova_test for the whole run.
load_dotenv()
os.environ["POSTGRES_DB"] = "nova_test"

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILES = [
    ROOT / "sql/ddl/001_shop.sql",
    ROOT / "sql/ddl/002_seed.sql",
    ROOT / "sql/migrations/003_orders_etl.sql",
]


def _admin_connection() -> psycopg.Connection:
    """Connect to the always-present `postgres` database, outside a transaction.

    CREATE/DROP DATABASE cannot run inside a transaction block, and you cannot
    drop the database you are connected to — hence autocommit, and dbname.
    """
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname="postgres",
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    )


@pytest.fixture(scope="session", autouse=True)
def test_database() -> None:
    with _admin_connection() as admin:
        admin.execute("DROP DATABASE IF EXISTS nova_test WITH (FORCE)")
        admin.execute("CREATE DATABASE nova_test")

    from ingestion.csv_ingestion.db import get_connection

    with get_connection() as conn:
        for path in SCHEMA_FILES:
            conn.execute(path.read_text())
        conn.commit()

    yield

    with _admin_connection() as admin:
        admin.execute("DROP DATABASE IF EXISTS nova_test WITH (FORCE)")
