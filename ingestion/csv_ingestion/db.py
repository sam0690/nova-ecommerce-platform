import os

import psycopg
from dotenv import load_dotenv

load_dotenv()

def get_connection() -> psycopg.Connection:
    """Create a PostgreSQL connection from environment variables."""

    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
