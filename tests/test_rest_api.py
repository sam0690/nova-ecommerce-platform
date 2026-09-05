"""Tests for the REST API source.

The point of these is the pagination loop. A client that stops after page one
still returns rows and still looks like it worked, so the only assertion that
catches it is a full count.
"""

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import httpx
import pytest

from ingestion.csv_ingestion.csv_loader import read_csv_rows
from ingestion.csv_ingestion.db import get_connection
from ingestion.rest_api import extract_orders
from ingestion.rest_api.extract_orders import fetch_orders, run_api_etl

ROOT = Path(__file__).resolve().parents[1]
LOADED_DAY = date(2026, 9, 5)
FRESH_DAY = date(2026, 9, 6)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def api_server() -> Iterator[str]:
    """Run scripts/fake_orders_api.py for the duration of the test session.

    On its own port, so it never collides with one you left running by hand.
    """

    port = _free_port()
    process = subprocess.Popen(
        [sys.executable, str(ROOT / "scripts/fake_orders_api.py")],
        env={**os.environ, "NOVA_API_PORT": str(port)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"

    try:
        for _ in range(50):
            try:
                # Any response means it is listening. A 401 counts.
                httpx.get(f"{url}/orders", timeout=0.5)
                break
            except httpx.ConnectError:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"fake API never came up on {url}")

        yield url

    finally:
        process.terminate()
        process.wait(timeout=5)


@pytest.fixture(autouse=True)
def point_client_at_test_server(
    api_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redirect the client at the test server.

    BASE_URL is read at import time, so setting NOVA_API_URL here would be too
    late — the module is already loaded. Patch the attribute instead.
    """

    monkeypatch.setattr(extract_orders, "BASE_URL", api_server)


def test_fetch_orders_follows_cursors_to_the_end() -> None:
    """The count only comes out right if every cursor was followed."""

    expected = sum(1 for _ in read_csv_rows(ROOT / f"data/orders_{LOADED_DAY}.csv"))

    fetched = sum(1 for _ in fetch_orders(LOADED_DAY))

    # Guards against the day the CSV shrinks below one page and this test
    # starts passing for the wrong reason.
    assert expected > extract_orders.PAGE_SIZE
    assert fetched == expected


def test_unknown_date_yields_no_rows() -> None:
    """A day with no data is an empty stream, not an error."""

    assert list(fetch_orders(date(2030, 1, 1))) == []


def test_bad_token_raises() -> None:
    """raise_for_status turns the 401 into an exception instead of empty data."""

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(extract_orders, "TOKEN", "not-the-token")

        with pytest.raises(httpx.HTTPStatusError) as caught:
            next(fetch_orders(LOADED_DAY))

    assert caught.value.response.status_code == 401


def test_run_api_etl_is_idempotent(tmp_path: Path) -> None:
    """A second run of the same day adds nothing."""

    def fact_count() -> int:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM shop.fact_orders")
            return int(cur.fetchone()[0])

    before = fact_count()

    staged, loaded = run_api_etl(FRESH_DAY, tmp_path / "dlq.jsonl")
    assert loaded == staged
    assert fact_count() == before + staged

    staged_again, loaded_again = run_api_etl(FRESH_DAY, tmp_path / "dlq2.jsonl")
    assert staged_again == staged
    assert loaded_again == 0
    assert fact_count() == before + staged


def test_rows_are_tagged_with_their_source(tmp_path: Path) -> None:
    """There is no filename, so provenance has to be written deliberately."""

    run_api_etl(FRESH_DAY, tmp_path / "dlq.jsonl")

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM shop.fact_orders WHERE source_file = %s",
            (f"api:orders:{FRESH_DAY}",),
        )
        assert cur.fetchone()[0] > 0
