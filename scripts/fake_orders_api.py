"""A deliberately unhelpful orders API, for practising REST ingestion against.

Serves the CSVs in data/ over HTTP so the pipeline has a second source that
behaves like a real vendor endpoint: bearer auth, opaque cursors, a page size
it does not respect, and optional failures.

    python scripts/fake_orders_api.py            # port 8088
    NOVA_API_FLAKY=1 python scripts/fake_orders_api.py

    GET /orders?date=2026-09-05&limit=100&cursor=<opaque>
    Authorization: Bearer dev-token
"""

import base64
import csv
import json
import os
import random
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
TOKEN = os.environ.get("NOVA_API_TOKEN", "dev-token")
MAX_LIMIT = 100          # the server's cap; asking for more does not get you more
FLAKY = os.environ.get("NOVA_API_FLAKY") == "1"


def _rows(day: str) -> list[dict[str, str]]:
    path = DATA_DIR / f"orders_{day}.csv"
    if not path.exists():
        return []                       # unknown date is an empty page, not a 404
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class Handler(BaseHTTPRequestHandler):
    def _json(self, status: int, payload: dict, headers: dict[str, str] = {}) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path != "/orders":
            return self._json(404, {"error": "not found"})

        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self._json(401, {"error": "unauthorized"})

        if FLAKY and random.random() < 0.15:
            if random.random() < 0.5:
                return self._json(429, {"error": "rate limited"}, {"Retry-After": "2"})
            return self._json(500, {"error": "internal error"})

        query = parse_qs(url.query)
        day = query.get("date", [""])[0]
        if not day:
            return self._json(400, {"error": "date is required"})

        limit = min(int(query.get("limit", ["50"])[0]), MAX_LIMIT)
        cursor = query.get("cursor", [None])[0]
        offset = int(base64.urlsafe_b64decode(cursor).decode()) if cursor else 0

        rows = _rows(day)
        page = rows[offset : offset + limit]
        nxt = offset + limit
        next_cursor = (
            base64.urlsafe_b64encode(str(nxt).encode()).decode() if nxt < len(rows) else None
        )
        self._json(200, {"data": page, "next_cursor": next_cursor})

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"{self.address_string()} {fmt % args}")


if __name__ == "__main__":
    port = int(os.environ.get("NOVA_API_PORT", "8088"))
    print(f"serving {DATA_DIR} on http://127.0.0.1:{port}/orders (flaky={FLAKY})")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
