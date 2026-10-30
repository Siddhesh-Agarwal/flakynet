from __future__ import annotations

import contextlib
import os
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

REDIS_URL = os.environ.get(
    "FLAKYNET_REDIS_URL",
    "redis://127.0.0.1:6379/0",
)
POSTGRES_DSN = os.environ.get(
    "FLAKYNET_POSTGRES_DSN",
    "postgresql://postgres:postgres@127.0.0.1:5432/postgres",
)


@pytest.fixture(scope="session")
def redis_url() -> str:
    """Redis URL, skipped when the service is unavailable."""
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(
        REDIS_URL,
        socket_timeout=1.0,
        socket_connect_timeout=1.0,
    )
    try:
        client.ping()
    except Exception as exc:
        pytest.skip(f"Redis is not available at {REDIS_URL}: {exc}")
    finally:
        client.close()
    return REDIS_URL


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    """PostgreSQL DSN, skipped when the service is unavailable."""
    psycopg = pytest.importorskip("psycopg")
    try:
        with psycopg.connect(POSTGRES_DSN, connect_timeout=2) as conn:
            conn.execute("SELECT 1")
    except Exception as exc:
        pytest.skip(f"PostgreSQL is not available at {POSTGRES_DSN}: {exc}")
    return POSTGRES_DSN


class _Server(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def handle_error(self, request: object, client_address: object) -> None:
        return


class _BytesHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/bytes":
            self.send_error(404)
            return
        try:
            count = int(parse_qs(parsed.query).get("n", ["0"])[0])
        except ValueError:
            self.send_error(400)
            return

        body = b"x" * count
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        return


@pytest.fixture(scope="session")
def http_server() -> Iterator[tuple[str, int]]:
    """A real local HTTP server used by the httpx and aiohttp clients."""
    server = _Server(("127.0.0.1", 0), _BytesHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="integration-http",
    )
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield str(host), int(port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
