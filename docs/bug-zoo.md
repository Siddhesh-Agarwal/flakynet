# Bug zoo — client behaviour under injected TCP faults

This file records client behaviour that looked surprising while running the
`integration` suite. These are **candidates for review, not filed upstream
issues**. Confirm whether each is intended before reporting it.

## Environment for these observations

- Python 3.14.8
- `redis-py` 8.1.0
- `psycopg` 3.3.6 / libpq 18.6
- `httpx` 0.28.1
- `aiohttp` 3.14.5
- Local Redis 9.0.6 (Valkey) and PostgreSQL 18.6

The compose file pins `redis:7-alpine` and `postgres:16-alpine`, so re-run the
candidates there before treating them as client bugs. The client versions are
the ones resolved by the `integration` dependency group.

Reproduce with:

```sh
docker compose up -d
uv sync --group integration
uv run pytest -m integration
```

All reproductions below use `seed=7` and the public `proxy()` context manager.

## Candidate 1 — redis-py 8 retries a reset ten times by default

**What happens:** one `GET` through a proxy that resets the server stream
creates **11 connections**. The first connection is reset at offset 4096; the
client retries ten more times and eventually raises
`redis.exceptions.ConnectionError`.

**Why it is surprising:** a single application command becomes eleven
connections. The first reset is hidden, and a real transient fault can
amplify load instead of failing fast. `r.get_retry().get_retries()` returns
`10` in redis-py 8.1.0.

**Minimal reproduction:**

```python
import redis
import flakynet as fn

direct = redis.Redis.from_url("redis://127.0.0.1:6379/0")
direct.set("big-key", b"x" * 200_000)

plan = fn.Plan(fn.Reset(after_bytes=4096, side="server"))
with fn.proxy(("127.0.0.1", 6379), plan, seed=7) as p:
    r = redis.Redis(host=p.host, port=p.port, socket_timeout=2)
    try:
        r.get("big-key")
    except redis.exceptions.RedisError as exc:
        print(type(exc).__name__, exc)

    print(len(p.fault_report()))  # observed: 11

direct.delete("big-key")
direct.close()
```

Observed exception:

```text
redis.exceptions.ConnectionError: Error while reading from 127.0.0.1:PORT : (104, 'Connection reset by peer')
```

**Question for review:** is the default ten-retry policy intended for a
connection reset during a read? If yes, document it prominently; if not, this
is an upstream candidate.

Relevant integration test: `tests/integration/test_redis.py::test_redis_reset_mid_get`.

## Candidate 2 — aiohttp reports a reset as a payload error

**What happens:** a reset in the middle of an HTTP body surfaces as
`aiohttp.ClientPayloadError`, with a `ContentLengthError` cause and a
`ConnectionResetError` embedded in the message. A caller catching
`ClientConnectionError` or `ServerDisconnectedError` will not catch it.

**Minimal reproduction** (the target is any HTTP server serving
`/bytes?n=200000`; the integration fixture starts one on an ephemeral port):

```python
import asyncio
import aiohttp
import flakynet as fn

plan = fn.Plan(fn.Reset(after_bytes=1000, side="server"))

async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.read()

with fn.proxy(("127.0.0.1", 8080), plan, seed=7) as p:
    try:
        asyncio.run(fetch(f"http://{p.host}:{p.port}/bytes?n=200000"))
    except Exception as exc:
        print(type(exc).__module__ + "." + type(exc).__name__)
        print(exc)
```

Observed (aiohttp 3.14.5):

```text
aiohttp.client_exceptions.ClientPayloadError: Response payload is not completed:
<ContentLengthError: 400, message='Not enough data to satisfy content length header
(received 884 of 200000 bytes).'>. ConnectionResetError(104, 'Connection reset by peer')
```

**Question for review:** is `ClientPayloadError` the intended taxonomy for a
mid-body connection reset, and should the cause be a connection error instead
of a content-length error?

Relevant integration test: `tests/integration/test_aiohttp.py::test_aiohttp_reset_mid_response`.

## Candidate 3 — libpq/psycopg blames the server for a proxy reset

**What happens:** a reset in the middle of a query response raises
`psycopg.OperationalError` whose message says the **server** closed the
connection unexpectedly. In this scenario the server is healthy; the proxy
reset the client-facing stream.

**Minimal reproduction:**

```python
import psycopg
import flakynet as fn

plan = fn.Plan(fn.Reset(after_bytes=8192, side="server"))
dsn = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"

with fn.proxy(("127.0.0.1", 5432), plan, seed=7) as p:
    through_proxy = psycopg.conninfo.make_conninfo(dsn, host=p.host, port=p.port)
    try:
        with psycopg.connect(through_proxy) as conn:
            conn.execute("SELECT repeat('x', 200000)").fetchall()
    except psycopg.Error as exc:
        print(type(exc).__name__, exc)
```

Observed:

```text
psycopg.OperationalError: consuming input failed: server closed the connection unexpectedly
    This probably means the server terminated abnormally
    before or while processing the request.
```

**Question for review:** is this purely libpq's standard diagnostic, or should
psycopg expose a reset-specific hint? It is useful to distinguish a proxy RST
from a database crash.

Relevant integration test: `tests/integration/test_postgres.py::test_postgres_reset_mid_query`.

## No candidate found

- **httpx 0.28.1:** the reset surfaced as `httpx.ReadError: [Errno 104]
  Connection reset by peer`, with one connection and no retry. This is clear
  and expected.
- **Fragment with `max_bytes=1`:** all four clients parsed the fragmented
  responses correctly. redis-py, psycopg, httpx, and aiohttp all handled
  one-byte writes without corruption or hangs in these tests. No candidate.
