# flakynet

Seeded, reproducible network faults for testing real clients over real sockets.
The core library needs no Docker, no sidecar, and no event loop of yours.

> **Status: alpha.** The seven faults, seeded resolution, events, fault
> reports, and the pytest fixture are implemented and covered by tests. The
> public API may still change before 1.0; platform and determinism limits are
> documented in [`docs/faults.md`](./docs/faults.md) and
> [`docs/determinism.md`](./docs/determinism.md).

## Why

Clients usually fail in production in ways unit tests never exercise: the
server resets mid-response, a write arrives in three-byte pieces, a commit is
delivered but its acknowledgement never comes back. Testing this today means
running a proxy daemon in Docker, or hand-writing socket mocks that do not
behave like a network.

flakynet runs a small TCP proxy inside your test process. You point a TCP
client at it, describe the faults you want, and give it a seed. When a test
fails, the seed replays the same fault plan.

## Quick look

This example is executed by `tests/test_readme.py`; it starts a real local
server, connects through flakynet, and checks the fault report.

```python
import socket
import threading

import flakynet as fn


def serve_once(listener: socket.socket) -> None:
    connection, _ = listener.accept()
    with connection:
        connection.sendall(b"x" * 64)
        connection.shutdown(socket.SHUT_WR)


listener = socket.create_server(("127.0.0.1", 0))
server = threading.Thread(target=serve_once, args=(listener,), daemon=True)
server.start()

plan = fn.Plan(
    fn.Fragment(max_bytes=3, side="server"),
    fn.Reset(after_bytes=40, side="server"),
)

with fn.proxy(listener.getsockname(), plan, seed=7) as p:
    with socket.create_connection((p.host, p.port), timeout=5.0) as client:
        received = bytearray()
        while True:
            try:
                chunk = client.recv(1024)
            except OSError:
                break
            if not chunk:
                break
            received.extend(chunk)
    p.assert_all_fired()
    print(f"received {len(received)} bytes")
    print([(event.fault, event.kind, event.offset) for event in p.canonical_events()])

server.join(timeout=5.0)
listener.close()
```

**Plans.** `Plan(*faults)` is the canonical form. `proxy()` also accepts a
single fault or any iterable of faults and normalizes them, so
`fn.proxy(target, fn.Reset(...))` is shorthand for
`fn.proxy(target, fn.Plan(fn.Reset(...)))`. An empty plan is a pass-through
proxy.

**Sides.** `side` names the stream a fault affects: `"client"` is what the
client sends, `"server"` is what the server sends. `saw(..., side=...)` names
the stream it watches.

**Seeds.** `seed` defaults to `0`, so phase-1 plans are deterministic unless
you ask for variety (explicit seeds, or `--flakynet-sweep`). Runtime event
occurrence still depends on scheduling; see
[`docs/determinism.md`](./docs/determinism.md).

### With pytest

This block is a complete test module; `tests/test_readme.py` runs it under
pytest.

```python
import socket
import socketserver
import threading

import flakynet as fn


class BytesServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class BytesHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        self.request.sendall(b"x" * 64)


def test_reset_is_replayed(flakynet):
    server = BytesServer(("127.0.0.1", 0), BytesHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        p = flakynet.proxy(
            server.server_address,
            fn.Reset(after_bytes=32, side="server"),
            seed=7,
        )
        with socket.create_connection((p.host, p.port), timeout=5.0) as client:
            while True:
                try:
                    if not client.recv(1024):
                        break
                except OSError:
                    break
        p.assert_all_fired()
        assert p.bytes_server_to_client == 32
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
```

`flakynet.proxy(...)` returns an already-started proxy and stops it when the
test ends; you can create several per test. Unnamed proxies are called `p0`,
`p1`, ... in creation order. The seed is chosen in this order: an explicit
`seed=` in the test (pinned), then `--flakynet-seed`, then the current
`--flakynet-sweep` value, then `0`. When a test using a proxy fails, the report
prints each proxy's name, effective seed, whether it was pinned, `repr(plan)`
and the exact replay command.

```
pytest --flakynet-seed=7        # replay one failing run
pytest --flakynet-sweep=200     # try seeds 0..199 serially; stop at the first failing seed
```

With pytest-xdist, each worker keeps its own sweep state. Run the sweep in a
single process when you need the global stop-at-first-failure behaviour and
the controller's first-failing-seed summary.

### Integration tests

The `integration` suite runs real `redis-py`, `psycopg`, `httpx`, and
`aiohttp` clients against real services. It is marked `integration`; the
normal `pytest` run skips it when the optional clients or services are absent.

Start Redis and PostgreSQL with the bundled compose file:

```sh
docker compose up -d
```

Install the optional clients and run the suite:

```sh
uv sync --group integration
uv run pytest -m integration
```

The Redis and PostgreSQL URLs default to
`redis://127.0.0.1:6379/0` and
`postgresql://postgres:postgres@127.0.0.1:5432/postgres`. Override them with
`FLAKYNET_REDIS_URL` and `FLAKYNET_POSTGRES_DSN` when your services live
elsewhere; the PostgreSQL DSN must use a TCP host and port. The `httpx` and
`aiohttp` tests start their own local HTTP server; only Redis and PostgreSQL
need external processes. CI runs this suite in a separate job with Redis and
PostgreSQL service containers.

Observed client behaviour and review candidates are recorded in
[`docs/bug-zoo.md`](./docs/bug-zoo.md).

## Faults

| Fault           | Kind     | What it does at the socket level                              |
| --------------- | -------- | ------------------------------------------------------------- |
| `Latency`       | window   | Delays each forwarded segment by a planned amount plus jitter |
| `Bandwidth`     | window   | Paces segments at a planned bytes-per-second rate             |
| `Fragment`      | window   | Forwards data in pieces of at most N bytes                    |
| `CloseAfter`    | discrete | Shutdown/close after N bytes; normally FIN, can be RST if unread data remains |
| `Reset`         | discrete | `SO_LINGER(1, 0)` then close after N bytes; RST on Linux, expected on macOS, not guaranteed on Windows |
| `Blackhole`     | discrete | Drains and discards after the trigger, leaving the connection open |
| `RefuseConnect` | discrete | Accepts then immediately aborts selected new connections      |

See [`docs/faults.md`](./docs/faults.md) for the exact trigger rules and the
per-OS FIN/RST table, including the current Windows `SO_LINGER` limitation.

There is deliberately no "packet loss" fault. TCP hides packet loss from
applications; what a client actually sees is stalls, resets, truncation and
fragmentation, which is what flakynet models.

**`Latency` applies per forwarded segment.** A segment is each piece the pump
writes after splitting at fault boundaries, so every `Fragment` piece is its
own segment. The delay for segment `n` is planned exactly and replays, but
**which bytes receive which delay is not replayable**: the OS decides how reads
are chunked, and the chunking decides where segments end. `Latency` logs only
`window_entered` and `window_exited`, never per-segment events, so its event
vocabulary does not expose which bytes received which delay. Measured elapsed
time is not promised.

**`Bandwidth` draws its rate once per connection and stream** at accept time.
The rate is part of the resolved plan; actual measured throughput is not
promised.

**`Blackhole` drains and discards** the affected stream after the trigger
rather than pausing reads. The sender's writes keep succeeding, the connection
stays open, and the peer sees only the bytes before the trigger. It does not
model TCP backpressure on the sender.

## Determinism

**The seed pins phase-1 resolution, not the runtime schedule.** For each
connection, `(seed, proxy name, connection index)` deterministically chooses
which faults apply, their static offsets/windows, bandwidth rates, armed
deltas, and the latency jitter key. No runtime code draws from a shared RNG,
and the process-wide `random` state is irrelevant.

**Deterministic offsets need a deterministic byte stream.** For one connection
with identical bytes in each direction and identical close boundaries, static
faults and same-stream `saw` anchors produce identical offsets, and their own
events are identical when the stream reaches the same end. The matcher is
chunk-invariant and the pump splits at fault boundaries, so OS read sizes do
not move a static offset or a same-stream anchor.

**What is not deterministic:** arrival order and timestamps; concurrent
connection indices; cross-stream `saw` offsets; competing terminals and sibling
cancellation; chunk-dependent `Latency`/`Bandwidth` pacing; measured time;
application and OS FIN/RST behaviour; and report statuses that depend on those
races. `canonical_events()` removes order and inexact offsets, but it cannot
make a scheduling-dependent event happen.

`proxy.events` is in arrival order and is diagnostic only.
`proxy.canonical_events()` sorts by `(connection, stream, offset, fault_index,
kind)` and removes the sequence number and timestamp; a non-exact offset
becomes `None`. Window faults log `window_entered` and `window_exited`;
discrete faults log `fired`; `Latency` never logs per-segment events. When
comparing concurrent connections, compare the multiset of per-connection event
sequences, not the mapping to clients.

See [`docs/determinism.md`](./docs/determinism.md) for the exact resolution
draw order, the full limits, report-status caveats, and comparison recipes.
`tests/test_determinism_limits.py` pins these limits.

## Fault reports

This block is also executed by `tests/test_readme.py`. It uses `RefuseConnect`
so it needs no target server: the proxy refuses the connection before
connecting upstream.

```python
import socket

import flakynet as fn

with fn.proxy(("127.0.0.1", 1), fn.RefuseConnect(), seed=7) as p:
    client = socket.create_connection((p.host, p.port), timeout=5.0)
    try:
        while True:
            try:
                if not client.recv(1):
                    break
            except OSError:
                break
    finally:
        client.close()
    for status in p.fault_report():
        print(status.describe())
    p.assert_all_fired()
    p.assert_all_fired(allow={"unmatched"})
```

| Status        | Meaning                                                                                                     |
| ------------- | ----------------------------------------------------------------------------------------------------------- |
| `FIRED`       | The fault fired (window faults: first byte forwarded inside the window)                                     |
| `PREEMPTED`   | An earlier flakynet fault ended the connection first                                                        |
| `ENDED_EARLY` | A client or server aborted the connection before the trigger point                                          |
| `MISSED`      | The exchange ended normally and the trigger was never reached (offset beyond the stream, or pattern absent) |
| `PENDING`     | The connection was still open when the report was taken                                                     |
| `UNMATCHED`   | No accepted connection matched the fault's selector                                                         |

Each report entry also records how the connection ended (`graceful`,
`client_abort`, `server_abort`, `error`, `flakynet`, or `pending`) and the bytes
forwarded in each direction.

By default `assert_all_fired()` accepts `PREEMPTED` and `ENDED_EARLY` and fails
on `MISSED`, `PENDING` and `UNMATCHED`, because a fault that silently never
applied usually means the test is not testing what you think. Opt out
per assertion with `allow=`. Call it after the client has finished.

## How it compares

<!-- TODO before publishing: re-verify every row against current releases. -->

| Tool                    | Approach                                       | Notes                                              |
| ----------------------- | ---------------------------------------------- | -------------------------------------------------- |
| Toxiproxy               | Separate proxy daemon, usually a container     | Mature and language-agnostic; needs infrastructure |
| netfault                | Small Python TCP proxy with a fault pipeline   | Closest in spirit                                  |
| simloop / simloom       | Simulated asyncio event loop with virtual time | Deterministic, but asyncio code only               |
| MockServer (chaos mode) | Java server with TCP/HTTP/gRPC faults          | Heavier, JVM-based                                 |

flakynet's niche: real sockets, in-process, seeded replay, usable from sync,
threaded and async code alike.

## Non-goals

TLS termination, UDP, payload corruption, HTTP-aware faults, a CLI or daemon.
Record and replay of streams may arrive later as a separate module.

## Roadmap

- **v0.1** core proxy, seven faults, events and fault reports, pytest fixture, seed replay — implemented
- **v0.2** recipes and tests against redis-py, psycopg, httpx and an asyncio client — implemented; plan minimization is not implemented
- **v0.3** a "bug zoo" of real failures found in popular clients — `docs/bug-zoo.md` collects candidates for review

## Development

```sh
uv sync
uv run pytest
uv run pytest -m integration      # after starting the services above
uv run ruff check . && uv run ruff format --check . && uv run ty check
```

See [`AGENT.md`](./AGENT.md) for design rules and contributor guidance.

## License

TBD (MIT suggested).
