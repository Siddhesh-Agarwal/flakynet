# flakynet

Seeded, reproducible network faults for testing real clients over real sockets.
No Docker, no sidecar, no event-loop requirement.

> **Status: pre-alpha, design phase.** The examples below show the *target*
> API. Nothing here is published or guaranteed yet.

## Why

Clients usually fail in production in ways unit tests never exercise: the
server resets mid-response, a write arrives in three-byte pieces, a commit is
delivered but its acknowledgement never comes back. Testing this today means
running a proxy daemon in Docker, or hand-writing socket mocks that do not
behave like a network.

flakynet runs a small TCP proxy inside your test process. You point any client
at it, describe the faults you want, and give it a seed. When a test fails,
the seed replays the same fault plan.

## Quick look (target API)

```python
import flakynet as fn
import psycopg

plan = fn.Plan(
    fn.Latency(ms=50, jitter_ms=20),
    fn.Fragment(max_bytes=3, side="server"),             # split writes: finds framing bugs
    fn.Reset(after_bytes=fn.between(200, 4000),          # TCP RST
             side="server", conn=fn.nth(2)),
    fn.Blackhole(after=fn.saw(b"COMMIT", side="client"), # request delivered, reply never arrives
                 side="server"),
)

with fn.proxy(("127.0.0.1", 5432), plan, seed=7) as p:
    conn = psycopg.connect(host=p.host, port=p.port, dbname="test")
    ...
    p.assert_all_fired()   # a fault that never fired is a bug in the test
    print(p.events)        # [FaultEvent(conn=2, fault="Reset", offset=1432), ...]
```

With pytest:

```python
def test_redis_survives(flakynet):
    p = flakynet.proxy(("127.0.0.1", 6379), fn.Reset(after_bytes=fn.between(10, 500)))
    ...
```

```
pytest --flakynet-seed=7        # replay one failing run
pytest --flakynet-sweep=200     # try seeds 0..199 and report the failing seed + plan
```

## Faults (v0.1 plan)

| Fault | What it does at the socket level |
|---|---|
| `Latency` | Delays forwarded data by a fixed amount plus jitter |
| `Bandwidth` | Caps throughput in bytes per second |
| `Fragment` | Forwards data in pieces of at most N bytes |
| `CloseAfter` | Graceful close (FIN) after N bytes |
| `Reset` | Abortive close (RST) after N bytes |
| `Blackhole` | Stops forwarding while keeping the connection open |
| `RefuseConnect` | Rejects or immediately closes new connections |

There is deliberately no "packet loss" fault. TCP hides packet loss from
applications; what a client actually sees is stalls, resets, truncation and
fragmentation, which is what flakynet models.

## How determinism works (and what it does not promise)

- Faults trigger on **stream byte offsets**, not on timers or on however the OS
  happened to chunk a read.
- Each connection gets its own random stream derived from `(seed, connection index)`.
  The fault plan for connection 2 is the same however other connections behave.
- The plan is resolved when a connection is accepted, before any data flows.

The seed pins **which faults fire and at which offsets**. It does not make the
operating system's scheduling or timing deterministic, so a timing-sensitive
bug may still need several runs to reproduce.

## How it compares

<!-- TODO before publishing: re-verify every row against current releases. -->

| Tool | Approach | Notes |
|---|---|---|
| Toxiproxy | Separate proxy daemon, usually a container | Mature and language-agnostic; needs infrastructure |
| netfault | Small Python TCP proxy with a fault pipeline | Closest in spirit |
| simloop / simloom | Simulated asyncio event loop with virtual time | Deterministic, but asyncio code only |
| MockServer (chaos mode) | Java server with TCP/HTTP/gRPC faults | Heavier, JVM-based |

flakynet's niche: real sockets, in-process, seeded replay, usable from sync,
threaded and async code alike.

## Non-goals

TLS termination, UDP, payload corruption, HTTP-aware faults, a CLI or daemon.
Record and replay of streams may arrive later as a separate module.

## Roadmap

- **v0.1** core proxy, seven faults, event log, pytest fixture, seed replay
- **v0.2** recipes and tests against redis-py, psycopg, httpx and an asyncio client; plan minimization
- **v0.3** a "bug zoo" of real failures found in popular clients

## Development

```sh
uv sync
uv run pytest
uv run ruff check && uv run ty check
```

See `CLAUDE.md` for design rules and contributor guidance.

## License

TBD (MIT suggested).
