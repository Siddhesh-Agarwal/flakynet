# flakynet

Seeded, reproducible network faults for testing real clients over real sockets.
No Docker, no sidecar, no event-loop requirement.

> **Status: pre-alpha, design phase.** The examples below show the _target_
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
    fn.Fragment(max_bytes=3, side="server"),              # split what the server sends
    fn.Reset(after_bytes=fn.between(200, 4000),           # TCP RST
             side="server", conn=fn.nth(2)),
    fn.Blackhole(after=fn.saw(b"COMMIT", side="client"),  # request delivered, reply never arrives
                 side="server"),
)

with fn.proxy(("127.0.0.1", 5432), plan, seed=7) as p:    # starts on enter, stops on exit
    conn = psycopg.connect(host=p.host, port=p.port, dbname="test")
    ...
    p.assert_all_fired()          # fails if a fault was reachable but never fired
    print(p.canonical_events())   # stable, comparable across runs
```

**Plans.** `Plan(*faults)` is the canonical form. `proxy()` also accepts a
single fault or any iterable of faults and normalizes them, so
`fn.proxy(target, fn.Reset(...))` is shorthand for
`fn.proxy(target, fn.Plan(fn.Reset(...)))`. An empty plan is a pass-through proxy.

**Sides.** `side` names the stream a fault affects: `"client"` is what the
client sends, `"server"` is what the server sends. `saw(..., side=...)` names
the stream it watches.

**Seeds.** `seed` defaults to `0`, so runs are deterministic unless you ask
for variety (explicit seeds, or `--flakynet-sweep`).

### With pytest

```python
def test_redis_survives(flakynet):   # flakynet is a factory, not a started proxy
    p = flakynet.proxy(("127.0.0.1", 6379), fn.Reset(after_bytes=fn.between(10, 500)))
    client = redis.Redis(host=p.host, port=p.port)   # p is already started
    ...                                              # stopped automatically at test end
```

`flakynet.proxy(...)` returns an already-started proxy and stops it when the
test ends; you can create several per test. The seed is chosen in this order:
an explicit `seed=` in the test (pinned), then `--flakynet-seed`, then the
current `--flakynet-sweep` value, then `0`. When a test using a proxy fails,
the report prints each proxy's name, seed, plan and the exact replay command.

```
pytest --flakynet-seed=7        # replay one failing run
pytest --flakynet-sweep=200     # try seeds 0..199 and report the failing seed + plan
```

## Faults (v0.1 plan)

| Fault           | Kind     | What it does at the socket level                              |
| --------------- | -------- | ------------------------------------------------------------- |
| `Latency`       | window   | Delays each forwarded segment by a planned amount plus jitter |
| `Bandwidth`     | window   | Caps throughput in bytes per second                           |
| `Fragment`      | window   | Forwards data in pieces of at most N bytes                    |
| `CloseAfter`    | discrete | Graceful close (FIN) after N bytes                            |
| `Reset`         | discrete | Abortive close (RST) after N bytes                            |
| `Blackhole`     | discrete | Stops forwarding while keeping the connection open            |
| `RefuseConnect` | discrete | Rejects or immediately closes new connections                 |

There is deliberately no "packet loss" fault. TCP hides packet loss from
applications; what a client actually sees is stalls, resets, truncation and
fragmentation, which is what flakynet models.

## How determinism works (and what it does not promise)

**Triggers are byte offsets; pacing happens after the trigger.** Faults start
and end at positions in a stream, not at timers and not at however the OS
happened to chunk a read. `Latency` and `Bandwidth` are time-based by nature:
the offset window decides _where_ they apply, and the delay or rate is applied
to the bytes inside it.

**Two phases.**

1. _At accept time_ each connection's plan is resolved from
   `(seed, proxy name, connection index)`. All random choices are made here:
   offsets, windows, rates, and the seed for per-segment latency jitter. Faults whose position is known
   now are _static_.
2. _While data flows_, faults triggered by `saw(...)` are _armed_. When the
   pattern completes at stream offset `M`, a fault on the same stream anchors
   at `M` (plus any pre-drawn extra bytes) and from then on behaves like a
   static one. No new random numbers are drawn at runtime.

A fault on the _other_ stream than the one being watched (as in the
`Blackhole` example above) starts with the next byte forwarded after the
pattern completes. The offset it lands on is recorded, but it is only
reproducible for request/response protocols, where the other side cannot send
before it has been asked.

**Replaying and comparing.** `proxy.events` is in arrival order and is not
stable between runs. `proxy.canonical_events()` is sorted by
`(connection, stream, offset, fault)` without timestamps or arrival order;
compare that.

**What the seed pins:** which faults apply to which connection, their offsets
and windows, and their planned parameters (rate, extra bytes after a match,
and the delay assigned to the n-th forwarded segment).

**What it does not pin:** OS scheduling or measured elapsed time; how the OS
chunks reads, and therefore which bytes share a `Latency` segment (the delay
values are replayable by segment number, their placement on bytes is not); the bytes
your application sends (offsets that depend on content are reproducible only
if the content is identical); and connection numbering, which follows accept
order, so it is stable only if your client opens connections in a
deterministic order.

## Fault reports

```python
p.fault_report()            # status per (fault, connection)
p.assert_all_fired()        # raises with a readable diagnosis
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

- **v0.1** core proxy, seven faults, events and fault reports, pytest fixture, seed replay
- **v0.2** recipes and tests against redis-py, psycopg, httpx and an asyncio client; plan minimization
- **v0.3** a "bug zoo" of real failures found in popular clients

## Development

```sh
uv sync
uv run pytest
uv run ruff check && uv run ty check
```

See [`AGENT.md`](./AGENT.md) for design rules and contributor guidance.

## License

TBD (MIT suggested).
