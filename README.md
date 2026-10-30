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
    print(p.canonical_events())   # compare with identical stream bytes; see limits below
```

**Plans.** `Plan(*faults)` is the canonical form. `proxy()` also accepts a
single fault or any iterable of faults and normalizes them, so
`fn.proxy(target, fn.Reset(...))` is shorthand for
`fn.proxy(target, fn.Plan(fn.Reset(...)))`. An empty plan is a pass-through proxy.

**Sides.** `side` names the stream a fault affects: `"client"` is what the
client sends, `"server"` is what the server sends. `saw(..., side=...)` names
the stream it watches.

**Seeds.** `seed` defaults to `0`, so phase-1 plans are deterministic unless
you ask for variety (explicit seeds, or `--flakynet-sweep`). Runtime event
occurrence still depends on scheduling; see the limits below.

### With pytest

```python
def test_redis_survives(flakynet):   # flakynet is a factory, not a started proxy
    p = flakynet.proxy(("127.0.0.1", 6379), fn.Reset(after_bytes=fn.between(10, 500)))
    client = redis.Redis(host=p.host, port=p.port)   # p is already started
    ...                                              # stopped automatically at test end
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
pytest --flakynet-sweep=200     # try seeds 0..199; stop at the first failing seed
```

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

## Faults (v0.1 plan)

| Fault           | Kind     | What it does at the socket level                              |
| --------------- | -------- | ------------------------------------------------------------- |
| `Latency`       | window   | Delays each forwarded segment by a planned amount plus jitter |
| `Bandwidth`     | window   | Paces segments at a planned bytes-per-second rate             |
| `Fragment`      | window   | Forwards data in pieces of at most N bytes                    |
| `CloseAfter`    | discrete | Graceful close (FIN) after N bytes                            |
| `Reset`         | discrete | Abortive close (RST) after N bytes                            |
| `Blackhole`     | discrete | Drains and discards after the trigger, leaving the connection open |
| `RefuseConnect` | discrete | Accepts then immediately resets selected new connections      |

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

## How determinism works (and what it does not promise)

**The seed pins phase-1 resolution, not the runtime schedule.** For each
connection, `(seed, proxy name, connection index)` deterministically chooses
which faults apply, their static offsets/windows, bandwidth rates, armed
deltas, and the latency jitter key. No runtime code draws from a shared RNG,
and the process-wide `random` state is irrelevant. Faults are still triggered
by byte offsets, not by timers: `Latency` and `Bandwidth` select bytes with an
offset window and apply pacing inside it.

**Deterministic offsets need a deterministic byte stream.** For a single
connection with identical bytes in each direction and identical close
boundaries, static faults and same-stream `saw` anchors produce identical
offsets. Their event entries are identical when the stream is allowed to reach
the same end. The matcher is chunk-invariant, and the pump splits at fault
boundaries, so OS read sizes do not move a static offset or a same-stream
anchor. `Latency` logs only `window_entered`/`window_exited`, never
per-segment delays.

**What is not deterministic:**

- _Arrival order and timestamps._ `events` is diagnostic only.
  `canonical_events()` sorts and strips those fields, but sorting cannot make a
  scheduling-dependent event _happen_.
- _Concurrent connection indices._ The index is assigned in accept order. Under
  simultaneous connects the OS decides that order, so the mapping from a client
  to `p0`, `p1`, ... (and therefore to its resolved plan) can change. Compare
  per-connection event sequences, or serialize connects when client identity
  matters.
- _Cross-stream `saw` faults._ These activate on the next byte forwarded in the
  target stream. The actual offset is recorded with `offset_exact=False` and
  canonicalized to `offset=None`. If the target stream has already closed, the
  fault never fires at all. Replay is reliable only for request/response
  protocols where the target cannot finish before the trigger.
- _Competing terminals and closing races._ A `Reset`/`CloseAfter` closes both
  sockets and cancels the sibling pump. If terminals race on both streams, the
  winning stream, and whether both `fired` events appear, can vary. A terminal
  can also cancel the sibling before it logs
  `window_entered`/`window_exited`, so window events on that stream may be
  partial or absent. `stop()` cancels in-flight pumps in the same way.
- _Chunking-dependent pacing._ Static offsets and same-stream anchors are
  chunk-invariant, but `Latency` segment boundaries (and therefore which bytes
  get which delay) and `Bandwidth` segment sizes/timing follow OS reads.
- _Measured time._ Planned values are fixed, but actual elapsed time,
  throughput, and the interleaving of the two pumps are not. No fault value is
  computed from measured elapsed time.
- _Application and platform behaviour._ Different bytes, close timing, or
  server responses change content-dependent triggers, stream lengths, and
  cross-stream races. RST vs FIN and bytes in flight at close are OS-dependent.
- _Python runtime versions._ Seed replay is tested within a Python runtime;
  cross-version equality of the standard-library RNG is not part of the
  promise.
- _Report statuses._ `PREEMPTED`, `ENDED_EARLY`, `MISSED`, and `PENDING` can
  follow the same races; take the report after the client has finished.

**Replaying and comparing.** `proxy.events` is in arrival order and is not
stable between runs. Each event records the connection, stream, offset,
whether the offset is exact, fault index, fault name, kind, resolved planned
parameters, sequence number, and monotonic timestamp. `proxy.canonical_events()`
sorts by `(connection, stream, offset, fault_index, kind)` and removes the
sequence number and timestamp; a non-exact offset becomes `None`. Window faults
log `window_entered` and `window_exited`; discrete faults log `fired`.
`Latency` logs only the two window events, never one per segment.

Use `canonical_events()` for the part the seed pins. When comparing concurrent
connections, compare the multiset of per-connection event sequences, not the
mapping to clients. A difference in a cross-stream occurrence, a terminal race,
or a sibling window event is a scheduling difference, not a seed defect.
`tests/test_determinism_limits.py` pins these limits.

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

Each report entry also records how the connection ended (`graceful`,
`client_abort`, `server_abort`, `error`, or `flakynet`) and the bytes forwarded
in each direction.

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
uv run pytest -m integration      # after starting the services above
uv run ruff check && uv run pyright
```

See [`AGENT.md`](./AGENT.md) for design rules and contributor guidance.

## License

TBD (MIT suggested).
