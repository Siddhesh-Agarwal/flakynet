## 2026-10-30 — Proxy engine transport

Decision: the proxy engine uses raw non-blocking sockets with asyncio
`loop.sock_accept`, `sock_connect`, `sock_recv`, and `sock_sendall`, and
`shutdown(SHUT_WR)` for half-close, rather than `StreamReader`/`StreamWriter`.

Reason: explicit socket ownership and `shutdown()` give reliable half-close
semantics on Linux, macOS, and Windows, and make cancellation and cleanup
straightforward.

Cost: more manual buffering and error handling than the stream APIs.

## 2026-10-30 — Phase-1 representation

Decision: keep absolute integer stream offsets, inclusive `Between` random
ranges, `(start, end)` window tuples with `None` meaning the whole stream, and
separate `after_bytes` / `after: Saw` trigger fields.

Reason: this is the existing pure-resolution representation and was confirmed
before extending phase 1. It keeps armed deltas pre-drawn at resolution and
avoids introducing a second offset/window abstraction.

Cost: there is no first-class `stream(...)` selector; stream choice remains the
`side` literal on faults and `Saw.side`.

## 2026-10-30 — Static fault wiring in pumps

Decision: wire static faults into the pumps. This began with `CloseAfter` and
`Reset`; the same pump now also handles `Latency`, `Bandwidth`, `Fragment`,
`Blackhole`, and `RefuseConnect`. Armed `saw` triggers still raise
`NotImplementedError` when a `Proxy` is constructed. Offsets are absolute
stream offsets; each pump splits the current read at the next fault boundary
before forwarding.

Reason: a plan that contains an unwired trigger never silently runs as if it
were fault-free, and every static fault shares one boundary-splitting engine.

Cost: armed triggers and the report/event-expansion work remain.

## 2026-10-30 — Reset/CloseAfter platform TODOs

- TODO(Windows): verify `SO_LINGER` packing (`struct.pack("ii", 1, 0)`) and
  whether the peer observes RST rather than FIN after `Reset` and
  `RefuseConnect`.
- TODO(macOS): verify that immediate `shutdown(SHUT_WR)` + close preserves the
  exact prefix for `CloseAfter`, and whether `Reset` produces RST.
- TODO(all): an RST close may discard bytes still in the kernel send buffer.
  The exact-prefix tests run on loopback and do not prove delivery under load.
- TODO(Windows): default timer granularity can make short `asyncio.sleep`
  intervals overshoot. The Latency/Bandwidth tests use generous lower bounds
  only, but the margin has not been measured on Windows.
- TODO(macOS): loopback scheduling and Nagle may change when segmented writes
  are observable. Fragment tests assert forwarded bytes, not receiver read
  boundaries.

## 2026-10-30 — Proxy name default

Decision: `Proxy(name=None)` now uses `"p0"` for the resolution RNG key and
thread name, instead of an object-identity-derived name. Standalone users who
create multiple proxies should pass distinct names.

Reason: the same seed must replay the same per-connection plan; `id(self)`
changes between runs.

Cost: two unnamed proxies in one process share the same RNG stream.

## 2026-10-30 — Blackhole semantics

Decision: `Blackhole` drains and discards the affected stream after the
trigger. The pump keeps calling `recv` on the source, never writes the bytes
to the peer, and keeps the connection open until the source reaches EOF or the
proxy stops.

Reason: the observable effect is "bytes vanish, connection stays open" without
conflating the fault with sender-side backpressure. Draining also lets the pump
notice EOF and keeps teardown simple.

Cost: the sender's writes do not block, so this does not model a stalled path
where the kernel receive buffer fills. `Bandwidth` and `Latency` cover pacing.

## 2026-10-30 — Latency delay function and events

Decision: the delay for segment ordinal `n` of a stream is
`ms + randint(0, jitter_ms)` at
`random.Random(f"{jitter_key}:{stream}:{n}")`, where `n` is zero-based and
counts every forwarded segment of that stream, including segments outside the
window. Every pump write is one segment, so each `Fragment` piece gets its own
delay. `Latency` logs only `window_entered` and `window_exited`; it never logs
per-segment events.

Reason: the delay sequence is a pure function of the resolved key, stream, and
ordinal, so phase-1 can test it exactly and runtime never consumes shared RNG
state. Keeping per-segment events out of `canonical_events()` avoids claiming
that byte-to-delay placement is replayable.

Cost: segment boundaries come from OS read chunking, so which bytes receive
which delay is not reproducible. The delay is applied before the write, which
makes a low bandwidth rate observable on the first segment but is bursty at
the kernel level.

## 2026-10-30 — Bandwidth pacing

Decision: the rate is drawn once per connection and stream at accept time and
stored in `BandwidthParams`. Inside the window the pump sleeps
`segment_bytes / rate` seconds before writing each segment. Multiple active
`Bandwidth` faults use the slowest rate.

Reason: the trigger remains an offset window, pacing is a pure function of the
resolved rate and the segment size, and throughput is capped without a timer
scheduler.

Cost: actual measured throughput depends on loopback, scheduling, and segment
sizes, and is not promised.

## 2026-10-30 — Event log, first slice

Decision: add `Proxy.events` and `Proxy.canonical_events()` now so the
`Latency` window events are observable. Window faults log `window_entered` and
`window_exited`; discrete faults log `fired`; `RefuseConnect` logs `fired`
with `stream=None`. `canonical_events()` sorts by
`(conn_index, stream, offset, fault_index, kind)` and excludes `seq` and
timestamps; a non-exact offset becomes `None`.

Reason: the fault determinism tests need a replay-stable comparison, and
`Latency` explicitly must not emit per-segment events. Keeping the vocabulary
small now leaves room for `fault_report()` later.

Cost: event kinds and report statuses are plain strings, not closed enums;
their vocabulary is documented in `events.py`, `report.py`, and this file.

## 2026-10-30 — Event record and report

Decision: every event stores `conn_index`, `stream`, `offset`, `offset_exact`,
`fault_index`, fault class name, kind, resolved planned params, `seq`, and a
monotonic timestamp. `canonical_events()` keeps the plan-derived fields,
removes `seq`/timestamps, sorts by
`(conn_index, stream, offset, fault_index, kind)`, and turns a non-exact
offset into `None`.

Decision: `fault_report()` returns one `FaultStatus` per (fault, connection),
plus an `UNMATCHED` entry when no accepted connection matched the fault's
selector. Each entry records the status, both forwarded byte counts, and how
the connection ended. Status precedence is:

1. `FIRED` if an event for the fault exists on the connection.
2. `PENDING` if the connection has not finished.
3. `MISSED` if the fault's target stream ended gracefully.
4. `PREEMPTED` if a terminal flakynet fault fired on the connection.
5. `ENDED_EARLY` if the connection ended by client abort, server abort, or
   error.
6. `MISSED` otherwise.

Reason: the offset and fault name make events self-describing, while the
sorted canonical view stays replay-stable. Preferring `MISSED` for a stream
that completed normally avoids calling a cross-stream terminal fault
"preemption" when the fault's own stream had already finished.

Cost: the report collapses side=`both` faults to one per connection, so it
does not say which stream fired. Event records retain that detail.

## 2026-10-30 — Connection end tracking

Decision: each stream records `graceful`, `aborted`, `flakynet`, `cancelled`,
`error`, or `pending`; the connection combines them into `graceful`,
`client_abort`, `server_abort`, `error`, `flakynet`, or `pending`. A receive
error marks the source stream aborted; a send error marks the destination
stream aborted. All state, event, and report access is guarded by the proxy's
single lock.

Reason: distinguishing client and server aborts needs the direction of the
failing socket operation, not just the pump that raised.

Cost: a connection cancelled by `stop()` remains `PENDING`, because it was
never observed to end on its own.

## 2026-10-30 — Determinism audit

Decision: narrow the README claim. The seed pins per-connection phase-1
resolution only. Static offsets and same-stream `saw` anchors are
chunk-invariant, and their events are stable when the application byte stream
and close boundaries are deterministic and no sibling terminal/cross-stream
race cuts the stream short. The README now explicitly says that
`canonical_events()` removes order and inexact offsets but cannot make a
scheduling-dependent event occur.

Findings:

- Competing terminals: a `Reset`/`CloseAfter` on either stream can win a race
  and cancel the other. Which stream logs `fired` can differ.
- Sibling cancellation: a terminal on one stream can cancel the other before it
  logs `window_entered`/`window_exited`, so those events can be partial or
  absent.
- Cross-stream `saw`: the target-stream offset is inexact, and the fault can
  miss entirely if the target already closed.
- Concurrent accepts: `_next_index` follows `sock_accept` order, so the
  client-to-connection-index mapping is OS-scheduled.
- Runtime RNG: none. `latency_delay_ms` builds a fresh `random.Random` from
  `(jitter_key, stream, ordinal)`; resolution uses a local `random.Random`.
  Process-wide `random.seed` does not affect events.
- Dict/set ordering: no effect. Event offsets are integer arithmetic over
  ordered tuples; `canonical_events()` sorts; `build_report()` sorts connection
  snapshots; membership sets are never iterated for ordering.
- Measured time: no fault value reads elapsed time. `asyncio.sleep` durations
  are planned values; actual sleep overshoot only changes scheduling and
  pacing.
- Partial reads: static offsets and same-stream anchors are chunk-invariant;
  `Latency` delay placement and `Bandwidth` segment sizes are not.

Tests: `tests/test_determinism_limits.py` demonstrates the terminal winner,
sibling window cancellation, cross-stream miss, accept-order mapping,
chunk-invariant window events, global-RNG independence, and pacing-time
independence.

Reason: the previous wording implied that comparing `canonical_events()` was
sufficient for any replay. The audit showed that event occurrence, not just
offset exactness, can be scheduling-dependent.

Cost: callers cannot assert exact canonical events for plans that let two
streams race; they must compare per-connection sequences or design
request/response triggers.

## 2026-10-30 — Public factory and pytest plugin

Decision: expose `flakynet.proxy(...)` as a thin factory in `api.py` that
returns a `Proxy` without binding. `Proxy` itself remains the context manager
and owns `start()`/`stop()`, so construction, binding, and teardown stay
separate.

Decision: the `pytest11` entry point provides a `flakynet` fixture that is a
factory. Each `.proxy(...)` starts immediately and registers `stop()` as a
pytest finalizer, which runs LIFO. Unnamed proxies use `p0`, `p1`, ... in
creation order. Seed precedence is explicit `seed=` (pinned), then
`--flakynet-seed`, then the current sweep seed, then `0`.

Decision: `--flakynet-sweep=N` parametrizes the fixture indirectly with seeds
`0..N-1` and stops the test's remaining seeds after the first failure. The
failure section lists each proxy's name, effective seed, pinned state, plan
repr, and replay command, and the terminal summary names the first failing
seed.

Reason: the fixture must hand the test an already-started proxy without making
the caller own the loop, while the failure report must be enough to replay the
exact run. Indirect parametrization keeps the sweep invisible in test
signatures and lets pytest's normal fixture setup/teardown run per seed.

Cost: the replay command strips the sweep parameter from the nodeid; tests
that also use other parametrizations should be replayed with their parameters
added back manually. String seeds passed through the CLI are converted to
`int` when they parse as integers.

## 2026-10-30 — Integration-test services

Decision: keep real-client dependencies in a separate `integration` group and
mark the suite `integration`. Redis and PostgreSQL come from `compose.yaml`
locally and from GitHub Actions service containers in CI; the httpx/aiohttp
tests start a real HTTP server in a session fixture. Service fixtures ping the
backend and skip the affected tests when it is absent, so the normal dev test
run does not require Docker.

Reason: protocol clients need real servers to expose their actual retry,
exception, and framing behavior, but the core proxy suite must stay fast and
portable.

Cost: the integration job installs optional clients and needs service
containers; local runs need `docker compose up -d` (or env-provided services).
Observed client surprises and review candidates live in `docs/bug-zoo.md`.

## 2026-10-30 — Proxy lifecycle

Decision: each `Proxy` owns one daemon thread and one asyncio loop. `start()`
binds port 0 and waits for readiness; `stop()` closes the listener, cancels the
accept loop and all connection tasks, closes sockets, and joins the thread.
Same-instance restart is supported, and byte counters reset on each `start()`.

Reason: the caller never owns an event loop, and the lifecycle maps directly to
a context manager.

Cost: one thread per proxy instance for the lifetime of the proxy.
