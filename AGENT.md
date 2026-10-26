# AGENT.md — flakynet

## What this project is

`flakynet` is a Python library that runs an in-process TCP proxy and injects
**seeded, reproducible network faults** between a client and a real server.
It is for testing how clients behave when the network misbehaves: resets,
truncated responses, fragmented writes, stalls, lost acknowledgements.

Positioning: real sockets, works with any client (sync, async, threaded, even
non-Python), no Docker or sidecar, and any failure replays from a seed.

Status: pre-alpha, design phase. `README.md` shows the _target_ API. Treat it
as a spec to refine, not as documentation of existing behaviour.

## Glossary (use these terms exactly)

- **Stream**: one direction of one connection. `side="client"` means the bytes
  the _client sends_ (client→server stream). `side="server"` means the bytes
  the _server sends_ (server→client stream). A fault's `side` is the stream it
  affects. `saw(..., side=...)` names the stream it _observes_.
- **Connection index**: 0-based, assigned in accept order, per proxy.
- **Stream offset**: count of bytes of that stream already forwarded.
- **Discrete fault**: acts once at a point (`Reset`, `CloseAfter`, `Blackhole`
  start, `RefuseConnect`). **Window fault**: acts over an offset range
  `[start, end)` (`Latency`, `Bandwidth`, `Fragment`); default window is the
  whole stream.
- **Static fault**: its offset/window is fully known at accept time.
  **Armed fault**: its start depends on a predicate (`saw`) and is only
  anchored once the predicate matches at runtime.
- **Fired**: a discrete fault executed its action; a window fault forwarded its
  first byte inside the window.

## Non-negotiable design rules

1. **Own event loop in a background thread.** The proxy runs its own asyncio
   loop in a daemon thread. Test code can be sync or async and just connects to
   `proxy.host:proxy.port`. Never require the caller to run an event loop.
2. **Triggers are stream offsets; pacing is applied after the trigger.**
   Faults start and end at stream offsets (or at offsets anchored by a
   predicate), never at wall-clock times or OS read boundaries. `Latency` and
   `Bandwidth` are inherently time-based _effects_: the trigger is the window
   `[start, end)` in offsets, and the delay or rate is applied to bytes inside
   it. The OS decides how `recv` chunks data, so the pump must split forwarding
   exactly at window and fault boundaries.
3. **Two-phase resolution.**
   - _Phase 1, accept time (pure):_ `resolve(plan, seed, proxy_name,
conn_index) -> ConnectionPlan`. All random draws happen here. Output has
     `StaticFault`s (absolute offsets/windows, pre-drawn parameters) and
     `ArmedFault`s (a trigger spec plus pre-drawn parameters, e.g. an extra
     `delta` bytes after the match). Unit-testable without sockets.
   - _Phase 2, runtime:_ pumps feed stream bytes to **matchers** (pure state
     machines, `feed(chunk) -> match_end_offset | None`, invariant to how bytes
     are chunked). When a matcher completes at stream offset `M`:
     same-stream faults anchor at `M + delta` and become static;
     cross-stream faults (trigger observes one side, fault affects the other)
     activate from the next byte forwarded in the target stream, and the event
     records the actual offset with `offset_exact=False`.
   - Runtime code never consumes a shared RNG stream. Any value needed at
     runtime (for example the jitter for the n-th forwarded segment) is a pure
     function of `(resolution key, stream, ordinal)`.
4. **RNG keying.** `random.Random(f"{seed}:{proxy_name}:{conn_index}")` for
   resolution; derived sub-keys append further fields. `proxy_name` defaults to
   `p0`, `p1`, ... in creation order within a test, so two proxies in one test
   do not mirror each other. String seeds are stable across processes.
   **Never use `hash()`** (it varies with `PYTHONHASHSEED`).
5. **Canonical events.** `proxy.events` is arrival order, with `seq` and
   monotonic timestamps; it is for debugging and is NOT stable across runs.
   `proxy.canonical_events()` returns a tuple sorted by
   `(conn_index, stream, offset, fault_index, kind)` with `seq` and timestamps
   removed, and `offset=None` where `offset_exact=False`. All replay and
   determinism comparisons use `canonical_events()` only.
6. **Honest determinism.** The seed pins the plan: which faults apply to which
   connection, their offsets/windows, and their planned parameters (rate,
   deltas, and the delay assigned to the n-th forwarded segment of a stream).
   It does not pin OS scheduling, measured timing, or application bytes.
   `Latency` is the one fault whose byte-level effect is not replayable: the
   sequence of delay values is pinned, but which bytes share a segment, and so
   which bytes receive which delay, depends on how the OS chunked reads. Offsets that depend on content (`saw`) are reproducible
   only if the application sends identical bytes. Connection index is assigned
   in accept order, so it is stable only if the client opens connections in a
   deterministic order. README and docstrings must not claim more.
7. **Do not use the term "packet loss".** TCP hides it. Name faults for what
   they do at the socket level (reset, truncate, stall, fragment).
8. **Zero runtime dependencies.** Standard library only. `pytest` is an
   optional integration, loaded via the `pytest11` entry point.

## API contract (v0.1, confirm before changing)

```python
def proxy(
    target: tuple[str, int],
    plan: Plan | Fault | Iterable[Fault] = (),
    *,
    seed: int | str = 0,
    name: str | None = None,
    host: str = "127.0.0.1",
) -> Proxy: ...
```

- `Plan(*faults)` is the canonical type: immutable, with a stable `repr` used
  in failure reports. `proxy()` normalizes `Plan | Fault | Iterable[Fault]` via
  `Plan.of(x)`. An empty plan is a plain pass-through proxy. Docs and examples
  show `Plan(...)` for multi-fault cases and a bare fault only as shorthand.
- `Proxy` is a context manager. Construction does not bind a socket;
  `__enter__`/`start()` binds (port 0) and starts the loop thread;
  `__exit__`/`stop()` closes listeners, cancels pumps and joins the thread.
  `events`, `canonical_events()` and `fault_report()` stay readable after stop.
- Seed default is `0` everywhere, so tests are deterministic by default and
  variety is opt-in (explicit seeds or `--flakynet-sweep`). `proxy.seed`,
  `proxy.name` and `proxy.plan` are always readable.
- **pytest fixture `flakynet` is a factory object**, not a module facade and
  not a started proxy: `flakynet.proxy(target, plan=(), *, seed=None, name=None)`
  returns an **already-started** `Proxy` registered for teardown at test end
  (LIFO). Multiple proxies per test are supported.
- **Seed precedence in the fixture:** (1) explicit `seed=` argument (pinned,
  never overridden; a sweep reports "seed pinned" for that test), (2)
  `--flakynet-seed`, (3) the current `--flakynet-sweep` value, (4) `0`.
- **Failure report** (pytest report section) lists, per proxy: name, effective
  seed and whether it was pinned, `repr(plan)`, and the replay command
  (`pytest path::test --flakynet-seed=N`).

## Fault reports and assert_all_fired

`proxy.fault_report()` returns per-(fault, connection) statuses:

| Status        | Meaning                                                                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `FIRED`       | The fault fired (see Glossary).                                                                                                                  |
| `PREEMPTED`   | flakynet ended the connection with an earlier fault first.                                                                                       |
| `ENDED_EARLY` | An endpoint aborted/reset/errored the connection before the trigger was reached.                                                                 |
| `MISSED`      | The exchange ended gracefully (or the stream completed) and the trigger was never reached: offset beyond stream length, or `saw` pattern absent. |
| `PENDING`     | The connection was still open when the report was taken.                                                                                         |

Fault-level `UNMATCHED` means no accepted connection matched the fault's
selector (for example `nth(2)` with only two connections).

`assert_all_fired(allow=frozenset())` passes when every fault is `FIRED` on at
least one connection, or its only non-fired outcomes are in the default
excused set `{PREEMPTED, ENDED_EARLY}` plus anything in `allow`. It fails on
`MISSED`, `PENDING` and `UNMATCHED` unless allowed (for example
`allow={"unmatched"}`), and the message lists each fault with connection,
status, bytes seen and how the connection ended. "Reachable but did not fire"
means `MISSED` or `PENDING`; "never applicable" means `PREEMPTED`,
`ENDED_EARLY`, or `UNMATCHED` that the test allowed. Call it after the client
has finished; calling it earlier reports `PENDING`.

## Tooling and layout

- Python 3.10+. Build backend: hatchling. Env and runner: `uv`.
- Lint/format: `ruff`. Types: `ty` in strict mode. Tests: `pytest` plus `hypothesis`.
- Layout: `src/flakynet/`, `tests/`, `docs/`.
- Planned modules (create only when needed): `proxy.py` (loop thread, accept,
  pumps), `plan.py` (Plan, selectors, `between`, `nth`, `every`, `saw`),
  `faults.py` (frozen dataclasses), `resolve.py` (pure phase-1 resolution),
  `matcher.py` (pure chunk-invariant pattern matcher), `events.py`,
  `report.py` (fault statuses), `pytest_plugin.py`.

Commands:

```
uv sync
uv run ruff check . && uv run ruff format --check .
uv run ty check
uv run pytest                      # fast unit + socket tests
uv run pytest -m integration       # real-client tests (may need services)
```

## Code style

- Fully typed public API. Fault types are `@dataclass(frozen=True, slots=True)`.
- No module-level mutable state. No global singletons.
- Public functions and classes get docstrings stating exact semantics,
  including what happens on each OS when relevant.
- Small, single-purpose functions. Prefer explicit over clever.
- Errors raised to users are specific exceptions defined in the package.

## Testing rules

- Tests use real loopback sockets. Bind to port 0; never hard-code ports.
- Do not use `time.sleep` to wait for state. Use events, barriers or polling
  with a deadline.
- Timing assertions need generous tolerances. Mark inherently timing-sensitive
  tests so they can be retried or deselected on slow CI.
- Every fault needs: (a) a pure resolution test, (b) a raw-socket behaviour
  test, (c) a determinism test.
- Pure-level determinism (Hypothesis): for random plans, seeds, proxy names and
  connection indexes, resolving twice yields identical `ConnectionPlan`s;
  different connection indexes are independent.
- Matcher property test: for any pattern, stream and chunking, `feed` returns
  the same match-end offset.
- Socket-level determinism: run the same scenario twice with a deterministic
  client (sequential connections, fixed payloads) and compare
  `canonical_events()`, never `events`. Concurrent-connection scenarios compare
  per-connection event sequences only.
- `Latency`: the delay for segment `n` is tested exactly as a pure function of
  `(key, stream, n)`. Socket tests assert delays only with generous tolerances
  and never assert which bytes were delayed.
- Fault-report tests cover each status, including `MISSED` vs `ENDED_EARLY`.
- Real-client tests (redis-py, psycopg, httpx, one asyncio client) are marked
  `integration`.

## Working agreements for Claude

- **Plan first** for anything larger than a single function: state the plan,
  list files you will touch, then implement.
- **Ask before** changing the public API, adding a dependency, or widening
  scope. When a requirement is ambiguous, ask a specific question instead of
  guessing.
- Make small, focused commits. One concern per commit. Do not mix refactors
  with behaviour changes.
- Keep `README.md` and docstrings in sync with the API. If you change an
  example, run it.
- Log non-obvious design decisions in `DECISIONS.md` (date, decision, reason).
- Report honestly: say what you did not test, and any behaviour you are unsure
  about on Windows or macOS.

## MVP scope (v0.1)

In: TCP proxy (sync context manager and async wrapper), faults `Latency`,
`Bandwidth`, `Fragment`, `Reset`, `CloseAfter`, `Blackhole`, `RefuseConnect`;
selectors (stream, connection `nth`/`every`/all, byte offset, `saw(bytes)`);
two-phase seeded resolution; events and `canonical_events()`; `fault_report()`
and `assert_all_fired()`; pytest fixture with seed reporting,
`--flakynet-seed`, `--flakynet-sweep`.

Out (do not build, even if tempting): TLS termination, UDP, payload corruption,
HTTP-aware faults, CLI or daemon mode, record/replay (a later `wscassette`
module), anything requiring root or kernel features.

## Confirmed decisions

- `assert_all_fired` default: `PREEMPTED` and `ENDED_EARLY` are excused;
  `MISSED`, `PENDING` and `UNMATCHED` fail unless listed in `allow=`.
- `Latency` is applied **per forwarded segment** (the unit the pump writes to
  the peer after splitting at fault boundaries; each piece produced by
  `Fragment` is its own segment). The delay for the n-th segment of a stream is
  a pure function of `(key, stream, n)`, drawn from `ms` and `jitter_ms`. The
  window `[start, end)` that selects which offsets it applies to is static or
  armed like any other fault. Segment boundaries come from the OS, so which
  bytes get which delay is NOT replayable; document this in the `Latency`
  docstring and README. `Latency` logs only `window_entered`/`window_exited`
  events, never per-segment events, so `canonical_events()` is unaffected.
- `Bandwidth` rate is drawn once per connection and stream at accept time;
  measured throughput is not promised.

## Decisions to confirm with the maintainer before implementing

- `Blackhole` semantics: drain-and-discard the source (default candidate) vs
  stop reading so backpressure builds.
- `saw(bytes)`: fault applies from the end of the match (`M`), plus optional
  pre-drawn `delta`; rolling buffer holds `len(pattern) - 1` bytes.
- Whether the async API is a thin wrapper over the thread-based proxy or a
  separate class.
- Package name availability on PyPI (`flakynet` is unchecked).
