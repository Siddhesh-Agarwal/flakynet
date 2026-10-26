# AGENT.md — flakynet

## What this project is

`flakynet` is a Python library that runs an in-process TCP proxy and injects
**seeded, reproducible network faults** between a client and a real server.
It is for testing how clients behave when the network misbehaves: resets,
truncated responses, fragmented writes, stalls, lost acknowledgements.

Positioning: real sockets, works with any client (sync, async, threaded, even
non-Python), no Docker or sidecar, and any failure replays from a seed.

Status: pre-alpha, design phase. `README.md` shows the *target* API. Treat it
as a spec to refine, not as documentation of existing behaviour.

## Non-negotiable design rules

1. **Own event loop in a background thread.** The proxy runs its own asyncio
   loop in a daemon thread. Test code can be sync or async and just connects to
   `proxy.host:proxy.port`. Never require the caller to run an event loop.
2. **Faults trigger on stream byte offsets, never on wall-clock time or on OS
   read sizes.** The OS decides how `recv` chunks data, so "fault on the 3rd
   chunk" is not reproducible. Track a per-direction byte offset and split
   forwarding exactly at fault boundaries.
3. **Plan resolution is a pure function** of `(plan, seed, connection_index)`.
   It runs at accept time, before any bytes flow, and yields concrete
   `ResolvedFault` objects with absolute offsets or predicates. It must be
   unit-testable without sockets.
4. **Per-connection RNG:** `random.Random(f"{seed}:{conn_index}")`. String
   seeds are stable across processes. **Never use `hash()`** (it varies with
   `PYTHONHASHSEED`).
5. **Be honest about determinism.** The seed pins the *fault plan and its
   offsets*. It does not pin OS scheduling or timing. README and docstrings
   must never claim more than that.
6. **Do not use the term "packet loss".** TCP hides it. Name faults for what
   they do at the socket level (reset, truncate, stall, fragment).
7. **Zero runtime dependencies.** Standard library only. `pytest` is an
   optional integration, loaded via the `pytest11` entry point.

## Tooling and layout

- Python 3.10+. Build backend: hatchling. Env and runner: `uv`.
- Lint/format: `ruff`. Types: `ty` in strict mode. Tests: `pytest` plus `hypothesis`.
- Layout: `src/flakynet/`, `tests/`, `docs/`.
- Planned modules (create only when needed):
  `proxy.py` (loop thread, accept, pumps), `plan.py` (Plan, selectors, `between`,
  `nth`, `every`, `saw`), `faults.py` (frozen dataclasses), `resolve.py`
  (pure plan resolution), `events.py`, `pytest_plugin.py`.

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
  test, (c) a determinism test (same seed gives same events).
- Determinism is tested with Hypothesis: for random plans and seeds, resolving
  twice yields identical `ResolvedFault` lists.
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
selectors (direction, connection `nth`/`every`/all, byte offset, `saw(bytes)`);
seeded resolution; event log; `assert_all_fired()`; pytest fixture with seed
reporting, `--flakynet-seed`, `--flakynet-sweep`.

Out (do not build, even if tempting): TLS termination, UDP, payload corruption,
HTTP-aware faults, CLI or daemon mode, record/replay (a later `wscassette`
module), anything requiring root or kernel features.

## Decisions to confirm with the maintainer before implementing

- `Blackhole` semantics: drain-and-discard the source (default candidate) vs
  stop reading so backpressure builds.
- Exact meaning of `saw(bytes)` across chunk boundaries (rolling buffer size,
  whether the fault applies from the next byte or the end of the match).
- Whether `Latency` is applied per forwarded segment or per fault boundary.
- Whether the async API is a thin wrapper over the thread-based proxy or a
  separate class.
- Package name availability on PyPI (`flakynet` is unchecked).

