# flakynet Slice 1: Pure Resolution and Matcher — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the dependency-free core of flakynet: fault/plan types, deterministic phase-1 resolution, and a chunk-invariant `saw` matcher, with tests, without implementing the proxy.

**Architecture:** Keep the first slice entirely pure. `plan.py` defines selectors and `Plan`; `faults.py` defines frozen fault dataclasses; `resolve.py` turns `(plan, seed, proxy_name, conn_index)` into a `ConnectionPlan` using a single per-connection `random.Random`; `matcher.py` implements `saw` as a chunk-invariant state machine. No sockets, threads, events, or pytest plugin in this slice.

**Tech Stack:** Python 3.10+, stdlib only at runtime, uv + `uv_build`, pytest, Hypothesis, ruff, ty.

**Spec:** `AGENT.md` (design rules and API contract) and `README.md` (target API). This plan implements the pure subset of AGENT's v0.1 scope.

## Global Constraints

- Python floor: `requires-python = ">=3.10"`. Use `from __future__ import annotations`; do not use syntax newer than 3.10.
- Build backend: `uv_build`; keep the `src/` layout.
- Zero runtime dependencies. Test/tool dependencies only in `[dependency-groups].dev`.
- Frozen fault dataclasses with `slots=True`.
- No module-level mutable state and no global singletons.
- Connection index is 0-based, per proxy; reject negative indexes.
- Fault `side` defaults to `"both"`; `saw(..., side=...)` requires an explicit `"client"` or `"server"`.
- `window=None` means the whole stream; `window=(start, end)` uses `[start, end)`, and `end=None` means end of stream.
- `between(lo, hi)` is inclusive and uniform; require `0 <= lo <= hi`.
- RNG key is exactly `f"{seed}:{proxy_name}:{conn_index}"`; never use `hash()`.
- `Blackhole` is drain-and-discard; `RefuseConnect` is accept-then-RST; the async API is a thin wrapper. These are proxy-slice decisions and must not leak into this slice.
- `assert_all_fired` semantics: every applicable `(fault, connection)` must be `FIRED` or excused. Not implemented here; keep the model compatible by expanding `side="both"` into two resolved applications.
- Armed `saw` triggers are supported for discrete faults in this slice (the documented `Blackhole` case); armed window faults are deferred.
- No proxy, socket, thread, pytest plugin, event log, or fault report in this slice.

## Review Focus

1. **Non-fault input to `Plan.of`:** a string, bytes, or list of non-faults must raise a specific `TypeError`, not be silently iterated.
2. **Invalid numeric ranges:** negative `between`, negative `after_bytes`, `max_bytes=0`, and `end < start` must fail with `ValueError`, not produce nonsense offsets.
3. **`saw` chunk invariance:** a pattern split across arbitrary chunk boundaries must return the same exclusive match-end offset as a single-chunk feed; an empty pattern must raise.
4. **Connection selectivity:** a fault scoped to `nth(1)` must produce no resolved faults for connection 0, and `fault_index` must still refer to the original plan index.
5. **`side="both"` expansion:** a fault affecting both streams must resolve to two applications with the same `fault_index` but different streams, so the later report layer can require each application to pass.

---

### Task 1: Project baseline and importable package

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/flakynet/__init__.py`
- Test: `tests/test_package.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `import flakynet` works; `uv run pytest` runs; `uv run ruff check .` runs.

- [ ] **Step 1: Write the smoke test**

`tests/test_package.py`:
```python
def test_package_imports():
    import flakynet

    assert flakynet.__name__ == "flakynet"
```

- [ ] **Step 2: Update `pyproject.toml`**

Replace the placeholder metadata:
```toml
[project]
name = "flakynet"
version = "0.1.0"
description = "Seeded, reproducible network faults for testing real clients over real sockets."
readme = "README.md"
requires-python = ">=3.10"
authors = [
    { name = "Siddhesh Agarwal" }
]
dependencies = []

[dependency-groups]
dev = [
    "pytest>=8",
    "hypothesis>=6",
    "ruff>=0.6",
    "ty",
]

[build-system]
requires = ["uv_build>=0.13.4,<0.14.0"]
build-backend = "uv_build"

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra"
markers = [
    "integration: tests that need a real service",
    "timing_sensitive: tests with wall-clock tolerances",
]

[tool.ruff]
target-version = "py310"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "RUF"]
```
Remove `[project.scripts]` entirely. Keep `.python-version` at `3.14` for local development; the floor is enforced by `requires-python` and `ruff target-version`.

- [ ] **Step 3: Remove the placeholder CLI entry point**

`src/flakynet/__init__.py`:
```python
"""Seeded, reproducible network faults for testing real clients over real sockets."""

__version__ = "0.1.0"
```

- [ ] **Step 4: Sync and verify**

Run:
```sh
uv sync
uv run pytest tests/test_package.py -v
uv run ruff check .
```
Expected: test PASS, ruff reports no errors.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml src/flakynet/__init__.py tests/test_package.py
git commit -m "chore: set up Python 3.10 package baseline"
```

---

### Task 2: Selectors, Plan, and frozen fault dataclasses

**Files:**
- Create: `src/flakynet/plan.py`
- Create: `src/flakynet/faults.py`
- Test: `tests/test_plan.py`
- Test: `tests/test_faults.py`

**Interfaces:**
- Produces in `plan.py`:
  - `Side = Literal["client", "server", "both"]`
  - `StreamSide = Literal["client", "server"]`
  - `ConnSelector = Nth | Every`
  - `ALL: Every`
  - `Between(low: int, high: int)`, `.draw(rng: random.Random) -> int`
  - `Nth(index: int)`, `.matches(conn_index: int) -> bool`
  - `Every()`, `.matches(conn_index: int) -> bool`
  - `Saw(pattern: bytes, side: StreamSide, delta: int | Between = 0)`
  - `between(low: int, high: int) -> Between`
  - `nth(index: int) -> Nth`
  - `every() -> Every`
  - `saw(pattern: bytes, *, side: StreamSide, delta: int | Between = 0) -> Saw`
  - `Plan(*faults: Fault)`, `.faults: tuple[Fault, ...]`, `.of(x: Plan | Fault | Iterable[Fault]) -> Plan`
- Produces in `faults.py`:
  - `Window = tuple[int | Between, int | Between | None] | None`
  - `Latency(ms: int, jitter_ms: int = 0, side: Side = "both", conn: ConnSelector = ALL, window: Window = None)`
  - `Bandwidth(bytes_per_second: int | Between, side: Side = "both", conn: ConnSelector = ALL, window: Window = None)`
  - `Fragment(max_bytes: int, side: Side = "both", conn: ConnSelector = ALL, window: Window = None)`
  - `Reset(after_bytes: int | Between | None = None, after: Saw | None = None, side: Side = "both", conn: ConnSelector = ALL)`
  - `CloseAfter(after_bytes: int | Between | None = None, after: Saw | None = None, side: Side = "both", conn: ConnSelector = ALL)`
  - `Blackhole(after_bytes: int | Between | None = None, after: Saw | None = None, side: Side = "both", conn: ConnSelector = ALL)`
  - `RefuseConnect(conn: ConnSelector = ALL)`
  - `Fault = Latency | Bandwidth | Fragment | Reset | CloseAfter | Blackhole | RefuseConnect`
  - `FAULT_TYPES = (Latency, Bandwidth, Fragment, Reset, CloseAfter, Blackhole, RefuseConnect)`

- [ ] **Step 1: Write the failing selector tests**

`tests/test_plan.py`:
```python
import random

import pytest

from flakynet.plan import ALL, between, every, nth, saw


def test_between_is_inclusive_and_uniform():
    rng = random.Random("test")
    draws = {between(2, 4).draw(rng) for _ in range(200)}
    assert draws == {2, 3, 4}


def test_between_rejects_negative_or_reversed_ranges():
    with pytest.raises(ValueError):
        between(-1, 3)
    with pytest.raises(ValueError):
        between(4, 2)


def test_nth_is_zero_based():
    assert nth(0).matches(0)
    assert not nth(0).matches(1)
    assert nth(2).matches(2)


def test_nth_rejects_negative_index():
    with pytest.raises(ValueError):
        nth(-1)


def test_every_matches_every_connection():
    assert every().matches(0)
    assert every().matches(99)
    assert ALL.matches(1)


def test_saw_requires_explicit_stream_side():
    with pytest.raises(TypeError):
        saw(b"x")  # type: ignore[call-arg]
    with pytest.raises(ValueError):
        saw(b"", side="client")
    with pytest.raises(ValueError):
        saw(b"x", side="both")  # type: ignore[arg-type]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_plan.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'flakynet.plan'`.

- [ ] **Step 3: Implement `plan.py`**

Use `@dataclass(frozen=True, slots=True)` for `Between`, `Nth`, `Every`, `Saw`, and `Plan`. Validate in `__post_init__`:
- `Between`: `0 <= low <= high`.
- `Nth`: `index >= 0`.
- `Saw`: `isinstance(pattern, bytes) and pattern`, `side in {"client", "server"}`, and `delta` is a non-negative int or a `Between` whose `low >= 0`.
- `Every`: no fields.

`Plan` needs a custom constructor because it takes varargs:
```python
@dataclass(frozen=True, slots=True, init=False)
class Plan:
    faults: tuple[Fault, ...]

    def __init__(self, *faults: Fault) -> None:
        from .faults import FAULT_TYPES

        if not all(isinstance(fault, FAULT_TYPES) for fault in faults):
            raise TypeError("Plan() expected Fault instances")
        object.__setattr__(self, "faults", tuple(faults))
```

`Plan.of(x)`:
- `Plan` -> return as-is.
- a fault instance -> `Plan(x)`.
- a non-string/bytes `Iterable` -> `Plan(*x)`.
- anything else -> `TypeError("Plan.of() expected Plan, Fault, or iterable of Fault")`.

Use a lazy import inside `Plan.__init__` and `Plan.of`:
```python
from .faults import FAULT_TYPES
```
This breaks the `plan.py` <-> `faults.py` cycle: `faults.py` imports selectors from `plan.py` at module load, while `plan.py` only imports `faults` inside methods. Define `Fault` under `TYPE_CHECKING` for annotations.

The dataclass default repr is acceptable. Do not test exact repr text beyond determinism.

- [ ] **Step 4: Write the failing fault tests**

`tests/test_faults.py`:
```python
import pytest

from flakynet.faults import (
    Bandwidth,
    Blackhole,
    CloseAfter,
    Fragment,
    Latency,
    RefuseConnect,
    Reset,
)
from flakynet.plan import ALL, Plan, between, nth, saw


def test_fault_defaults():
    assert Latency(ms=50, jitter_ms=20).side == "both"
    assert Latency(ms=50, jitter_ms=20).conn == ALL
    assert Latency(ms=50, jitter_ms=20).window is None
    assert Fragment(max_bytes=3).side == "both"
    assert RefuseConnect().conn == ALL


def test_latency_rejects_negative_values():
    with pytest.raises(ValueError):
        Latency(ms=-1)
    with pytest.raises(ValueError):
        Latency(ms=1, jitter_ms=-1)


def test_window_rejects_reversed_or_negative_bounds():
    with pytest.raises(ValueError):
        Fragment(max_bytes=3, window=(10, 5))
    with pytest.raises(ValueError):
        Fragment(max_bytes=3, window=(-1, 5))
    with pytest.raises(ValueError):
        Fragment(max_bytes=0)


def test_bandwidth_requires_positive_rate():
    with pytest.raises(ValueError):
        Bandwidth(bytes_per_second=0)
    with pytest.raises(ValueError):
        Bandwidth(bytes_per_second=between(0, 10))  # low must be > 0


def test_discrete_fault_requires_exactly_one_trigger():
    with pytest.raises(ValueError):
        Reset()
    with pytest.raises(ValueError):
        Reset(after_bytes=1, after=saw(b"x", side="client"))
    with pytest.raises(ValueError):
        CloseAfter()
    with pytest.raises(ValueError):
        Blackhole()


def test_discrete_fault_accepts_byte_or_saw_trigger():
    assert Reset(after_bytes=between(10, 20)).after_bytes is not None
    assert Blackhole(after=saw(b"COMMIT", side="client")).after is not None


def test_invalid_side_is_rejected():
    with pytest.raises(ValueError):
        Latency(ms=1, side="both_ways")  # type: ignore[arg-type]


def test_plan_normalizes_single_fault_and_iterable():
    fault = Reset(after_bytes=1)
    assert Plan.of(fault).faults == (fault,)
    assert Plan.of([fault]).faults == (fault,)
    assert Plan.of(Plan(fault)).faults == (fault,)
    assert Plan.of(()).faults == ()


def test_plan_rejects_non_fault_inputs():
    with pytest.raises(TypeError):
        Plan.of("not a fault")
    with pytest.raises(TypeError):
        Plan.of([object()])


def test_plan_preserves_connection_scope():
    fault = Reset(after_bytes=1, conn=nth(1))
    assert Plan.of(fault).faults[0].conn == nth(1)
```

- [ ] **Step 5: Run them to verify they fail**

Run: `uv run pytest tests/test_faults.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'flakynet.faults'`.

- [ ] **Step 6: Implement `faults.py`**

Use `from __future__ import annotations`. Define the dataclasses exactly as listed in **Interfaces**. Validate in `__post_init__`:
- `side in {"client", "server", "both"}`.
- `conn` is `Nth | Every`; use `isinstance(conn, (Nth, Every))`.
- `Latency`: `ms >= 0`, `jitter_ms >= 0`.
- `Bandwidth`: int rate `> 0`, or `Between` with `low > 0`.
- `Fragment`: `max_bytes > 0`.
- Window: if not `None`, it must be a 2-tuple; numeric start must be `>= 0`; numeric end must be `>= start`; if start or end is a `Between`, validate its range separately.
- Discrete: exactly one of `after_bytes` and `after` is not `None`; `after_bytes` must be an int `>= 0` or a `Between` with `low >= 0`; `after` must be a `Saw`.
- `RefuseConnect` has no `side`.

Define `Fault` and `FAULT_TYPES` at the bottom. `FAULT_TYPES` must be a tuple for `isinstance` checks.

- [ ] **Step 7: Run the plan and fault tests**

Run: `uv run pytest tests/test_plan.py tests/test_faults.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/flakynet/plan.py src/flakynet/faults.py tests/test_plan.py tests/test_faults.py
git commit -m "feat: add selectors, Plan, and frozen fault dataclasses"
```

---

### Task 3: Chunk-invariant `saw` matcher

**Files:**
- Create: `src/flakynet/matcher.py`
- Test: `tests/test_matcher.py`

**Interfaces:**
- Produces:
  - `Matcher(pattern: bytes)`
  - `Matcher.feed(chunk: bytes) -> int | None` — returns the absolute exclusive end offset of the first match, or `None`. After the first match, returns `None` forever.

- [ ] **Step 1: Write the failing matcher tests**

`tests/test_matcher.py`:
```python
import pytest
from hypothesis import given, strategies as st

from flakynet.matcher import Matcher


def test_match_in_single_chunk():
    m = Matcher(b"COMMIT")
    assert m.feed(b"BEGIN;COMMIT;") == 12


def test_match_split_across_chunks():
    m = Matcher(b"COMMIT")
    assert m.feed(b"BEG") is None
    assert m.feed(b"IN;COM") is None
    assert m.feed(b"MIT;") == 12


def test_match_at_stream_start_and_end():
    m = Matcher(b"abc")
    assert m.feed(b"abc") == 3
    m2 = Matcher(b"abc")
    assert m2.feed(b"xxabc") == 5


def test_fires_once():
    m = Matcher(b"abc")
    assert m.feed(b"abcabc") == 3
    assert m.feed(b"abc") is None


def test_empty_pattern_rejected():
    with pytest.raises(ValueError):
        Matcher(b"")


def _feed_all(pattern: bytes, chunks: list[bytes]) -> int | None:
    m = Matcher(pattern)
    for chunk in chunks:
        result = m.feed(chunk)
        if result is not None:
            return result
    return None


@given(
    pattern=st.binary(min_size=1, max_size=8),
    stream=st.binary(min_size=0, max_size=64),
    cuts=st.lists(st.integers(min_value=0, max_value=64), max_size=12),
)
def test_match_end_is_chunk_invariant(pattern, stream, cuts):
    boundaries = sorted({0, len(stream), *[min(c, len(stream)) for c in cuts]})
    chunks = [stream[a:b] for a, b in zip(boundaries, boundaries[1:])]
    assert _feed_all(pattern, chunks) == _feed_all(pattern, [stream])
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_matcher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'flakynet.matcher'`.

- [ ] **Step 3: Implement `Matcher`**

Keep `self._seen` (bytes fed before the current chunk), `self._tail` (at most `len(pattern) - 1` bytes), and `self._matched` (bool). In `feed`:
- If `_matched` or `chunk == b""`, return `None`.
- `base = self._seen - len(self._tail)`
- `data = self._tail + chunk`
- `index = data.find(self._pattern)`
- Update `_seen += len(chunk)`.
- `keep = len(self._pattern) - 1`; `self._tail = data[-keep:] if keep else b""`. The explicit guard is required because `data[-0:]` is the whole buffer.
- If `index >= 0`: set `_matched = True`, return `base + index + len(self._pattern)`.
- Else return `None`.

Use `from __future__ import annotations`. No module-level mutable state.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_matcher.py -v`
Expected: PASS, including the Hypothesis property.

- [ ] **Step 5: Commit**

```bash
git add src/flakynet/matcher.py tests/test_matcher.py
git commit -m "feat: add chunk-invariant saw matcher"
```

---

### Task 4: Deterministic phase-1 static resolution

**Files:**
- Create: `src/flakynet/resolve.py`
- Test: `tests/test_resolve.py`

**Interfaces:**
- Consumes: `Plan`, `Between`, `Saw`, `StreamSide`, `Nth`, `Every`, and all fault types.
- Produces:
  - `rng_key(seed: int | str, proxy_name: str, conn_index: int) -> str`
  - `LatencyParams(jitter_key: int)`
  - `BandwidthParams(bytes_per_second: int)`
  - `FragmentParams(max_bytes: int)`
  - `ResolvedParams = LatencyParams | BandwidthParams | FragmentParams | None`
  - `StaticFault(fault_index: int, fault: Fault, stream: StreamSide | None, start: int, end: int | None, params: ResolvedParams)`
  - `ConnectionPlan(seed: int | str, proxy_name: str, conn_index: int, rng_key: str, faults: tuple[StaticFault, ...])`
  - `resolve(plan: Plan, seed: int | str, proxy_name: str, conn_index: int) -> ConnectionPlan`

- [ ] **Step 1: Write the failing static-resolution tests**

`tests/test_resolve.py`:
```python
import pytest
from hypothesis import given, strategies as st

from flakynet.faults import Bandwidth, Fragment, Latency, RefuseConnect, Reset
from flakynet.plan import Plan, between, nth
from flakynet.resolve import (
    BandwidthParams,
    FragmentParams,
    LatencyParams,
    resolve,
    rng_key,
)


def test_rng_key_is_stable_and_includes_all_components():
    assert rng_key(7, "p0", 2) == "7:p0:2"
    assert rng_key("seed", "p1", 0) == "seed:p1:0"


def test_resolve_is_deterministic():
    plan = Plan(Reset(after_bytes=between(10, 20)))
    a = resolve(plan, seed=7, proxy_name="p0", conn_index=0)
    b = resolve(plan, seed=7, proxy_name="p0", conn_index=0)
    assert a == b


def test_different_connection_indexes_have_different_keys():
    plan = Plan(Reset(after_bytes=between(10, 20)))
    a = resolve(plan, seed=7, proxy_name="p0", conn_index=0)
    b = resolve(plan, seed=7, proxy_name="p0", conn_index=1)
    assert a.rng_key != b.rng_key


def test_negative_connection_index_rejected():
    with pytest.raises(ValueError):
        resolve(Plan(), 0, "p0", -1)


def test_conn_selector_skips_other_connections():
    plan = Plan(Reset(after_bytes=10, conn=nth(1)))
    assert resolve(plan, 0, "p0", 0).faults == ()
    assert len(resolve(plan, 0, "p0", 1).faults) == 2  # side="both" -> two streams


def test_side_both_expands_to_two_streams_with_same_fault_index():
    plan = Plan(Reset(after_bytes=10))
    result = resolve(plan, 0, "p0", 0)
    assert {f.stream for f in result.faults} == {"client", "server"}
    assert {f.fault_index for f in result.faults} == {0}


def test_window_defaults_to_whole_stream():
    plan = Plan(Latency(ms=10))
    result = resolve(plan, 0, "p0", 0)
    assert len(result.faults) == 2
    assert result.faults[0].params == result.faults[1].params
    for fault in result.faults:
        assert (fault.start, fault.end) == (0, None)
        assert isinstance(fault.params, LatencyParams)


def test_window_resolves_to_bounds():
    plan = Plan(Fragment(max_bytes=3, side="server", window=(10, 20)))
    result = resolve(plan, 0, "p0", 0)
    assert len(result.faults) == 1
    fault = result.faults[0]
    assert (fault.stream, fault.start, fault.end) == ("server", 10, 20)
    assert fault.params == FragmentParams(max_bytes=3)


def test_resolution_rejects_reversed_drawn_window():
    plan = Plan(Fragment(max_bytes=3, side="server", window=(between(10, 20), between(0, 5))))
    with pytest.raises(ValueError):
        resolve(plan, 0, "p0", 0)


def test_bandwidth_rate_is_drawn_at_resolution():
    plan = Plan(Bandwidth(bytes_per_second=between(100, 200), side="server"))
    result = resolve(plan, 0, "p0", 0)
    fault = result.faults[0]
    assert isinstance(fault.params, BandwidthParams)
    assert 100 <= fault.params.bytes_per_second <= 200


def test_refuse_connect_is_connection_level():
    result = resolve(Plan(RefuseConnect()), 0, "p0", 0)
    assert len(result.faults) == 1
    fault = result.faults[0]
    assert fault.stream is None
    assert (fault.start, fault.end) == (0, 0)


def test_fault_index_refers_to_original_plan():
    plan = Plan(Reset(after_bytes=1, conn=nth(1)), Reset(after_bytes=2))
    result = resolve(plan, 0, "p0", 0)
    assert [f.fault_index for f in result.faults] == [1, 1]


@given(
    seed=st.integers(min_value=0, max_value=10_000),
    proxy_name=st.text(alphabet="p0123456789", min_size=1, max_size=4),
    conn_index=st.integers(min_value=0, max_value=20),
    lo=st.integers(min_value=0, max_value=1000),
    hi=st.integers(min_value=0, max_value=1000),
)
def test_resolution_is_reproducible_for_arbitrary_inputs(seed, proxy_name, conn_index, lo, hi):
    lo, hi = min(lo, hi), max(lo, hi)
    plan = Plan(Reset(after_bytes=between(lo, hi)))
    assert resolve(plan, seed, proxy_name, conn_index) == resolve(
        plan, seed, proxy_name, conn_index
    )
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_resolve.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'flakynet.resolve'`.

- [ ] **Step 3: Implement `resolve.py`**

Use `from __future__ import annotations`. Implement:

```python
def rng_key(seed: int | str, proxy_name: str, conn_index: int) -> str:
    return f"{seed}:{proxy_name}:{conn_index}"
```

`resolve`:
1. Reject `conn_index < 0` with `ValueError`.
2. `rng = random.Random(rng_key(seed, proxy_name, conn_index))`.
3. Iterate `enumerate(plan)`.
4. Skip if `not fault.conn.matches(conn_index)`.
5. `RefuseConnect` -> one `StaticFault(fault_index, fault, stream=None, start=0, end=0, params=None)`.
6. For `Latency`, draw `jitter_key = rng.getrandbits(64)` once per fault, before expanding sides, so both streams share the same key and the stream remains part of the runtime delay function.
7. For other faults, determine sides:
   - `("client", "server")` if `fault.side == "both"` else `(fault.side,)`.
8. For each side, resolve:
   - Window faults (`Latency`, `Bandwidth`, `Fragment`): `start = _draw_offset(window[0]) if window else 0`; `end = _draw_offset(window[1]) if window and window[1] is not None else None`; raise `ValueError` if `end is not None and end < start`; draw params:
     - `Latency`: `LatencyParams(jitter_key=jitter_key)`.
     - `Bandwidth`: `BandwidthParams(_draw_positive(fault.bytes_per_second, rng))`.
     - `Fragment`: `FragmentParams(fault.max_bytes)`.
   - Discrete faults (`Reset`, `CloseAfter`, `Blackhole`): for this task, raise `NotImplementedError` if `fault.after is not None`; otherwise `start = _draw_offset(fault.after_bytes, rng)`; `end = start`; `params=None`.
9. Return `ConnectionPlan(seed, proxy_name, conn_index, key, tuple(resolved))`.

Helpers:
```python
def _draw_offset(value: int | Between, rng: random.Random) -> int:
    return value.draw(rng) if isinstance(value, Between) else value

def _draw_positive(value: int | Between, rng: random.Random) -> int:
    result = _draw_offset(value, rng)
    if result <= 0:
        raise ValueError("resolved value must be positive")
    return result
```

Do not draw RNG for skipped faults. Draw in plan order and side order `("client", "server")` so the stream is deterministic. Use `assert fault.after_bytes is not None` before drawing a static discrete trigger to satisfy type checkers.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_resolve.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/flakynet/resolve.py tests/test_resolve.py
git commit -m "feat: resolve static faults from seed and connection index"
```

---

### Task 5: Armed faults for `saw` triggers

**Files:**
- Modify: `src/flakynet/resolve.py`
- Test: `tests/test_resolve.py`

**Interfaces:**
- Consumes: `Saw` from `plan.py` and discrete faults from `faults.py`.
- Produces:
  - `ArmedFault(fault_index: int, fault: Fault, stream: StreamSide, trigger: Saw, delta: int, start: int | None = None, end: int | None = None, params: ResolvedParams = None)`
  - `ConnectionPlan.faults: tuple[StaticFault | ArmedFault, ...]`
  - `resolve()` now emits `ArmedFault` for discrete faults with `after=Saw(...)`.

- [ ] **Step 1: Write the failing armed-fault tests**

Append to `tests/test_resolve.py`:
```python
from flakynet.faults import Blackhole, CloseAfter
from flakynet.plan import saw
from flakynet.resolve import ArmedFault


def test_armed_same_stream_fault_has_no_start_until_match():
    plan = Plan(Blackhole(after=saw(b"X", side="server"), side="server"))
    result = resolve(plan, 0, "p0", 0)
    fault = result.faults[0]
    assert isinstance(fault, ArmedFault)
    assert fault.stream == "server"
    assert fault.trigger.side == "server"
    assert fault.start is None
    assert fault.delta == 0


def test_armed_cross_stream_fault_is_recorded_on_target_stream():
    plan = Plan(Blackhole(after=saw(b"COMMIT", side="client"), side="server"))
    result = resolve(plan, 0, "p0", 0)
    fault = result.faults[0]
    assert isinstance(fault, ArmedFault)
    assert fault.stream == "server"
    assert fault.trigger.side == "client"


def test_armed_delta_can_be_drawn():
    plan = Plan(CloseAfter(after=saw(b"X", side="server", delta=between(1, 5))))
    result = resolve(plan, 0, "p0", 0)
    assert isinstance(result.faults[0], ArmedFault)
    assert 1 <= result.faults[0].delta <= 5


def test_armed_side_both_expands_to_two_streams():
    plan = Plan(Blackhole(after=saw(b"X", side="server")))
    result = resolve(plan, 0, "p0", 0)
    assert {f.stream for f in result.faults} == {"client", "server"}
    assert all(isinstance(f, ArmedFault) for f in result.faults)


def test_armed_resolution_is_deterministic():
    plan = Plan(Blackhole(after=saw(b"X", side="client", delta=between(1, 5))))
    a = resolve(plan, 7, "p0", 0)
    b = resolve(plan, 7, "p0", 0)
    assert a == b
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/test_resolve.py -v`
Expected: FAIL because `ArmedFault` is not defined / `Blackhole(after=...)` is rejected.

- [ ] **Step 3: Implement armed resolution**

Add `ArmedFault` as a frozen dataclass. In the discrete branch:
```python
if fault.after is not None:
    delta = _draw_offset(fault.after.delta, rng)
    resolved.append(
        ArmedFault(
            fault_index=fault_index,
            fault=fault,
            stream=side,
            trigger=fault.after,
            delta=delta,
        )
    )
else:
    assert fault.after_bytes is not None
    start = _draw_offset(fault.after_bytes, rng)
    resolved.append(
        StaticFault(
            fault_index=fault_index,
            fault=fault,
            stream=side,
            start=start,
            end=start,
            params=None,
        )
    )
```
`ArmedFault.start` and `.end` stay `None`; the pump anchors them at runtime. Do not draw RNG for the matcher itself; the `delta` draw is the only armed-fault draw.

Update `ConnectionPlan.faults` type to `tuple[StaticFault | ArmedFault, ...]`.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/test_resolve.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/flakynet/resolve.py tests/test_resolve.py
git commit -m "feat: resolve armed saw-triggered faults"
```

---

### Task 6: Public exports and full-slice verification

**Files:**
- Modify: `src/flakynet/__init__.py`
- Test: `tests/test_package.py`
- Modify: `pyproject.toml` only if `ty` needs a `[tool.ty]` section.

**Interfaces:**
- Produces: the public surface for the pure slice.

- [ ] **Step 1: Write the failing export test**

Append to `tests/test_package.py`:
```python
def test_public_exports():
    import flakynet as fn

    assert fn.Plan
    assert fn.Latency
    assert fn.Bandwidth
    assert fn.Fragment
    assert fn.Reset
    assert fn.CloseAfter
    assert fn.Blackhole
    assert fn.RefuseConnect
    assert fn.between
    assert fn.nth
    assert fn.every
    assert fn.saw
    assert fn.ALL
    assert fn.resolve
    assert fn.Matcher
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_package.py::test_public_exports -v`
Expected: FAIL with `AttributeError`.

- [ ] **Step 3: Update `__init__.py`**

Export the public names:
```python
from .faults import (
    Bandwidth,
    Blackhole,
    CloseAfter,
    Fragment,
    Latency,
    RefuseConnect,
    Reset,
)
from .matcher import Matcher
from .plan import ALL, Plan, between, every, nth, saw
from .resolve import ArmedFault, ConnectionPlan, StaticFault, resolve
```

Define `__all__`. Keep `__version__ = "0.1.0"`.

- [ ] **Step 4: Run the full verification**

Run:
```sh
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run ty check
```
Expected: all tests pass; ruff reports no issues; `ty` reports no errors. If `ty` is unavailable or fails for tooling reasons unrelated to the code, record the exact command and error in the handoff rather than claiming it passed.

- [ ] **Step 5: Commit**

```bash
git add src/flakynet/__init__.py tests/test_package.py pyproject.toml
git commit -m "feat: export the pure resolution API"
```

---

## Self-Review

**Spec coverage:**
- Phase-1 pure resolution: Tasks 4–5.
- `StaticFault`/`ArmedFault`: Tasks 4–5.
- RNG keying `(seed, proxy_name, conn_index)`: Task 4.
- 0-based connection index and `nth`: Tasks 2, 4.
- `side` default `"both"` and explicit `saw` side: Task 2.
- `window=(start, end)` / whole-stream default: Tasks 2, 4.
- `between` inclusive uniform: Task 2.
- `saw` chunk invariance: Task 3.
- `Plan` normalization: Task 2.
- Fault dataclasses and validation: Task 2.
- Public exports: Task 6.
- Not in this slice: proxy, sockets, events, fault report, pytest plugin, `assert_all_fired`, async wrapper, `RefuseConnect` runtime behavior, `Blackhole` runtime behavior. These are explicitly deferred to the next slice.

**Step scan:** Each test step names the test and its assertions. Each implementation step gives the exact signature and file. No "handle edge cases" or "write tests for the above."

**Type consistency:** `Side`, `StreamSide`, `ConnSelector`, `Between`, `Saw`, `Window`, `Fault`, `StaticFault`, `ArmedFault`, `ResolvedParams`, `ConnectionPlan`, and `resolve` are used consistently across tasks. `Plan.of` uses a lazy import to avoid the `plan.py`/`faults.py` cycle.

**Review Focus coverage:** The five review-focus items each have a test:
- Non-fault `Plan.of` input: Task 2.
- Invalid ranges: Tasks 2, 4.
- `saw` chunk invariance: Task 3.
- Connection selectivity and `fault_index`: Task 4.
- `side="both"` expansion: Tasks 4–5.

**Proportion:** The plan is longer than the eventual code but stays at signatures and test assertions; implementation bodies are given only where the algorithm is non-obvious (`Matcher.feed`, `resolve`).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-10-26-flakynet-pure-resolution.md`. Please review the plan. Which execution approach would you prefer?

- **Subagent-driven** — a fresh subagent implements each task and a fresh reviewer checks it before the next one starts, then a whole-branch review at the end. Most thorough; costs a fresh context per task and per review.
- **Native** — I implement every task myself in this session, then one fresh reviewer on the most capable model checks the whole branch. Cheapest and fastest; no independent review until the end.

For this plan I recommend **Native**, because the tasks are tightly coupled around a small set of shared types and the total surface is small enough for one context to hold. Does the plan capture what you want, and which approach should we use?
