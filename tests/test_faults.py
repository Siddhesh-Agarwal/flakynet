from typing import Any, cast

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


def test_latency_rejects_non_integer_values():
    with pytest.raises(TypeError):
        Latency(ms=cast("Any", 0.5))
    with pytest.raises(TypeError):
        Latency(ms=cast("Any", float("nan")))
    with pytest.raises(TypeError):
        Latency(ms=1, jitter_ms=cast("Any", 0.5))


def test_numeric_fault_fields_require_integers():
    with pytest.raises(TypeError):
        Bandwidth(bytes_per_second=cast("Any", 0.5))
    with pytest.raises(TypeError):
        Fragment(max_bytes=cast("Any", 3.0))
    with pytest.raises(TypeError):
        Fragment(max_bytes=True)
    with pytest.raises(TypeError):
        Fragment(max_bytes=3, window=cast("Any", (0.5, 5)))
    with pytest.raises(TypeError):
        Fragment(max_bytes=3, window=cast("Any", (0, 5.0)))
    with pytest.raises(TypeError):
        Reset(after_bytes=cast("Any", 0.5))
    with pytest.raises(ValueError):
        Reset(after_bytes=-1)


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
        Latency(ms=1, side=cast("Any", "both_ways"))


def test_invalid_conn_selector_is_rejected():
    with pytest.raises(TypeError):
        Latency(ms=1, conn=cast("Any", "every"))
    with pytest.raises(TypeError):
        RefuseConnect(conn=cast("Any", object()))


def test_plan_normalizes_single_fault_and_iterable():
    fault = Reset(after_bytes=1)
    assert Plan.of(fault).faults == (fault,)
    assert Plan.of([fault]).faults == (fault,)
    assert Plan.of(Plan(fault)).faults == (fault,)
    assert Plan.of(()).faults == ()


def test_plan_rejects_non_fault_inputs():
    with pytest.raises(TypeError):
        Plan.of(cast("Any", "not a fault"))
    with pytest.raises(TypeError):
        Plan.of(cast("Any", b"not a fault"))
    with pytest.raises(TypeError):
        Plan.of(cast("Any", [object()]))


def test_plan_preserves_connection_scope():
    fault = Reset(after_bytes=1, conn=nth(1))
    assert Plan.of(fault).faults[0].conn == nth(1)
