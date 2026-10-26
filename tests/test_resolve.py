from typing import Any, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from flakynet.faults import (
    Bandwidth,
    Blackhole,
    CloseAfter,
    Fragment,
    Latency,
    RefuseConnect,
    Reset,
)
from flakynet.plan import Plan, between, nth, saw
from flakynet.resolve import (
    ArmedFault,
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


def test_non_integer_connection_index_rejected():
    with pytest.raises(TypeError):
        resolve(Plan(), 0, "p0", cast(Any, 0.5))


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
