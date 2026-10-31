from __future__ import annotations

import socket
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from flakynet import Latency, Plan, Proxy
from flakynet.resolve import LatencyParams

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from flakynet.events import CanonicalEvent


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the deadline")


def read_exact(sock: socket.socket, count: int) -> bytes:
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise AssertionError(f"EOF after {len(data)} of {count} bytes")
        data.extend(chunk)
    return bytes(data)


def round_trip(proxy: Proxy, payload: bytes) -> bytes:
    with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
        sock.sendall(payload)
        return read_exact(sock, len(payload))


def per_connection(events: Iterable[CanonicalEvent]) -> set[tuple[object, ...]]:
    groups: dict[int, list[tuple[object, ...]]] = {}
    for event in events:
        groups.setdefault(event.conn_index, []).append(
            (
                event.stream,
                event.offset,
                event.fault_index,
                event.fault,
                event.kind,
                event.params,
            ),
        )
    return {tuple(group) for group in groups.values()}


def test_event_fields_and_canonical_stripping(echo_server: tuple[str, int]) -> None:
    plan = Plan(Latency(ms=0, side="server", window=(0, 4)))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"abcd")
            assert read_exact(sock, 4) == b"abcd"
        wait_until(lambda: len(proxy.canonical_events()) >= 2)
        raw = proxy.events
        canonical = proxy.canonical_events()

    first = raw[0]
    assert first.conn_index == 0
    assert first.stream == "server"
    assert first.offset == 0
    assert first.offset_exact is True
    assert first.fault_index == 0
    assert first.fault == "Latency"
    assert first.kind == "window_entered"
    assert isinstance(first.params, LatencyParams)
    assert first.params.ms == 0
    assert first.seq == 0
    assert first.timestamp > 0
    assert [event.seq for event in raw] == list(range(len(raw)))

    assert all(not hasattr(event, "seq") for event in canonical)
    assert all(not hasattr(event, "timestamp") for event in canonical)
    assert [event.kind for event in canonical] == ["window_entered", "window_exited"]
    assert all(event.fault == "Latency" for event in canonical)
    assert all(isinstance(event.params, LatencyParams) for event in canonical)


def run_sequential(
    target: tuple[str, int],
) -> tuple[bytes, tuple[CanonicalEvent, ...]]:
    plan = Plan(Latency(ms=0, side="server", window=(0, 10)))
    with Proxy(target, plan, seed=7, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"0123456789")
            received = read_exact(sock, 10)
        wait_until(lambda: proxy.active_connections == 0)
        return received, proxy.canonical_events()


def test_canonical_events_identical_across_sequential_runs(
    echo_server: tuple[str, int],
) -> None:
    first = run_sequential(echo_server)
    second = run_sequential(echo_server)
    assert first[0] == second[0] == b"0123456789"
    assert first[1] == second[1]
    assert [event.kind for event in first[1]] == ["window_entered", "window_exited"]


def run_concurrent(
    target: tuple[str, int],
    count: int = 8,
) -> tuple[list[bytes], set[tuple[object, ...]]]:
    payload = b"ping"
    plan = Plan(Latency(ms=0, side="server", window=(0, len(payload))))
    with Proxy(target, plan, seed=3, name="p0") as proxy:
        with ThreadPoolExecutor(max_workers=count) as pool:
            results = list(pool.map(lambda _: round_trip(proxy, payload), range(count)))
        wait_until(lambda: proxy.active_connections == 0)
        return results, per_connection(proxy.canonical_events())


def test_concurrent_connections_compare_per_connection_sequences(
    echo_server: tuple[str, int],
) -> None:
    payload = b"ping"
    first = run_concurrent(echo_server)
    second = run_concurrent(echo_server)
    assert first[0] == second[0] == [payload] * 8
    # Accept order, and therefore conn_index, may vary under concurrency, so
    # compare the multiset of per-connection sequences rather than global order.
    assert first[1] == second[1]
