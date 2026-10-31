"""Tests that document the limits of seed-based determinism.

Each test either demonstrates a scheduling-dependent outcome or proves that a
suspected source cannot affect fault offsets.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import socket
import threading
import time
from typing import TYPE_CHECKING

import pytest

from flakynet import CloseAfter, Latency, Plan, Proxy, Reset, saw
from flakynet.report import ConnectionSnapshot, build_report

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from flakynet.events import CanonicalEvent


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition was not met before the deadline")


def wait_for_event(
    proxy: Proxy,
    *,
    kind: str = "fired",
    stream: str | None = None,
) -> None:
    wait_until(
        lambda: any(
            event.kind == kind and (stream is None or event.stream == stream)
            for event in proxy.canonical_events()
        ),
    )


@contextlib.contextmanager
def scripted_server(
    *,
    banner: bytes = b"",
    wait_for_client_byte: bool = False,
    close_after_banner: bool = False,
) -> Generator[tuple[tuple[str, int], threading.Event, threading.Event], None, None]:
    """A server whose banner is released on demand.

    Yields the address, an event that releases the banner, and an event that
    is set once the banner was written.
    """
    listener = socket.create_server(("127.0.0.1", 0))
    send_now = threading.Event()
    banner_sent = threading.Event()
    done = threading.Event()

    def run() -> None:
        try:
            conn, _ = listener.accept()
            with conn:
                if wait_for_client_byte:
                    conn.recv(1)
                else:
                    send_now.wait(timeout=2.0)
                if banner:
                    conn.sendall(banner)
                    banner_sent.set()
                if close_after_banner:
                    conn.shutdown(socket.SHUT_WR)
                while conn.recv(65536):
                    pass
        except OSError:
            pass
        finally:
            listener.close()
            done.set()

    thread = threading.Thread(target=run, daemon=True, name="scripted-server")
    thread.start()
    host, port = listener.getsockname()[:2]
    try:
        yield (str(host), int(port)), send_now, banner_sent
    finally:
        listener.close()
        thread.join(timeout=2.0)


def run_round_trip(
    target: tuple[str, int],
    plan: Plan,
    payload: bytes,
    *,
    chunks: list[bytes] | None = None,
) -> tuple[CanonicalEvent, ...]:
    with Proxy(target, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.settimeout(2.0)
            if chunks is None:
                sock.sendall(payload)
            else:
                for chunk in chunks:
                    sock.sendall(chunk)
            received = bytearray()
            while len(received) < len(payload):
                data = sock.recv(len(payload) - len(received))
                if not data:
                    break
                received.extend(data)
        wait_until(lambda: proxy.active_connections == 0)
        return proxy.canonical_events()


@pytest.mark.timing_sensitive
def test_terminal_fault_winner_depends_on_arrival_order() -> None:
    """A both-stream terminal has no fixed winning stream."""
    plan = Plan(Reset(after_bytes=1, side="both"))

    with (
        scripted_server(banner=b"s" * 1000) as (target, send_now, _),
        Proxy(target, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(2.0)
        with contextlib.suppress(OSError):
            sock.sendall(b"x" * 100)
        wait_for_event(proxy, stream="client")
        send_now.set()
        with contextlib.suppress(OSError):
            sock.recv(65536)
    wait_until(lambda: proxy.active_connections == 0)
    client_first = {event.stream for event in proxy.canonical_events() if event.kind == "fired"}

    with (
        scripted_server(banner=b"s" * 1000) as (target, send_now, _),
        Proxy(target, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(2.0)
        send_now.set()
        wait_for_event(proxy, stream="server")
        with contextlib.suppress(OSError):
            sock.sendall(b"x" * 100)
        with contextlib.suppress(OSError):
            sock.recv(65536)
    wait_until(lambda: proxy.active_connections == 0)
    server_first = {event.stream for event in proxy.canonical_events() if event.kind == "fired"}

    assert client_first == {"client"}
    assert server_first == {"server"}


@pytest.mark.timing_sensitive
def test_terminal_fault_can_cancel_sibling_window_events() -> None:
    """Sibling cancellation can remove window events from the log."""
    plan = Plan(
        Reset(after_bytes=1, side="client"),
        Latency(ms=0, side="server", window=(0, 1000)),
    )

    with (
        scripted_server(banner=b"s" * 1000) as (target, send_now, _),
        Proxy(target, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(2.0)
        with contextlib.suppress(OSError):
            sock.sendall(b"x" * 100)
        wait_for_event(proxy, stream="client")
        send_now.set()
        with contextlib.suppress(OSError):
            sock.recv(65536)
    wait_until(lambda: proxy.active_connections == 0)
    client_first = [
        event
        for event in proxy.canonical_events()
        if event.stream == "server" and event.kind in {"window_entered", "window_exited"}
    ]

    with (
        scripted_server(banner=b"s" * 1000) as (target, send_now, _),
        Proxy(target, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(2.0)
        send_now.set()
        wait_for_event(proxy, kind="window_exited", stream="server")
        with contextlib.suppress(OSError):
            sock.sendall(b"x" * 100)
        with contextlib.suppress(OSError):
            sock.recv(65536)
    wait_until(lambda: proxy.active_connections == 0)
    server_first = [
        event
        for event in proxy.canonical_events()
        if event.stream == "server" and event.kind in {"window_entered", "window_exited"}
    ]

    assert client_first == []
    assert [event.kind for event in server_first] == [
        "window_entered",
        "window_exited",
    ]


@pytest.mark.timing_sensitive
def test_cross_stream_armed_fault_can_miss_if_target_closes_first() -> None:
    """A cross-stream fault needs the target stream to still be alive."""
    plan = Plan(CloseAfter(after=saw(b"X", side="client"), side="server"))

    with (
        scripted_server(
            banner=b"s" * 100,
            wait_for_client_byte=True,
            close_after_banner=True,
        ) as (target, _, _),
        Proxy(target, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(2.0)
        with contextlib.suppress(OSError):
            sock.sendall(b"X")
        wait_for_event(proxy)
        with contextlib.suppress(OSError):
            while sock.recv(65536):
                pass
    wait_until(lambda: proxy.active_connections == 0)
    trigger_first = [event for event in proxy.canonical_events() if event.kind == "fired"]

    with (
        scripted_server(
            banner=b"s" * 100,
            close_after_banner=True,
        ) as (target, send_now, banner_sent),
        Proxy(target, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(2.0)
        send_now.set()
        assert banner_sent.wait(timeout=2.0)
        with contextlib.suppress(OSError):
            while sock.recv(65536):
                pass
        with contextlib.suppress(OSError):
            sock.sendall(b"X")
    wait_until(lambda: proxy.active_connections == 0)
    target_closed = [event for event in proxy.canonical_events() if event.kind == "fired"]

    assert len(trigger_first) == 1
    assert trigger_first[0].offset is None
    assert target_closed == []


def run_ordered(
    target: tuple[str, int],
    payloads: list[bytes],
) -> tuple[CanonicalEvent, ...]:
    plan = Plan(CloseAfter(after_bytes=10, side="server"))
    with Proxy(target, plan, seed=0, name="p0") as proxy:
        for payload in payloads:
            with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
                sock.settimeout(2.0)
                with contextlib.suppress(OSError):
                    sock.sendall(payload)
                with contextlib.suppress(OSError):
                    while sock.recv(65536):
                        pass
            wait_until(lambda: proxy.active_connections == 0)
        return proxy.canonical_events()


def test_connection_index_follows_accept_order_not_client_identity(
    echo_server: tuple[str, int],
) -> None:
    """The same seed maps plans by accept order, not by client identity."""
    long_payload = b"x" * 100
    short_payload = b"x" * 5

    long_first = run_ordered(echo_server, [long_payload, short_payload])
    short_first = run_ordered(echo_server, [short_payload, long_payload])

    long_first_fired = [event for event in long_first if event.kind == "fired"]
    short_first_fired = [event for event in short_first if event.kind == "fired"]
    assert [event.conn_index for event in long_first_fired] == [0]
    assert [event.conn_index for event in short_first_fired] == [1]


def test_global_random_state_does_not_change_canonical_events(
    echo_server: tuple[str, int],
) -> None:
    """Runtime code never draws from the process-wide RNG."""
    plan = Plan(Latency(ms=0, side="server", window=(0, 4)))
    payload = b"abcd"

    random.seed(1)
    first = run_round_trip(echo_server, plan, payload)
    random.seed(2)
    second = run_round_trip(echo_server, plan, payload)

    assert first == second


def test_actual_sleep_duration_does_not_change_events(
    echo_server: tuple[str, int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Measured sleep time is never read back into a fault value."""
    plan = Plan(Latency(ms=50, side="server", window=(0, 4)))
    payload = b"abcd"
    normal = run_round_trip(echo_server, plan, payload)

    async def fake_sleep(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    fast = run_round_trip(echo_server, plan, payload)

    assert normal == fast


def test_static_window_events_are_chunk_invariant(
    echo_server: tuple[str, int],
) -> None:
    """OS read sizes cannot move a static window boundary."""
    plan = Plan(Latency(ms=0, side="server", window=(0, 10)))
    payload = b"0123456789"

    unsplit = run_round_trip(echo_server, plan, payload)
    split = run_round_trip(
        echo_server,
        plan,
        payload,
        chunks=[bytes([byte]) for byte in payload],
    )

    assert unsplit == split
    assert [event.kind for event in unsplit] == [
        "window_entered",
        "window_exited",
    ]


def test_report_order_does_not_depend_on_input_order() -> None:
    """The report sorts snapshots; dict insertion order cannot leak through."""
    plan = Plan(CloseAfter(after_bytes=1, side="server"))
    later = ConnectionSnapshot(
        conn_index=1,
        bytes_client_to_server=0,
        bytes_server_to_client=0,
        client_end="graceful",
        server_end="graceful",
        end_reason="graceful",
        terminal_fired=False,
    )
    earlier = ConnectionSnapshot(
        conn_index=0,
        bytes_client_to_server=0,
        bytes_server_to_client=0,
        client_end="graceful",
        server_end="graceful",
        end_reason="graceful",
        terminal_fired=False,
    )

    report = build_report(plan, [], [later, earlier])

    assert [entry.conn_index for entry in report] == [0, 1]
