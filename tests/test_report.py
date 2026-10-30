from __future__ import annotations

import contextlib
import socket
import struct
import sys
import threading
import time
from collections.abc import Callable, Iterator

import pytest

from flakynet import (
    ENDED_EARLY,
    FIRED,
    MISSED,
    PENDING,
    PREEMPTED,
    UNMATCHED,
    CloseAfter,
    Latency,
    Plan,
    Proxy,
    Reset,
    nth,
    saw,
)


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the deadline")


def round_trip(proxy: Proxy, payload: bytes) -> bytes:
    with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
        sock.sendall(payload)
        received = bytearray()
        while len(received) < len(payload):
            chunk = sock.recv(65536)
            if not chunk:
                break
            received.extend(chunk)
        return bytes(received)


def recv_until_eof(sock: socket.socket) -> bytes:
    chunks: list[bytes] = []
    while True:
        try:
            chunk = sock.recv(65536)
        except OSError:
            break
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def statuses_for(report, fault_index: int) -> list[str]:
    return [entry.status for entry in report if entry.fault_index == fault_index]


@contextlib.contextmanager
def reset_server() -> Iterator[tuple[str, int]]:
    listener = socket.create_server(("127.0.0.1", 0))

    def run() -> None:
        try:
            conn, _ = listener.accept()
            conn.settimeout(5.0)
            conn.recv(1)
            conn.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_LINGER,
                struct.pack("ii", 1, 0),
            )
            conn.close()
        except OSError:
            pass
        finally:
            listener.close()

    thread = threading.Thread(target=run, daemon=True, name="reset-server")
    thread.start()
    host, port = listener.getsockname()[:2]
    try:
        yield str(host), int(port)
    finally:
        listener.close()
        thread.join(timeout=2.0)


def test_fired_status(echo_server: tuple[str, int]) -> None:
    plan = Plan(CloseAfter(after_bytes=10, side="server"))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"x" * 100)
            with contextlib.suppress(OSError):
                sock.recv(65536)
        wait_until(lambda: proxy.active_connections == 0)
        report = proxy.fault_report()

    assert [(entry.fault_index, entry.conn_index, entry.status) for entry in report] == [
        (0, 0, FIRED)
    ]
    assert report[0].ended == "flakynet"
    assert report[0].bytes_server_to_client == 10


def test_window_entered_counts_as_fired(echo_server: tuple[str, int]) -> None:
    plan = Plan(Latency(ms=0, side="server", window=(0, 4)))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        assert round_trip(proxy, b"abcd") == b"abcd"
        wait_until(lambda: proxy.active_connections == 0)
        report = proxy.fault_report()

    assert statuses_for(report, 0) == [FIRED]


def test_missed_when_pattern_absent_and_stream_ends_gracefully(
    echo_server: tuple[str, int],
) -> None:
    plan = Plan(CloseAfter(after=saw(b"NEVER", side="server"), side="server"))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"hello")
            sock.shutdown(socket.SHUT_WR)
            assert recv_until_eof(sock) == b"hello"
        wait_until(lambda: proxy.active_connections == 0)
        report = proxy.fault_report()

    assert statuses_for(report, 0) == [MISSED]
    assert report[0].ended == "graceful"


def test_ended_early_when_client_aborts_first(echo_server: tuple[str, int]) -> None:
    plan = Plan(CloseAfter(after=saw(b"NEVER", side="client"), side="client"))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        sock = socket.create_connection((proxy.host, proxy.port), timeout=5.0)
        wait_until(lambda: proxy.active_connections == 1)
        sock.sendall(b"partial")
        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )
        sock.close()
        wait_until(lambda: proxy.active_connections == 0)
        report = proxy.fault_report()

    if sys.platform.startswith("linux"):
        assert statuses_for(report, 0) == [ENDED_EARLY]
        assert report[0].ended == "client_abort"
    else:
        assert report[0].status in {ENDED_EARLY, MISSED}


def test_preempted_by_earlier_reset(echo_server: tuple[str, int]) -> None:
    plan = Plan(
        Reset(after_bytes=1, side="server"),
        CloseAfter(after_bytes=1000, side="server"),
    )
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            with contextlib.suppress(OSError):
                sock.sendall(b"x" * 100)
            with contextlib.suppress(OSError):
                sock.recv(65536)
        wait_until(lambda: proxy.active_connections == 0)
        report = proxy.fault_report()

    assert statuses_for(report, 0) == [FIRED]
    assert statuses_for(report, 1) == [PREEMPTED]
    assert report[1].ended == "flakynet"


def test_server_abort_is_recorded() -> None:
    with reset_server() as target:
        plan = Plan(CloseAfter(after=saw(b"NEVER", side="server"), side="server"))
        with Proxy(target, plan, seed=0, name="p0") as proxy:
            with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
                with contextlib.suppress(OSError):
                    sock.sendall(b"hello")
                with contextlib.suppress(OSError):
                    sock.recv(65536)
            wait_until(lambda: proxy.active_connections == 0)
            report = proxy.fault_report()

    if sys.platform.startswith("linux"):
        assert report[0].status == ENDED_EARLY
        assert report[0].ended == "server_abort"
    else:
        assert report[0].status in {ENDED_EARLY, MISSED}


def test_upstream_connect_error_is_recorded() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        target = probe.getsockname()

    plan = Plan(CloseAfter(after_bytes=1, side="server"))
    with Proxy((str(target[0]), int(target[1])), plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.settimeout(2.0)
            assert sock.recv(1) == b""
        wait_until(lambda: proxy.active_connections == 0)
        report = proxy.fault_report()

    assert report[0].status == ENDED_EARLY
    assert report[0].ended == "error"


def test_pending_while_connection_is_open(echo_server: tuple[str, int]) -> None:
    plan = Plan(CloseAfter(after=saw(b"NEVER", side="server"), side="server"))
    with (
        Proxy(echo_server, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        wait_until(lambda: proxy.active_connections == 1)
        sock.sendall(b"open")
        report = proxy.fault_report()
        assert statuses_for(report, 0) == [PENDING]
        assert report[0].ended == "pending"


def test_unmatched_when_no_connection_matches_selector(
    echo_server: tuple[str, int],
) -> None:
    plan = Plan(CloseAfter(after_bytes=1, side="server", conn=nth(2)))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        assert round_trip(proxy, b"a") == b"a"
        wait_until(lambda: proxy.active_connections == 0)
        report = proxy.fault_report()

    assert len(report) == 1
    assert report[0].conn_index is None
    assert report[0].status == UNMATCHED
    assert report[0].ended == "not accepted"


def test_assert_all_fired_passes_for_fired_fault(echo_server: tuple[str, int]) -> None:
    plan = Plan(CloseAfter(after_bytes=5, side="server"))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"x" * 50)
            with contextlib.suppress(OSError):
                sock.recv(65536)
        wait_until(lambda: proxy.active_connections == 0)
        proxy.assert_all_fired()


def test_assert_all_fired_passes_when_fired_on_any_connection(
    echo_server: tuple[str, int],
) -> None:
    plan = Plan(CloseAfter(after_bytes=10, side="server"))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"x" * 50)
            with contextlib.suppress(OSError):
                sock.recv(65536)
        wait_until(lambda: proxy.active_connections == 0)

        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"short")
            sock.shutdown(socket.SHUT_WR)
            assert recv_until_eof(sock) == b"short"
        wait_until(lambda: proxy.active_connections == 0)

        report = proxy.fault_report()
        assert [entry.status for entry in report] == [FIRED, MISSED]
        proxy.assert_all_fired()


def test_assert_all_fired_excuses_preempted_by_default(
    echo_server: tuple[str, int],
) -> None:
    plan = Plan(
        Reset(after_bytes=1, side="server"),
        CloseAfter(after_bytes=1000, side="server"),
    )
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            with contextlib.suppress(OSError):
                sock.sendall(b"x" * 100)
            with contextlib.suppress(OSError):
                sock.recv(65536)
        wait_until(lambda: proxy.active_connections == 0)
        proxy.assert_all_fired()


def test_assert_all_fired_failure_message_is_readable(
    echo_server: tuple[str, int],
) -> None:
    plan = Plan(CloseAfter(after=saw(b"NEVER", side="server"), side="server"))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"hello")
            sock.shutdown(socket.SHUT_WR)
            recv_until_eof(sock)
        wait_until(lambda: proxy.active_connections == 0)
        with pytest.raises(AssertionError) as excinfo:
            proxy.assert_all_fired()

    message = str(excinfo.value)
    assert "assert_all_fired() failed" in message
    assert "CloseAfter" in message
    assert "MISSED" in message
    assert "conn 0" in message
    assert "ended=graceful" in message


def test_assert_all_fired_allow_unmatched(echo_server: tuple[str, int]) -> None:
    plan = Plan(CloseAfter(after_bytes=1, side="server", conn=nth(2)))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        assert round_trip(proxy, b"a") == b"a"
        wait_until(lambda: proxy.active_connections == 0)
        with pytest.raises(AssertionError):
            proxy.assert_all_fired()
        proxy.assert_all_fired(allow={"unmatched"})


def test_assert_all_fired_allow_missed(echo_server: tuple[str, int]) -> None:
    plan = Plan(CloseAfter(after=saw(b"NEVER", side="server"), side="server"))
    with Proxy(echo_server, plan, seed=0, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"hello")
            sock.shutdown(socket.SHUT_WR)
            recv_until_eof(sock)
        wait_until(lambda: proxy.active_connections == 0)
        proxy.assert_all_fired(allow={"MISSED"})
