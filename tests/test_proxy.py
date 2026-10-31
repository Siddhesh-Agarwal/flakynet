from __future__ import annotations

import contextlib
import queue
import socket
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import pytest

from flakynet import (
    Bandwidth,
    Blackhole,
    CanonicalEvent,
    CloseAfter,
    Event,
    Fragment,
    Latency,
    Plan,
    Proxy,
    RefuseConnect,
    Reset,
    between,
    nth,
    resolve,
    saw,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


def _send_all(sock: socket.socket, payload: bytes, errors: list[BaseException]) -> None:
    try:
        sock.sendall(payload)
    except BaseException as exc:  # noqa: BLE001 - record every sender failure
        errors.append(exc)


def round_trip(proxy: Proxy, payload: bytes, timeout: float = 5.0) -> bytes:
    with socket.create_connection((proxy.host, proxy.port), timeout=timeout) as sock:
        errors: list[BaseException] = []
        sender = threading.Thread(
            target=_send_all,
            args=(sock, payload, errors),
            daemon=True,
            name="test-sender",
        )
        sender.start()
        received = bytearray()
        while len(received) < len(payload):
            chunk = sock.recv(65536)
            if not chunk:
                break
            received.extend(chunk)
        sender.join(timeout=timeout)
        if sender.is_alive():
            raise AssertionError("sender thread did not finish")
        if errors:
            raise errors[0]
        return bytes(received)


def recv_until_eof(sock: socket.socket) -> bytes:
    chunks: list[bytes] = []
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the deadline")


def abort_first_connection(proxy: Proxy) -> None:
    with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
        sock.settimeout(2.0)
        with contextlib.suppress(OSError):
            sock.recv(1)


def read_exact(sock: socket.socket, count: int) -> bytes:
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise AssertionError(f"EOF after {len(data)} of {count} bytes")
        data.extend(chunk)
    return bytes(data)


def read_prefix_and_end(sock: socket.socket, count: int) -> tuple[bytes, str]:
    prefix = read_exact(sock, count)
    try:
        extra = sock.recv(1)
    except OSError:
        return prefix, "reset"
    if extra == b"":
        return prefix, "eof"
    return prefix + extra, "more"


@contextlib.contextmanager
def recording_server() -> Generator[
    tuple[tuple[str, int], list[bytes], threading.Event], None, None
]:
    listener = socket.create_server(("127.0.0.1", 0))
    received: list[bytes] = []
    done = threading.Event()

    def run() -> None:
        try:
            conn, _ = listener.accept()
            with conn:
                data = bytearray()
                while True:
                    try:
                        chunk = conn.recv(65536)
                    except OSError:
                        break
                    if not chunk:
                        break
                    data.extend(chunk)
                received.append(bytes(data))
        except OSError:
            pass
        finally:
            listener.close()
            done.set()

    thread = threading.Thread(target=run, daemon=True, name="recording-server")
    thread.start()
    host, port = listener.getsockname()[:2]
    try:
        yield (str(host), int(port)), received, done
    finally:
        listener.close()
        thread.join(timeout=2.0)


@contextlib.contextmanager
def half_close_server(
    banner: bytes,
) -> Generator[tuple[tuple[str, int], list[bytes], threading.Event], None, None]:
    listener = socket.create_server(("127.0.0.1", 0))
    received: list[bytes] = []
    done = threading.Event()

    def run() -> None:
        try:
            conn, _ = listener.accept()
            with conn:
                conn.sendall(banner)
                conn.shutdown(socket.SHUT_WR)
                data = bytearray()
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    data.extend(chunk)
                received.append(bytes(data))
        except OSError:
            pass
        finally:
            listener.close()
            done.set()

    thread = threading.Thread(target=run, daemon=True, name="half-close-server")
    thread.start()
    host, port = listener.getsockname()[:2]
    try:
        yield (str(host), int(port)), received, done
    finally:
        listener.close()
        thread.join(timeout=2.0)


@contextlib.contextmanager
def abrupt_then_echo_server() -> Generator[tuple[str, int], None, None]:
    listener = socket.create_server(("127.0.0.1", 0))

    def run() -> None:
        try:
            first, _ = listener.accept()
            first.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_LINGER,
                struct.pack("ii", 1, 0),
            )
            first.close()

            second, _ = listener.accept()
            with second:
                while True:
                    data = second.recv(65536)
                    if not data:
                        break
                    second.sendall(data)
        except OSError:
            pass
        finally:
            listener.close()

    thread = threading.Thread(target=run, daemon=True, name="abrupt-echo-server")
    thread.start()
    host, port = listener.getsockname()[:2]
    try:
        yield str(host), int(port)
    finally:
        listener.close()
        thread.join(timeout=2.0)


@contextlib.contextmanager
def collecting_server() -> Generator[
    tuple[tuple[str, int], queue.Queue[bytes], threading.Event], None, None
]:
    listener = socket.create_server(("127.0.0.1", 0))
    chunks: queue.Queue[bytes] = queue.Queue()
    done = threading.Event()

    def run() -> None:
        try:
            conn, _ = listener.accept()
            with conn:
                while True:
                    try:
                        data = conn.recv(65536)
                    except OSError:
                        break
                    if not data:
                        break
                    chunks.put(data)
        except OSError:
            pass
        finally:
            listener.close()
            done.set()

    thread = threading.Thread(target=run, daemon=True, name="collecting-server")
    thread.start()
    host, port = listener.getsockname()[:2]
    try:
        yield (str(host), int(port)), chunks, done
    finally:
        listener.close()
        thread.join(timeout=2.0)


@contextlib.contextmanager
def sending_server(payload: bytes) -> Generator[tuple[str, int], None, None]:
    listener = socket.create_server(("127.0.0.1", 0))

    def run() -> None:
        try:
            conn, _ = listener.accept()
            with conn:
                conn.sendall(payload)
                while conn.recv(65536):
                    pass
        except OSError:
            pass
        finally:
            listener.close()

    thread = threading.Thread(target=run, daemon=True, name="sending-server")
    thread.start()
    host, port = listener.getsockname()[:2]
    try:
        yield str(host), int(port)
    finally:
        listener.close()
        thread.join(timeout=2.0)


@contextlib.contextmanager
def line_server() -> Generator[tuple[tuple[str, int], list[bytes], threading.Event], None, None]:
    listener = socket.create_server(("127.0.0.1", 0))
    received: list[bytes] = []
    done = threading.Event()

    def run() -> None:
        try:
            conn, _ = listener.accept()
            with conn:
                buffer = bytearray()
                while True:
                    data = conn.recv(65536)
                    if not data:
                        break
                    buffer.extend(data)
                    while b"\n" in buffer:
                        line, _, rest = buffer.partition(b"\n")
                        buffer[:] = rest
                        received.append(bytes(line) + b"\n")
                        if line == b"COMMIT":
                            conn.sendall(b"OK\n")
        except OSError:
            pass
        finally:
            listener.close()
            done.set()

    thread = threading.Thread(target=run, daemon=True, name="line-server")
    thread.start()
    host, port = listener.getsockname()[:2]
    try:
        yield (str(host), int(port)), received, done
    finally:
        listener.close()
        thread.join(timeout=2.0)


def collect_exact(chunks: queue.Queue[bytes], count: int, timeout: float = 2.0) -> bytes:
    data = bytearray()
    deadline = time.monotonic() + timeout
    while len(data) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"received {len(data)} of {count} bytes before the deadline")
        data.extend(chunks.get(timeout=remaining))
    return bytes(data)


def run_and_capture(
    target: tuple[str, int],
    plan: Plan,
    payload: bytes,
    *,
    seed: int | str = 1234,
    window_exits: int = 1,
) -> tuple[bytes, tuple[CanonicalEvent, ...]]:
    with (
        Proxy(target, plan, seed=seed, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=10.0) as sock,
    ):
        sock.sendall(payload)
        received = read_exact(sock, len(payload))
        # The client can finish reading before the pump resumes from its last
        # send and records the window exits, especially on Windows.
        wait_until(
            lambda: (
                sum(event.kind == "window_exited" for event in proxy.canonical_events())
                >= window_exits
            ),
        )
        events = proxy.canonical_events()
    return received, events


def test_one_byte_round_trip(echo_server: tuple[str, int]) -> None:
    with Proxy(echo_server) as proxy:
        assert round_trip(proxy, b"x") == b"x"
        wait_until(lambda: proxy.bytes_client_to_server == 1)
        wait_until(lambda: proxy.bytes_server_to_client == 1)


def test_one_megabyte_round_trip(echo_server: tuple[str, int]) -> None:
    payload = b"x" * 1_000_000
    with Proxy(echo_server) as proxy:
        assert round_trip(proxy, payload, timeout=10.0) == payload
        wait_until(lambda: proxy.bytes_client_to_server == len(payload), timeout=5.0)
        wait_until(lambda: proxy.bytes_server_to_client == len(payload), timeout=5.0)


def test_many_concurrent_connections(echo_server: tuple[str, int]) -> None:
    payloads = [f"payload-{index}-".encode() * 128 for index in range(32)]
    with Proxy(echo_server) as proxy:
        with ThreadPoolExecutor(max_workers=32) as pool:
            results = list(pool.map(lambda payload: round_trip(proxy, payload), payloads))
        assert results == payloads
        wait_until(lambda: proxy.active_connections == 0, timeout=5.0)


def test_start_stop_twice_same_instance(echo_server: tuple[str, int]) -> None:
    proxy = Proxy(echo_server)
    proxy.start()
    assert round_trip(proxy, b"one") == b"one"
    proxy.stop()

    proxy.start()
    assert round_trip(proxy, b"two") == b"two"
    proxy.stop()

    assert proxy.active_connections == 0


def test_two_instances_sequentially(echo_server: tuple[str, int]) -> None:
    with Proxy(echo_server) as first:
        assert round_trip(first, b"first") == b"first"
    with Proxy(echo_server) as second:
        assert round_trip(second, b"second") == b"second"


def test_no_thread_left_after_exit(echo_server: tuple[str, int]) -> None:
    with Proxy(echo_server) as proxy:
        assert round_trip(proxy, b"ping") == b"ping"

    wait_until(
        lambda: (
            not any(thread.name.startswith("flakynet-proxy") for thread in threading.enumerate())
        ),
    )
    assert proxy.active_connections == 0
    with pytest.raises(OSError):
        socket.create_connection((proxy.host, proxy.port), timeout=1.0).close()


def test_client_half_close(echo_server: tuple[str, int]) -> None:
    payload = b"hello-half-close"
    with (
        Proxy(echo_server) as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
        assert recv_until_eof(sock) == payload


def test_server_half_close() -> None:
    with (
        half_close_server(b"banner") as (target, received, done),
        Proxy(target) as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        assert recv_until_eof(sock) == b"banner"
        sock.sendall(b"client-data")
        sock.shutdown(socket.SHUT_WR)
    assert done.wait(timeout=2.0)
    assert received == [b"client-data"]


def test_client_abrupt_close(echo_server: tuple[str, int]) -> None:
    with Proxy(echo_server) as proxy:
        sock = socket.create_connection((proxy.host, proxy.port), timeout=5.0)
        sock.sendall(b"x")
        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", 1, 0),
        )
        sock.close()
        wait_until(lambda: proxy.active_connections == 0, timeout=5.0)
        assert round_trip(proxy, b"after-rst") == b"after-rst"


def test_server_abrupt_close_then_recovery() -> None:
    with abrupt_then_echo_server() as target, Proxy(target) as proxy:
        abort_first_connection(proxy)
        wait_until(lambda: proxy.active_connections == 0, timeout=5.0)
        assert round_trip(proxy, b"recovered") == b"recovered"


def test_stop_with_active_connection(echo_server: tuple[str, int]) -> None:
    proxy = Proxy(echo_server)
    proxy.start()
    sock = socket.create_connection((proxy.host, proxy.port), timeout=5.0)
    sock.settimeout(2.0)

    proxy.stop()

    try:
        data = sock.recv(1)
    except OSError:
        data = b""
    sock.close()
    assert data == b""
    assert proxy.active_connections == 0
    wait_until(
        lambda: (
            not any(thread.name.startswith("flakynet-proxy") for thread in threading.enumerate())
        ),
    )


def test_upstream_connect_failure_closes_client() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        target = probe.getsockname()

    with Proxy((str(target[0]), int(target[1]))) as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            # Windows retries the SYN after an RST, so a refused local connect
            # can take a couple of seconds to reach the proxy.
            sock.settimeout(30.0 if sys.platform == "win32" else 2.0)
            assert sock.recv(1) == b""
        wait_until(lambda: proxy.active_connections == 0)


def test_close_after_server_side_exact_prefix(echo_server: tuple[str, int]) -> None:
    fault = CloseAfter(after_bytes=10, side="server")
    with (
        Proxy(echo_server, Plan(fault), seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.sendall(b"x" * 1024)
        prefix, status = read_prefix_and_end(sock, 10)
        assert prefix == b"x" * 10
        assert status == "eof"


def test_reset_server_side_exact_prefix(echo_server: tuple[str, int]) -> None:
    fault = Reset(after_bytes=10, side="server")
    with (
        Proxy(echo_server, Plan(fault), seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.sendall(b"x" * 1024)
        prefix, status = read_prefix_and_end(sock, 10)
        assert prefix == b"x" * 10
        if sys.platform.startswith("linux"):
            assert status == "reset"
        else:
            assert status in {"reset", "eof"}


def test_close_after_client_side_exact_prefix() -> None:
    with recording_server() as (target, received, done):
        fault = CloseAfter(after_bytes=10, side="client")
        with (
            Proxy(target, Plan(fault), seed=0, name="p0") as proxy,
            socket.create_connection(
                (proxy.host, proxy.port),
                timeout=5.0,
            ) as sock,
        ):
            with contextlib.suppress(OSError):
                sock.sendall(b"x" * 1024)
            assert done.wait(timeout=2.0)
        assert received == [b"x" * 10]


def test_reset_client_side_exact_prefix() -> None:
    with recording_server() as (target, received, done):
        fault = Reset(after_bytes=10, side="client")
        with (
            Proxy(target, Plan(fault), seed=0, name="p0") as proxy,
            socket.create_connection(
                (proxy.host, proxy.port),
                timeout=5.0,
            ) as sock,
        ):
            with contextlib.suppress(OSError):
                sock.sendall(b"x" * 1024)
            assert done.wait(timeout=2.0)
        assert received == [b"x" * 10]


def test_close_after_same_seed_same_prefix(echo_server: tuple[str, int]) -> None:
    plan = Plan(CloseAfter(after_bytes=between(100, 1000), side="server"))
    expected = resolve(plan, 1234, "p0", 0).faults[0].start
    assert expected is not None
    observed: list[int] = []

    for _ in range(2):
        with (
            Proxy(echo_server, plan, seed=1234, name="p0") as proxy,
            socket.create_connection(
                (proxy.host, proxy.port),
                timeout=5.0,
            ) as sock,
        ):
            sock.sendall(b"x" * 4096)
            prefix, status = read_prefix_and_end(sock, expected)
            assert prefix == b"x" * expected
            assert status == "eof"
            observed.append(len(prefix))

    assert observed == [expected, expected]


def test_reset_same_seed_same_prefix(echo_server: tuple[str, int]) -> None:
    plan = Plan(Reset(after_bytes=between(100, 1000), side="server"))
    expected = resolve(plan, 1234, "p0", 0).faults[0].start
    assert expected is not None
    observed: list[int] = []

    for _ in range(2):
        with (
            Proxy(echo_server, plan, seed=1234, name="p0") as proxy,
            socket.create_connection(
                (proxy.host, proxy.port),
                timeout=5.0,
            ) as sock,
        ):
            sock.sendall(b"x" * 4096)
            prefix, status = read_prefix_and_end(sock, expected)
            assert prefix == b"x" * expected
            if sys.platform.startswith("linux"):
                assert status == "reset"
            else:
                assert status in {"reset", "eof"}
            observed.append(len(prefix))

    assert observed == [expected, expected]


def test_close_after_large_offset_splits_across_reads(
    echo_server: tuple[str, int],
) -> None:
    payload = b"x" * 200_000
    fault = CloseAfter(after_bytes=70_000, side="server")
    with (
        Proxy(echo_server, Plan(fault), seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=10.0) as sock,
    ):
        errors: list[BaseException] = []
        sender = threading.Thread(
            target=_send_all,
            args=(sock, payload, errors),
            daemon=True,
        )
        sender.start()
        prefix, status = read_prefix_and_end(sock, 70_000)
        sender.join(timeout=10.0)

    assert not sender.is_alive()
    assert prefix == b"x" * 70_000
    assert status == "eof"


def test_proxy_accepts_all_static_faults() -> None:
    plan = Plan(
        Latency(ms=0),
        Bandwidth(bytes_per_second=1000),
        Fragment(max_bytes=10),
        Reset(after_bytes=1),
        CloseAfter(after_bytes=1),
        Blackhole(after_bytes=1),
        RefuseConnect(),
    )
    proxy = Proxy(("127.0.0.1", 1), plan)
    assert proxy.plan == plan


def test_zero_offset_fault_fires_without_client_data(echo_server: tuple[str, int]) -> None:
    plan = Plan(CloseAfter(after_bytes=0, side="client"))
    with (
        Proxy(echo_server, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(1.0)
        assert sock.recv(1) == b""


def test_proxy_accepts_armed_faults() -> None:
    plan = Plan(
        Reset(after=saw(b"A", side="client")),
        CloseAfter(after=saw(b"B", side="server")),
        Blackhole(after=saw(b"C", side="client")),
    )
    proxy = Proxy(("127.0.0.1", 1), plan)
    assert proxy.plan == plan


@pytest.mark.timing_sensitive
def test_latency_delays_server_stream_inside_window(echo_server: tuple[str, int]) -> None:
    payload = b"latency!"
    plan = Plan(Latency(ms=200, side="server", window=(0, len(payload))))
    with (
        Proxy(echo_server, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=10.0) as sock,
    ):
        started = time.monotonic()
        sock.sendall(payload)
        assert read_exact(sock, len(payload)) == payload
        elapsed = time.monotonic() - started
    assert elapsed >= 0.10


def test_latency_same_seed_same_canonical_events(echo_server: tuple[str, int]) -> None:
    payload = b"latency!"
    plan = Plan(Latency(ms=1, side="server", window=(0, len(payload))))
    first = run_and_capture(echo_server, plan, payload)
    second = run_and_capture(echo_server, plan, payload)
    assert first[0] == second[0] == payload
    assert first[1] == second[1]
    assert [event.kind for event in first[1]] == ["window_entered", "window_exited"]


@pytest.mark.timing_sensitive
def test_bandwidth_caps_server_stream_inside_window(echo_server: tuple[str, int]) -> None:
    payload = b"b" * 2000
    plan = Plan(Bandwidth(bytes_per_second=2000, side="server", window=(0, len(payload))))
    with (
        Proxy(echo_server, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=10.0) as sock,
    ):
        started = time.monotonic()
        sock.sendall(payload)
        assert read_exact(sock, len(payload)) == payload
        elapsed = time.monotonic() - started
    assert elapsed >= 0.4


def test_bandwidth_same_seed_same_canonical_events(echo_server: tuple[str, int]) -> None:
    payload = b"b" * 100
    plan = Plan(
        Bandwidth(
            bytes_per_second=between(1000, 2000),
            side="server",
            window=(0, len(payload)),
        ),
    )
    first = run_and_capture(echo_server, plan, payload)
    second = run_and_capture(echo_server, plan, payload)
    assert first[0] == second[0] == payload
    assert first[1] == second[1]
    assert resolve(plan, 1234, "p0", 0) == resolve(plan, 1234, "p0", 0)


def test_fragment_forwards_exact_payload(echo_server: tuple[str, int]) -> None:
    payload = bytes(range(256)) * 4
    plan = Plan(Fragment(max_bytes=7, side="server"))
    with (
        Proxy(echo_server, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=10.0) as sock,
    ):
        sock.sendall(payload)
        assert read_exact(sock, len(payload)) == payload


@pytest.mark.timing_sensitive
def test_fragment_pieces_are_separate_latency_segments(echo_server: tuple[str, int]) -> None:
    payload = b"f" * 40
    plan = Plan(
        Fragment(max_bytes=1, side="server", window=(0, len(payload))),
        Latency(ms=5, side="server", window=(0, len(payload))),
    )
    with (
        Proxy(echo_server, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=10.0) as sock,
    ):
        started = time.monotonic()
        sock.sendall(payload)
        assert read_exact(sock, len(payload)) == payload
        elapsed = time.monotonic() - started
    assert elapsed >= 0.10


def test_latency_logs_only_window_events_even_when_fragmented(
    echo_server: tuple[str, int],
) -> None:
    payload = b"f" * 20
    plan = Plan(
        Fragment(max_bytes=1, side="server", window=(0, len(payload))),
        Latency(ms=0, side="server", window=(0, len(payload))),
    )
    received, events = run_and_capture(echo_server, plan, payload, window_exits=2)
    assert received == payload
    assert {event.kind for event in events} == {"window_entered", "window_exited"}
    latency_events = [event for event in events if event.fault_index == 1]
    assert [event.kind for event in latency_events] == ["window_entered", "window_exited"]


def test_fragment_same_seed_same_canonical_events(echo_server: tuple[str, int]) -> None:
    payload = b"f" * 100
    plan = Plan(Fragment(max_bytes=7, side="server", window=(0, len(payload))))
    first = run_and_capture(echo_server, plan, payload)
    second = run_and_capture(echo_server, plan, payload)
    assert first[0] == second[0] == payload
    assert first[1] == second[1]


def test_blackhole_client_side_drops_after_prefix() -> None:
    with collecting_server() as (target, chunks, done):
        plan = Plan(Blackhole(after_bytes=10, side="client"))
        with (
            Proxy(target, plan, seed=0, name="p0") as proxy,
            socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
        ):
            sock.sendall(b"x" * 100)
            assert collect_exact(chunks, 10) == b"x" * 10
            with pytest.raises(queue.Empty):
                chunks.get(timeout=0.2)
            sock.sendall(b"y" * 100)
            with pytest.raises(queue.Empty):
                chunks.get(timeout=0.2)
        assert done.wait(timeout=2.0)


def test_blackhole_server_side_keeps_connection_open() -> None:
    payload = b"s" * 100
    with sending_server(payload) as target:
        plan = Plan(Blackhole(after_bytes=10, side="server"))
        with (
            Proxy(target, plan, seed=0, name="p0") as proxy,
            socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
        ):
            sock.settimeout(5.0)
            assert read_exact(sock, 10) == b"s" * 10
            sock.settimeout(0.3)
            with pytest.raises(socket.timeout):
                sock.recv(1)


def run_blackhole_scenario(seed: int) -> tuple[bytes, tuple[CanonicalEvent, ...]]:
    with collecting_server() as (target, chunks, done):
        plan = Plan(Blackhole(after_bytes=10, side="client"))
        with (
            Proxy(target, plan, seed=seed, name="p0") as proxy,
            socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
        ):
            sock.sendall(b"x" * 100)
            prefix = collect_exact(chunks, 10)
            wait_until(lambda: len(proxy.canonical_events()) >= 1)
            events = proxy.canonical_events()
        assert done.wait(timeout=2.0)
    return prefix, events


def test_blackhole_same_seed_same_prefix_and_events() -> None:
    first = run_blackhole_scenario(1234)
    second = run_blackhole_scenario(1234)
    assert first[0] == second[0] == b"x" * 10
    assert first[1] == second[1]
    assert [event.kind for event in first[1]] == ["fired"]


def run_refuse_scenario(
    echo_server: tuple[str, int],
) -> tuple[list[str], tuple[CanonicalEvent, ...]]:
    plan = Plan(RefuseConnect(conn=nth(0)))
    statuses: list[str] = []
    with Proxy(echo_server, plan, seed=1234, name="p0") as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.settimeout(1.0)
            try:
                data = sock.recv(1)
            except OSError:
                statuses.append("reset")
            else:
                statuses.append("eof" if data == b"" else "more")

        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.sendall(b"ok")
            assert read_exact(sock, 2) == b"ok"

        wait_until(lambda: len(proxy.canonical_events()) >= 1)
        events = proxy.canonical_events()
    return statuses, events


def test_refuse_connect_resets_selected_connection(echo_server: tuple[str, int]) -> None:
    statuses, events = run_refuse_scenario(echo_server)
    if sys.platform.startswith("linux"):
        assert statuses == ["reset"]
    else:
        assert statuses[0] in {"reset", "eof"}
    assert events == (
        CanonicalEvent(
            conn_index=0,
            stream=None,
            offset=0,
            fault_index=0,
            fault="RefuseConnect",
            kind="fired",
            params=None,
        ),
    )


def test_refuse_connect_same_seed_same_status_and_events(echo_server: tuple[str, int]) -> None:
    first = run_refuse_scenario(echo_server)
    second = run_refuse_scenario(echo_server)
    assert first == second


def recv_until_eof_safe(sock: socket.socket) -> bytes:
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


def run_armed_same_stream(
    target: tuple[str, int],
    chunks: list[bytes],
) -> tuple[bytes, int | None, tuple[CanonicalEvent, ...]]:
    plan = Plan(
        CloseAfter(
            after=saw(b"COMMIT", side="server", delta=2),
            side="server",
        ),
    )
    with (
        Proxy(target, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(5.0)
        for chunk in chunks:
            with contextlib.suppress(OSError):
                sock.sendall(chunk)
        received = recv_until_eof_safe(sock)
        wait_until(lambda: any(event.kind == "fired" for event in proxy.events))
        fired = [event for event in proxy.events if event.kind == "fired"]
        offset = fired[0].offset if fired else None
        events = proxy.canonical_events()
    return received, offset, events


def test_armed_same_stream_anchor_is_chunk_invariant(echo_server: tuple[str, int]) -> None:
    payload = b"BEGINCOMMITXY"
    unsplit = run_armed_same_stream(echo_server, [payload])
    split = run_armed_same_stream(echo_server, [bytes([byte]) for byte in payload])
    assert unsplit[0] == split[0] == payload
    assert unsplit[1] == split[1] == 13
    assert unsplit[2] == split[2]


def run_armed_same_stream_determinism(
    target: tuple[str, int],
) -> tuple[bytes, tuple[CanonicalEvent, ...]]:
    plan = Plan(
        CloseAfter(
            after=saw(b"X", side="server", delta=between(0, 4)),
            side="server",
        ),
    )
    with (
        Proxy(target, plan, seed=1234, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(5.0)
        with contextlib.suppress(OSError):
            sock.sendall(b"prefixXtail!!!!")
        received = recv_until_eof_safe(sock)
        wait_until(lambda: any(event.kind == "fired" for event in proxy.events))
        events = proxy.canonical_events()
    return received, events


def test_armed_same_stream_is_deterministic_across_runs(
    echo_server: tuple[str, int],
) -> None:
    first = run_armed_same_stream_determinism(echo_server)
    second = run_armed_same_stream_determinism(echo_server)
    assert first == second
    assert [event.kind for event in first[1]] == ["fired"]


def run_armed_cross_stream(
    target: tuple[str, int],
) -> tuple[bytes, tuple[Event, ...], tuple[CanonicalEvent, ...]]:
    plan = Plan(CloseAfter(after=saw(b"X", side="client"), side="server"))
    with (
        Proxy(target, plan, seed=0, name="p0") as proxy,
        socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
    ):
        sock.settimeout(5.0)
        with contextlib.suppress(OSError):
            sock.sendall(b"X" + b"y" * 200)
        received = recv_until_eof_safe(sock)
        wait_until(lambda: any(event.kind == "fired" for event in proxy.events))
        raw = tuple(event for event in proxy.events if event.kind == "fired")
        canonical = tuple(event for event in proxy.canonical_events() if event.kind == "fired")
    return received, raw, canonical


def test_armed_cross_stream_records_inexact_offset(echo_server: tuple[str, int]) -> None:
    _, raw, canonical = run_armed_cross_stream(echo_server)
    assert len(raw) == 1
    assert raw[0].offset_exact is False
    assert raw[0].stream == "server"
    assert len(canonical) == 1
    assert canonical[0].offset is None


def test_armed_request_delivered_reply_blackholed() -> None:
    with line_server() as (target, received, done):
        plan = Plan(Blackhole(after=saw(b"COMMIT\n", side="client"), side="server"))
        with (
            Proxy(target, plan, seed=0, name="p0") as proxy,
            socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock,
        ):
            sock.sendall(b"COMMIT\n")
            wait_until(lambda: received == [b"COMMIT\n"])
            sock.settimeout(0.3)
            with pytest.raises(socket.timeout):
                sock.recv(1)

            wait_until(lambda: any(event.kind == "fired" for event in proxy.events))
            fired = next(event for event in proxy.events if event.kind == "fired")
            assert fired.offset_exact is False
            canonical = [event for event in proxy.canonical_events() if event.kind == "fired"]
            assert canonical[0].offset is None
        assert done.wait(timeout=2.0)
