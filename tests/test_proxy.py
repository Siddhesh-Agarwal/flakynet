from __future__ import annotations

import contextlib
import socket
import struct
import sys
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor

import pytest

from flakynet import (
    CloseAfter,
    Latency,
    Plan,
    Proxy,
    Reset,
    between,
    resolve,
    saw,
)


def _send_all(sock: socket.socket, payload: bytes, errors: list[BaseException]) -> None:
    try:
        sock.sendall(payload)
    except BaseException as exc:
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
def recording_server() -> Iterator[tuple[tuple[str, int], list[bytes], threading.Event]]:
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
) -> Iterator[tuple[tuple[str, int], list[bytes], threading.Event]]:
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
def abrupt_then_echo_server() -> Iterator[tuple[str, int]]:
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
        )
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
        )
    )


def test_upstream_connect_failure_closes_client() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        target = probe.getsockname()

    with Proxy((str(target[0]), int(target[1]))) as proxy:
        with socket.create_connection((proxy.host, proxy.port), timeout=5.0) as sock:
            sock.settimeout(2.0)
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


def test_proxy_rejects_unwired_faults() -> None:
    with pytest.raises(NotImplementedError):
        Proxy(("127.0.0.1", 1), Plan(Latency(ms=1)))


def test_proxy_rejects_armed_faults() -> None:
    fault = CloseAfter(after=saw(b"X", side="server"), side="server")
    with pytest.raises(NotImplementedError):
        Proxy(("127.0.0.1", 1), Plan(fault))
