from __future__ import annotations

import contextlib
import socket
import time
from typing import TYPE_CHECKING

import pytest

from flakynet import CloseAfter, Plan, Reset, proxy

if TYPE_CHECKING:
    from collections.abc import Callable


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the deadline")


def test_proxy_factory_does_not_bind_until_start() -> None:
    created = proxy(("127.0.0.1", 1))
    with pytest.raises(RuntimeError):
        _ = created.port
    assert not created.is_running

    created.start()
    try:
        assert created.is_running
        assert created.port > 0
    finally:
        created.stop()
    assert not created.is_running


def test_proxy_factory_returns_a_context_manager(
    echo_server: tuple[str, int],
) -> None:
    with proxy(echo_server) as created:
        assert created.is_running
        assert created.port > 0
    assert not created.is_running
    assert created.events == ()


def test_proxy_factory_normalizes_single_fault_iterable_and_plan() -> None:
    fault = Reset(after_bytes=1)
    expected = Plan(fault)
    assert proxy(("127.0.0.1", 1), fault).plan == expected
    assert proxy(("127.0.0.1", 1), [fault]).plan == expected
    assert proxy(("127.0.0.1", 1), expected).plan == expected
    assert proxy(("127.0.0.1", 1)).plan == Plan()


def test_events_and_report_are_readable_after_stop(
    echo_server: tuple[str, int],
) -> None:
    created = proxy(
        echo_server,
        CloseAfter(after_bytes=5, side="server"),
        seed=3,
        name="p0",
    )
    created.start()
    with socket.create_connection((created.host, created.port), timeout=5.0) as sock:
        with contextlib.suppress(OSError):
            sock.sendall(b"x" * 50)
        with contextlib.suppress(OSError):
            sock.recv(65536)
    wait_until(lambda: created.active_connections == 0)
    created.stop()

    assert created.seed == 3
    assert created.name == "p0"
    assert created.events
    assert created.canonical_events()
    assert created.fault_report()
