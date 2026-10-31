from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from flakynet import Plan, Proxy
from flakynet import proxy as make_proxy

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

SEED = 7


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met before the deadline")


def fired(proxy: Proxy) -> bool:
    return any(status.status == "FIRED" for status in proxy.fault_report())


def redis_target(url: str) -> tuple[str, int]:
    parsed = urlparse(url)
    return (parsed.hostname or "127.0.0.1", parsed.port or 6379)


def redis_db(url: str) -> int:
    return int(urlparse(url).path.strip("/") or 0)


@contextlib.contextmanager
def running_proxy(
    target: tuple[str, int],
    plan: Plan,
    *,
    seed: int | str = SEED,
    name: str = "p0",
) -> Generator[Proxy, None, None]:
    with make_proxy(target, plan, seed=seed, name=name) as proxy:
        yield proxy
