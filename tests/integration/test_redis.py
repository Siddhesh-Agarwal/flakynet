from __future__ import annotations

import pytest
from support import (
    fired,
    redis_db,
    redis_target,
    running_proxy,
    wait_until,
)

from flakynet import Fragment, Plan, Reset

redis = pytest.importorskip("redis")

pytestmark = pytest.mark.integration

RESET_KEY = "flakynet:integration:redis:reset"
FRAGMENT_KEY = "flakynet:integration:redis:fragment"


def proxied_client(proxy, url: str):
    return redis.Redis(
        host=proxy.host,
        port=proxy.port,
        db=redis_db(url),
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
    )


def direct_client(url: str):
    return redis.Redis.from_url(
        url,
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
    )


def test_redis_reset_mid_get(redis_url: str) -> None:
    direct = direct_client(redis_url)
    try:
        direct.set(RESET_KEY, b"x" * 200_000)
        plan = Plan(Reset(after_bytes=4096, side="server"))
        with running_proxy(redis_target(redis_url), plan) as proxy:
            client = proxied_client(proxy, redis_url)
            try:
                with pytest.raises(redis.exceptions.RedisError):
                    client.get(RESET_KEY)
            finally:
                client.close()
            wait_until(lambda: proxy.active_connections == 0)
            assert fired(proxy)
    finally:
        direct.delete(RESET_KEY)
        direct.close()


def test_redis_fragment_one_byte(redis_url: str) -> None:
    direct = direct_client(redis_url)
    try:
        direct.set(FRAGMENT_KEY, b"x" * 2000)
        plan = Plan(Fragment(max_bytes=1, side="server"))
        with running_proxy(redis_target(redis_url), plan) as proxy:
            client = proxied_client(proxy, redis_url)
            try:
                assert client.get(FRAGMENT_KEY) == b"x" * 2000
            finally:
                client.close()
            wait_until(lambda: proxy.active_connections == 0)
            assert fired(proxy)
    finally:
        direct.delete(FRAGMENT_KEY)
        direct.close()
