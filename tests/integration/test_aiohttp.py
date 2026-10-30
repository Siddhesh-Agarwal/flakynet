from __future__ import annotations

import asyncio

import pytest
from support import fired, running_proxy, wait_until

from flakynet import Fragment, Plan, Reset

aiohttp = pytest.importorskip("aiohttp")

pytestmark = pytest.mark.integration


async def fetch(url: str) -> bytes:
    timeout = aiohttp.ClientTimeout(total=10.0)
    async with (
        aiohttp.ClientSession(timeout=timeout, trust_env=False) as session,
        session.get(url) as response,
    ):
        response.raise_for_status()
        return await response.read()


def proxy_url(proxy, path: str) -> str:
    return f"http://{proxy.host}:{proxy.port}{path}"


def test_aiohttp_reset_mid_response(http_server: tuple[str, int]) -> None:
    plan = Plan(Reset(after_bytes=1000, side="server"))
    with running_proxy(http_server, plan) as proxy:
        with pytest.raises(aiohttp.ClientError):
            asyncio.run(fetch(proxy_url(proxy, "/bytes?n=200000")))
        wait_until(lambda: proxy.active_connections == 0)
        assert fired(proxy)


def test_aiohttp_fragment_one_byte(http_server: tuple[str, int]) -> None:
    plan = Plan(Fragment(max_bytes=1, side="server"))
    with running_proxy(http_server, plan) as proxy:
        body = asyncio.run(fetch(proxy_url(proxy, "/bytes?n=2000")))
        assert body == b"x" * 2000
        wait_until(lambda: proxy.active_connections == 0)
        assert fired(proxy)
