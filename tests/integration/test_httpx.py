from __future__ import annotations

import pytest
from support import fired, running_proxy, wait_until

from flakynet import Fragment, Plan, Reset

httpx = pytest.importorskip("httpx")

pytestmark = pytest.mark.integration


def proxy_url(proxy, path: str) -> str:
    return f"http://{proxy.host}:{proxy.port}{path}"


def test_httpx_reset_mid_response(http_server: tuple[str, int]) -> None:
    plan = Plan(Reset(after_bytes=1000, side="server"))
    with running_proxy(http_server, plan) as proxy:
        with (
            httpx.Client(timeout=5.0, trust_env=False) as client,
            pytest.raises(httpx.HTTPError),
        ):
            client.get(proxy_url(proxy, "/bytes?n=200000"))
        wait_until(lambda: proxy.active_connections == 0)
        assert fired(proxy)


def test_httpx_fragment_one_byte(http_server: tuple[str, int]) -> None:
    plan = Plan(Fragment(max_bytes=1, side="server"))
    with running_proxy(http_server, plan) as proxy:
        with httpx.Client(timeout=10.0, trust_env=False) as client:
            response = client.get(proxy_url(proxy, "/bytes?n=2000"))
        assert response.status_code == 200
        assert response.content == b"x" * 2000
        wait_until(lambda: proxy.active_connections == 0)
        assert fired(proxy)
