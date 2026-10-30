from __future__ import annotations

import pytest
from support import fired, running_proxy, wait_until

from flakynet import Fragment, Plan, Reset

psycopg = pytest.importorskip("psycopg")

pytestmark = pytest.mark.integration


def postgres_target(dsn: str) -> tuple[str, int]:
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    return (str(info.get("host", "127.0.0.1")), int(info.get("port", 5432)))


def proxied_dsn(proxy, dsn: str) -> str:
    return psycopg.conninfo.make_conninfo(
        dsn,
        host=proxy.host,
        port=proxy.port,
    )


def test_postgres_reset_mid_query(postgres_dsn: str) -> None:
    plan = Plan(Reset(after_bytes=8192, side="server"))
    with running_proxy(postgres_target(postgres_dsn), plan) as proxy:
        with (
            pytest.raises(psycopg.Error),
            psycopg.connect(
                proxied_dsn(proxy, postgres_dsn),
                connect_timeout=3,
            ) as conn,
        ):
            conn.execute("SELECT repeat('x', 200000)").fetchall()
        wait_until(lambda: proxy.active_connections == 0)
        assert fired(proxy)


def test_postgres_fragment_one_byte(postgres_dsn: str) -> None:
    plan = Plan(Fragment(max_bytes=1, side="server"))
    with running_proxy(postgres_target(postgres_dsn), plan) as proxy:
        with psycopg.connect(
            proxied_dsn(proxy, postgres_dsn),
            connect_timeout=3,
        ) as conn:
            row = conn.execute("SELECT repeat('x', 2000)").fetchone()
        assert row == ("x" * 2000,)
        wait_until(lambda: proxy.active_connections == 0)
        assert fired(proxy)
