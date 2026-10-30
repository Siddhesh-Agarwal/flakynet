from __future__ import annotations

import re

import pytest

ECHO_CONFTEST = """
import socketserver
import threading

import pytest


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        while True:
            data = self.request.recv(65536)
            if not data:
                break
            self.request.sendall(data)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    block_on_close = False


@pytest.fixture
def echo():
    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
"""


def test_plugin_replay_reproduces_canonical_events(pytester: pytest.Pytester) -> None:
    pytester.makeconftest(ECHO_CONFTEST)
    pytester.makepyfile(
        """
        import contextlib
        import socket

        import flakynet as fn


        def test_replay(flakynet, echo):
            p = flakynet.proxy(
                echo,
                fn.CloseAfter(after_bytes=fn.between(0, 30), side="server"),
            )
            with socket.create_connection((p.host, p.port), timeout=5.0) as sock:
                with contextlib.suppress(OSError):
                    sock.sendall(b"x" * 100)
                while True:
                    try:
                        chunk = sock.recv(65536)
                    except OSError:
                        break
                    if not chunk:
                        break
            print("EVENTS", p.canonical_events())
            assert False
        """
    )

    first = pytester.runpytest("--flakynet-seed=7")
    first.assert_outcomes(failed=1)
    first_output = first.stdout.str()
    assert "seed=7 (not pinned, from --flakynet-seed)" in first_output
    assert "replay: pytest" in first_output
    first_events = re.search(r"EVENTS (.*)", first_output)
    assert first_events is not None

    second = pytester.runpytest("--flakynet-seed=7")
    second.assert_outcomes(failed=1)
    second_events = re.search(r"EVENTS (.*)", second.stdout.str())
    assert second_events is not None
    assert first_events.group(1) == second_events.group(1)


def test_plugin_sweep_reports_first_failing_seed(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        """
        def test_sweep(flakynet):
            p = flakynet.proxy(("127.0.0.1", 1))
            assert p.seed != 2, f"failed at {p.seed}"
        """
    )

    result = pytester.runpytest("--flakynet-sweep=5")
    result.assert_outcomes(passed=2, failed=1, skipped=2)
    output = result.stdout.str()
    assert "sweep seed: 2" in output
    assert "seed=2 (not pinned, from --flakynet-sweep)" in output
    assert "first failing seed 2" in output


def test_plugin_reports_pinned_seed(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        """
        def test_pinned(flakynet):
            p = flakynet.proxy(("127.0.0.1", 1), seed=7)
            assert p.seed == 7
            assert False
        """
    )

    result = pytester.runpytest("--flakynet-seed=99")
    result.assert_outcomes(failed=1)
    output = result.stdout.str()
    assert "p0: seed=7 (pinned by test)" in output
    assert "--flakynet-seed=7" in output


def test_plugin_names_proxies_in_creation_order(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        """
        import flakynet as fn


        def test_two_proxies(flakynet):
            plan = fn.Plan(fn.Reset(after_bytes=fn.between(0, 10**9)))
            first = flakynet.proxy(("127.0.0.1", 1), plan, seed=5)
            second = flakynet.proxy(("127.0.0.1", 1), plan, seed=5)
            assert (first.name, second.name) == ("p0", "p1")
            left = fn.resolve(plan, 5, first.name, 0).faults[0].start
            right = fn.resolve(plan, 5, second.name, 0).faults[0].start
            assert left != right
        """
    )

    result = pytester.runpytest()
    result.assert_outcomes(passed=1)


def test_plugin_seed_precedence_default_and_cli(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        """
        def test_seed(flakynet):
            p = flakynet.proxy(("127.0.0.1", 1))
            print("SEED", p.seed)
        """
    )

    default = pytester.runpytest("-s")
    default.assert_outcomes(passed=1)
    assert "SEED 0" in default.stdout.str()

    cli = pytester.runpytest("-s", "--flakynet-seed=11")
    cli.assert_outcomes(passed=1)
    assert "SEED 11" in cli.stdout.str()
