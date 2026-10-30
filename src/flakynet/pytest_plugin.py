# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

import shlex
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pytest

from .api import proxy as make_proxy
from .faults import Fault
from .plan import Plan
from .proxy import Proxy


@dataclass(frozen=True, slots=True)
class _ProxyRecord:
    proxy: Proxy
    seed: int | str
    pinned: bool
    source: str


class FlakynetFactory:
    """The ``flakynet`` pytest fixture: a factory for started proxies.

    Each ``.proxy(...)`` call starts the proxy immediately and registers its
    ``stop()`` as a test finalizer. Pytest runs finalizers LIFO, so proxies
    stop in reverse creation order. Unnamed proxies are called ``p0``,
    ``p1``, ... in creation order.
    """

    def __init__(self, request: pytest.FixtureRequest) -> None:
        self._request = request
        self._config = request.config
        sweep_seed = getattr(request, "param", None)
        self._sweep_seed = sweep_seed if isinstance(sweep_seed, int) else None
        self._created = 0
        self.proxies: list[_ProxyRecord] = []
        request.node.stash[FLAKYNET_FACTORY_KEY] = self

    def proxy(
        self,
        target: tuple[str, int],
        plan: Plan | Fault | Iterable[Fault] = (),
        *,
        seed: int | str | None = None,
        name: str | None = None,
        host: str = "127.0.0.1",
    ) -> Proxy:
        """Create, start, and register one proxy for test teardown."""
        effective, pinned, source = self._resolve_seed(seed)
        index = self._created
        self._created += 1
        proxy_name = name if name is not None else f"p{index}"

        created = make_proxy(
            target,
            plan,
            seed=effective,
            name=proxy_name,
            host=host,
        )
        created.start()
        self._request.addfinalizer(created.stop)
        self.proxies.append(
            _ProxyRecord(
                proxy=created,
                seed=effective,
                pinned=pinned,
                source=source,
            )
        )
        return created

    def _resolve_seed(self, explicit: int | str | None) -> tuple[int | str, bool, str]:
        if explicit is not None:
            return explicit, True, "explicit"
        cli_seed = self._config.getoption("flakynet_seed")
        if cli_seed is not None:
            return _parse_seed(cli_seed), False, "--flakynet-seed"
        if self._sweep_seed is not None:
            return self._sweep_seed, False, "--flakynet-sweep"
        return 0, False, "default"

    def failure_text(self, item: pytest.Item) -> str:
        """Build the failure section for every proxy created by this test."""
        base = _base_nodeid(item)
        lines = ["flakynet proxies:"]
        if self._sweep_seed is not None:
            lines.append(f"  sweep seed: {self._sweep_seed}")
        for record in self.proxies:
            provenance = "pinned by test" if record.pinned else f"not pinned, from {record.source}"
            lines.append(
                f"  {record.proxy.name}: seed={record.seed} ({provenance}), "
                f"plan={record.proxy.plan!r}"
            )
            lines.append(
                f"    replay: pytest {shlex.quote(base)} "
                f"--flakynet-seed={shlex.quote(str(record.seed))}"
            )
        return "\n".join(lines)


FLAKYNET_FACTORY_KEY: pytest.StashKey[FlakynetFactory] = pytest.StashKey()
SWEEP_FAILURES_KEY: pytest.StashKey[dict[str, int]] = pytest.StashKey()


def _parse_seed(value: object) -> int | str:
    if isinstance(value, int):
        return value
    text = str(value)
    try:
        return int(text)
    except ValueError:
        return text


def _sweep_seed_for_item(item: pytest.Item) -> int | None:
    callspec = getattr(item, "callspec", None)
    if callspec is None:
        return None
    value = callspec.params.get("flakynet")
    return value if isinstance(value, int) else None


def _base_nodeid(item: pytest.Item) -> str:
    callspec = getattr(item, "callspec", None)
    if callspec is not None and "flakynet" in callspec.params:
        return item.nodeid.split("[", 1)[0]
    return item.nodeid


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("flakynet")
    group.addoption(
        "--flakynet-seed",
        action="store",
        dest="flakynet_seed",
        default=None,
        metavar="SEED",
        help="seed for flakynet proxies (overridden by an explicit seed= argument)",
    )
    group.addoption(
        "--flakynet-sweep",
        action="store",
        type=int,
        dest="flakynet_sweep",
        default=None,
        metavar="N",
        help="try seeds 0..N-1 and stop at the first failing seed (ignored with --flakynet-seed)",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.stash[SWEEP_FAILURES_KEY] = {}


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    sweep = metafunc.config.getoption("flakynet_sweep")
    if not sweep or sweep <= 0:
        return
    if metafunc.config.getoption("flakynet_seed") is not None:
        return
    if "flakynet" not in metafunc.fixturenames:
        return
    metafunc.parametrize("flakynet", list(range(sweep)), indirect=True)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    sweep_seed = _sweep_seed_for_item(item)
    if sweep_seed is None:
        return
    failures = item.config.stash.get(SWEEP_FAILURES_KEY, {})
    base = _base_nodeid(item)
    if base in failures:
        pytest.skip(f"flakynet sweep stopped: first failing seed {failures[base]}")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item,
    call: pytest.CallInfo[None],
) -> Any:
    outcome = yield
    report = outcome.get_result()
    if not report.failed:
        return

    sweep_seed = _sweep_seed_for_item(item)
    if sweep_seed is not None:
        failures = item.config.stash.setdefault(SWEEP_FAILURES_KEY, {})
        failures.setdefault(_base_nodeid(item), sweep_seed)

    factory = item.stash.get(FLAKYNET_FACTORY_KEY, None)
    if factory is None or not factory.proxies:
        return
    report.sections.append(("flakynet", factory.failure_text(item)))


def pytest_terminal_summary(
    terminalreporter: Any,
    exitstatus: int,
    config: pytest.Config,
) -> None:
    failures = config.stash.get(SWEEP_FAILURES_KEY, {})
    if not failures:
        return
    terminalreporter.write_sep("=", "flakynet sweep")
    for nodeid, seed in failures.items():
        terminalreporter.write_line(f"{nodeid}: first failing seed {seed}")


@pytest.fixture
def flakynet(request: pytest.FixtureRequest) -> FlakynetFactory:
    """Factory for started proxies; stops them LIFO at test teardown."""
    return FlakynetFactory(request)
