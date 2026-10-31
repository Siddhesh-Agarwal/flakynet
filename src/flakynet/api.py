from __future__ import annotations

from typing import TYPE_CHECKING

from ._proxy import Proxy

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .faults import Fault
    from .plan import Plan


def proxy(
    target: tuple[str, int],
    plan: Plan | Fault | Iterable[Fault] = (),
    *,
    seed: int | str = 0,
    name: str | None = None,
    host: str = "127.0.0.1",
) -> Proxy:
    """Return a :class:`Proxy` without binding a socket.

    ``plan`` is normalized with :meth:`Plan.of`, so a single fault, an
    iterable of faults, or a ``Plan`` are all accepted. Construction does not
    bind; call :meth:`Proxy.start` or use the returned object as a context
    manager. ``events``, ``canonical_events()``, and ``fault_report()`` remain
    readable after ``stop()``.

    ``seed`` defaults to ``0``. ``name=None`` uses the stable ``"p0"`` default;
    pass distinct names when several proxies share a test or process.
    """
    return Proxy(target, plan, seed=seed, name=name, host=host)
