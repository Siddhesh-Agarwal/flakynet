from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from .plan import StreamSide
from .resolve import ResolvedParams

EventKind: TypeAlias = str
"""The kind of runtime observation.

Current kinds are ``"window_entered"``, ``"window_exited"``, and ``"fired"``.
``Latency`` emits only the first two; it never emits per-segment events.
"""


@dataclass(frozen=True, slots=True)
class Event:
    """One runtime observation in arrival order.

    ``seq`` increases per proxy run and ``timestamp`` is
    ``time.monotonic()`` at arrival. Neither is stable between runs, so do not
    compare events directly; compare :meth:`Proxy.canonical_events` instead.

    ``offset`` is the stream offset the event refers to. ``window_entered``
    records the offset of the first byte forwarded inside the window;
    ``window_exited`` records the exclusive window end, or the stream length
    when the window was open-ended or the stream ended inside it. ``fired``
    records the trigger offset of a discrete fault. ``stream`` is ``None`` for
    connection-level faults such as ``RefuseConnect``.

    ``fault`` is the fault class name and ``params`` is the resolved phase-1
    parameter object (or ``None`` for faults without parameters). For a
    cross-stream armed fault, ``offset`` is the actual offset and
    ``offset_exact`` is ``False``.
    """

    conn_index: int
    stream: StreamSide | None
    offset: int | None
    offset_exact: bool
    fault_index: int
    fault: str
    kind: EventKind
    params: ResolvedParams
    seq: int
    timestamp: float


@dataclass(frozen=True, slots=True)
class CanonicalEvent:
    """A replay-stable view of :class:`Event`.

    Arrival order and timestamps are removed, and ``offset`` is ``None`` when
    the original event had ``offset_exact=False``. Sort order is
    ``(conn_index, stream, offset, fault_index, kind)`` with ``None`` sorting
    before real values.
    """

    conn_index: int
    stream: StreamSide | None
    offset: int | None
    fault_index: int
    fault: str
    kind: EventKind
    params: ResolvedParams

    @property
    def sort_key(self) -> tuple[int, str, int, int, str]:
        """Return the total-order key used by ``canonical_events()``."""
        return (
            self.conn_index,
            "" if self.stream is None else self.stream,
            -1 if self.offset is None else self.offset,
            self.fault_index,
            self.kind,
        )
