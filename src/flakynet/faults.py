from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from .plan import ALL, Between, ConnSelector, Every, Nth, Saw, Side

# A static window [start, end); None means the whole stream.
Window: TypeAlias = tuple[int | Between, int | Between | None] | None


def _require_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    return value


def _validate_side(side: Side) -> None:
    if side not in ("client", "server", "both"):
        raise ValueError("side must be 'client', 'server', or 'both'")


_WINDOW_ARITY = 2


def _validate_conn(conn: ConnSelector) -> None:
    if not isinstance(conn, (Nth, Every)):
        raise TypeError("conn must be nth(...) or every()")


def _validate_window(window: Window) -> None:
    if window is None:
        return
    if not isinstance(window, tuple) or len(window) != _WINDOW_ARITY:
        raise ValueError("window must be a (start, end) tuple or None")

    start, end = window
    if isinstance(start, Between):
        pass
    else:
        start = _require_int(start, "window start")
        if start < 0:
            raise ValueError("window start must be a non-negative int or Between")

    if end is None:
        return
    if isinstance(end, Between):
        return

    end = _require_int(end, "window end")
    if end < 0:
        raise ValueError("window end must be a non-negative int, Between, or None")
    if isinstance(start, int) and end < start:
        raise ValueError("window end must be greater than or equal to start")


def _validate_discrete(
    after_bytes: int | Between | None,
    after: Saw | None,
    side: Side,
    conn: ConnSelector,
) -> None:
    _validate_side(side)
    _validate_conn(conn)
    if (after_bytes is None) == (after is None):
        raise ValueError("exactly one of after_bytes or after must be set")
    if after_bytes is not None:
        if isinstance(after_bytes, Between):
            if after_bytes.low < 0:
                raise ValueError("after_bytes must be non-negative")
        else:
            value = _require_int(after_bytes, "after_bytes")
            if value < 0:
                raise ValueError("after_bytes must be a non-negative int or Between")
    if after is not None and not isinstance(after, Saw):
        raise ValueError("after must be a Saw trigger")


@dataclass(frozen=True, slots=True)
class Latency:
    """Delay each forwarded segment whose offset is inside a window.

    The window ``[start, end)`` is the trigger; the delay is pacing applied
    inside it. A forwarded segment is each piece the pump writes after
    splitting at fault boundaries, so every piece produced by ``Fragment`` is
    its own segment. The delay for the zero-based segment ordinal ``n`` of a
    stream is ``ms + randint(0, jitter_ms)`` at
    ``random.Random(f"{jitter_key}:{stream}:{n}")``, a pure function of the
    resolved key, stream, and ordinal. Segment ordinals count every forwarded
    segment of that stream, including segments outside the window.

    The delay values replay, but **which bytes receive which delay does not**:
    segment boundaries come from how the OS chunked reads. ``Latency`` logs
    only ``window_entered`` and ``window_exited`` events, never per-segment
    events, and it makes no promise about measured elapsed time.
    """

    ms: int
    jitter_ms: int = 0
    side: Side = "both"
    conn: ConnSelector = ALL
    window: Window = None

    def __post_init__(self) -> None:
        _validate_side(self.side)
        _validate_conn(self.conn)
        ms = _require_int(self.ms, "ms")
        jitter_ms = _require_int(self.jitter_ms, "jitter_ms")
        if ms < 0 or jitter_ms < 0:
            raise ValueError("ms and jitter_ms must be non-negative")
        _validate_window(self.window)


@dataclass(frozen=True, slots=True)
class Bandwidth:
    """Cap throughput in bytes per second inside a window.

    The window ``[start, end)`` is the trigger; a delay of
    ``segment_bytes / rate`` seconds is applied before each segment written
    inside it. The rate is drawn once per connection and stream at accept time
    and is part of the resolved plan. Measured throughput is not promised.
    """

    bytes_per_second: int | Between
    side: Side = "both"
    conn: ConnSelector = ALL
    window: Window = None

    def __post_init__(self) -> None:
        _validate_side(self.side)
        _validate_conn(self.conn)
        if isinstance(self.bytes_per_second, Between):
            if self.bytes_per_second.low <= 0:
                raise ValueError("bytes_per_second must be positive")
        else:
            rate = _require_int(self.bytes_per_second, "bytes_per_second")
            if rate <= 0:
                raise ValueError("bytes_per_second must be a positive int or Between")
        _validate_window(self.window)


@dataclass(frozen=True, slots=True)
class Fragment:
    """Forward data in pieces of at most ``max_bytes`` inside a window.

    Pieces are separate writes, not guaranteed separate reads; TCP may
    coalesce them before the client observes them. Each piece is its own
    forwarded segment, so each piece receives its own ``Latency`` delay.
    """

    max_bytes: int
    side: Side = "both"
    conn: ConnSelector = ALL
    window: Window = None

    def __post_init__(self) -> None:
        _validate_side(self.side)
        _validate_conn(self.conn)
        max_bytes = _require_int(self.max_bytes, "max_bytes")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be a positive int")
        _validate_window(self.window)


@dataclass(frozen=True, slots=True)
class Reset:
    """Abort the connection with TCP RST at the trigger point."""

    after_bytes: int | Between | None = None
    after: Saw | None = None
    side: Side = "both"
    conn: ConnSelector = ALL

    def __post_init__(self) -> None:
        _validate_discrete(self.after_bytes, self.after, self.side, self.conn)


@dataclass(frozen=True, slots=True)
class CloseAfter:
    """Gracefully close the connection with FIN at the trigger point."""

    after_bytes: int | Between | None = None
    after: Saw | None = None
    side: Side = "both"
    conn: ConnSelector = ALL

    def __post_init__(self) -> None:
        _validate_discrete(self.after_bytes, self.after, self.side, self.conn)


@dataclass(frozen=True, slots=True)
class Blackhole:
    """Drop the affected stream while keeping the connection open.

    The proxy drains and discards bytes at and after the trigger, so the
    sender is not blocked by TCP backpressure and the peer sees nothing after
    the prefix. This models bytes vanishing in the network, not a stalled
    receiver; use ``Bandwidth`` or ``Latency`` for pacing.
    """

    after_bytes: int | Between | None = None
    after: Saw | None = None
    side: Side = "both"
    conn: ConnSelector = ALL

    def __post_init__(self) -> None:
        _validate_discrete(self.after_bytes, self.after, self.side, self.conn)


@dataclass(frozen=True, slots=True)
class RefuseConnect:
    """Reject new connections; the proxy accepts then immediately resets them."""

    conn: ConnSelector = ALL

    def __post_init__(self) -> None:
        _validate_conn(self.conn)


Fault: TypeAlias = Latency | Bandwidth | Fragment | Reset | CloseAfter | Blackhole | RefuseConnect

FAULT_TYPES = (Latency, Bandwidth, Fragment, Reset, CloseAfter, Blackhole, RefuseConnect)
