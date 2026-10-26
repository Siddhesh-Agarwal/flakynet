from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from .plan import ALL, Between, ConnSelector, Every, Nth, Saw, Side

Window: TypeAlias = tuple[int | Between, int | Between | None] | None


def _validate_side(side: Side) -> None:
    if side not in ("client", "server", "both"):
        raise ValueError("side must be 'client', 'server', or 'both'")


def _validate_conn(conn: ConnSelector) -> None:
    if not isinstance(conn, (Nth, Every)):
        raise ValueError("conn must be nth(...) or every()")


def _validate_window(window: Window) -> None:
    if window is None:
        return
    if not isinstance(window, tuple) or len(window) != 2:
        raise ValueError("window must be a (start, end) tuple or None")

    start, end = window
    if isinstance(start, Between):
        pass
    elif not isinstance(start, int) or start < 0:
        raise ValueError("window start must be a non-negative int or Between")

    if end is None:
        return
    if isinstance(end, Between):
        pass
    elif not isinstance(end, int) or end < 0:
        raise ValueError("window end must be a non-negative int, Between, or None")
    elif isinstance(start, int) and end < start:
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
        elif not isinstance(after_bytes, int) or after_bytes < 0:
            raise ValueError("after_bytes must be a non-negative int or Between")
    if after is not None and not isinstance(after, Saw):
        raise ValueError("after must be a Saw trigger")


@dataclass(frozen=True, slots=True)
class Latency:
    ms: int
    jitter_ms: int = 0
    side: Side = "both"
    conn: ConnSelector = ALL
    window: Window = None

    def __post_init__(self) -> None:
        _validate_side(self.side)
        _validate_conn(self.conn)
        if self.ms < 0 or self.jitter_ms < 0:
            raise ValueError("ms and jitter_ms must be non-negative")
        _validate_window(self.window)


@dataclass(frozen=True, slots=True)
class Bandwidth:
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
        elif not isinstance(self.bytes_per_second, int) or self.bytes_per_second <= 0:
            raise ValueError("bytes_per_second must be a positive int or Between")
        _validate_window(self.window)


@dataclass(frozen=True, slots=True)
class Fragment:
    max_bytes: int
    side: Side = "both"
    conn: ConnSelector = ALL
    window: Window = None

    def __post_init__(self) -> None:
        _validate_side(self.side)
        _validate_conn(self.conn)
        if not isinstance(self.max_bytes, int) or self.max_bytes <= 0:
            raise ValueError("max_bytes must be a positive int")
        _validate_window(self.window)


@dataclass(frozen=True, slots=True)
class Reset:
    after_bytes: int | Between | None = None
    after: Saw | None = None
    side: Side = "both"
    conn: ConnSelector = ALL

    def __post_init__(self) -> None:
        _validate_discrete(self.after_bytes, self.after, self.side, self.conn)


@dataclass(frozen=True, slots=True)
class CloseAfter:
    after_bytes: int | Between | None = None
    after: Saw | None = None
    side: Side = "both"
    conn: ConnSelector = ALL

    def __post_init__(self) -> None:
        _validate_discrete(self.after_bytes, self.after, self.side, self.conn)


@dataclass(frozen=True, slots=True)
class Blackhole:
    after_bytes: int | Between | None = None
    after: Saw | None = None
    side: Side = "both"
    conn: ConnSelector = ALL

    def __post_init__(self) -> None:
        _validate_discrete(self.after_bytes, self.after, self.side, self.conn)


@dataclass(frozen=True, slots=True)
class RefuseConnect:
    conn: ConnSelector = ALL

    def __post_init__(self) -> None:
        _validate_conn(self.conn)


Fault: TypeAlias = Latency | Bandwidth | Fragment | Reset | CloseAfter | Blackhole | RefuseConnect

FAULT_TYPES = (Latency, Bandwidth, Fragment, Reset, CloseAfter, Blackhole, RefuseConnect)
