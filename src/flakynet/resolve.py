from __future__ import annotations

import random
from dataclasses import dataclass
from typing import TypeAlias

from .faults import (
    Bandwidth,
    Fault,
    Fragment,
    Latency,
    RefuseConnect,
    Window,
)
from .plan import Between, Plan, Saw, Side, StreamSide


def _validate_conn_index(conn_index: object) -> int:
    if isinstance(conn_index, bool) or not isinstance(conn_index, int):
        raise TypeError("conn_index must be an int")
    if conn_index < 0:
        raise ValueError("conn_index must be non-negative")
    return conn_index


def rng_key(seed: int | str, proxy_name: str, conn_index: int) -> str:
    """Return the stable per-connection RNG key ``seed:proxy_name:conn_index``."""
    return f"{seed}:{proxy_name}:{conn_index}"


@dataclass(frozen=True, slots=True)
class LatencyParams:
    """Resolved latency parameters.

    ``jitter_key`` seeds the per-segment delay sequence. ``ms`` and
    ``jitter_ms`` are copied from the fault so the delay is a pure function of
    these parameters, the stream, and the segment ordinal.
    """

    jitter_key: int
    ms: int
    jitter_ms: int


def latency_delay_ms(
    params: LatencyParams,
    stream: StreamSide,
    ordinal: int,
) -> int:
    """Return the planned delay for segment ``ordinal`` of ``stream``.

    The delay is ``ms + randint(0, jitter_ms)`` at
    ``random.Random(f"{jitter_key}:{stream}:{ordinal}")``. ``ordinal`` is
    zero-based and counts every forwarded segment of that stream, including
    segments outside a latency window. It is a pure function of the resolved
    key, stream, and ordinal; no shared RNG state is consumed at runtime.

    Which application bytes share a segment depends on how the OS chunked the
    reads, so the delay values replay but their placement on the byte stream
    does not.
    """
    if isinstance(ordinal, bool) or not isinstance(ordinal, int):
        raise TypeError("ordinal must be an int")
    if ordinal < 0:
        raise ValueError("ordinal must be non-negative")
    if stream not in ("client", "server"):
        raise ValueError("stream must be 'client' or 'server'")
    rng = random.Random(f"{params.jitter_key}:{stream}:{ordinal}")
    return params.ms + rng.randint(0, params.jitter_ms)


@dataclass(frozen=True, slots=True)
class BandwidthParams:
    """Resolved bandwidth rate in bytes per second."""

    bytes_per_second: int


@dataclass(frozen=True, slots=True)
class FragmentParams:
    """Resolved fragment size cap in bytes."""

    max_bytes: int


ResolvedParams: TypeAlias = LatencyParams | BandwidthParams | FragmentParams | None


@dataclass(frozen=True, slots=True)
class StaticFault:
    """A fault anchored to absolute stream offsets at accept time."""

    fault_index: int
    fault: Fault
    stream: StreamSide | None
    start: int
    end: int | None
    params: ResolvedParams


@dataclass(frozen=True, slots=True)
class ArmedFault:
    """A fault whose start awaits a ``saw`` match at runtime."""

    fault_index: int
    fault: Fault
    stream: StreamSide
    trigger: Saw
    delta: int
    start: int | None = None
    end: int | None = None
    params: ResolvedParams = None


@dataclass(frozen=True, slots=True)
class ConnectionPlan:
    """Phase-1 resolution output for one connection.

    ``faults`` preserves plan order and is expanded per affected stream.
    """

    seed: int | str
    proxy_name: str
    conn_index: int
    rng_key: str
    faults: tuple[StaticFault | ArmedFault, ...]


def _sides(side: Side) -> tuple[StreamSide, ...]:
    if side == "both":
        return ("client", "server")
    return (side,)


def _draw_offset(value: int | Between, rng: random.Random) -> int:
    return value.draw(rng) if isinstance(value, Between) else value


def _draw_positive(value: int | Between, rng: random.Random) -> int:
    result = _draw_offset(value, rng)
    if result <= 0:
        raise ValueError("resolved value must be positive")
    return result


def _resolve_window(window: Window, rng: random.Random) -> tuple[int, int | None]:
    if window is None:
        return 0, None
    start = _draw_offset(window[0], rng)
    end = None if window[1] is None else _draw_offset(window[1], rng)
    if end is not None and end < start:
        raise ValueError("window end must be greater than or equal to start")
    return start, end


def resolve(
    plan: Plan,
    seed: int | str,
    proxy_name: str,
    conn_index: int,
) -> ConnectionPlan:
    """Resolve ``plan`` for one connection.

    This is pure and socket-free: every random draw happens here, keyed by
    ``(seed, proxy_name, conn_index)``. Faults are emitted in plan order and
    expanded per affected stream. ``conn_index`` is zero-based.
    """
    conn_index = _validate_conn_index(conn_index)

    key = rng_key(seed, proxy_name, conn_index)
    rng = random.Random(key)
    resolved: list[StaticFault | ArmedFault] = []

    for fault_index, fault in enumerate(plan):
        if not fault.conn.matches(conn_index):
            continue

        if isinstance(fault, RefuseConnect):
            resolved.append(StaticFault(fault_index, fault, None, 0, 0, None))
            continue

        jitter_key: int | None = None
        if isinstance(fault, Latency):
            jitter_key = rng.getrandbits(64)

        for side in _sides(fault.side):
            if isinstance(fault, Latency):
                if jitter_key is None:
                    raise RuntimeError("latency jitter key was not drawn")
                start, end = _resolve_window(fault.window, rng)
                params: ResolvedParams = LatencyParams(
                    jitter_key=jitter_key,
                    ms=fault.ms,
                    jitter_ms=fault.jitter_ms,
                )
            elif isinstance(fault, Bandwidth):
                start, end = _resolve_window(fault.window, rng)
                params = BandwidthParams(_draw_positive(fault.bytes_per_second, rng))
            elif isinstance(fault, Fragment):
                start, end = _resolve_window(fault.window, rng)
                params = FragmentParams(fault.max_bytes)
            else:
                if fault.after is not None:
                    delta = _draw_offset(fault.after.delta, rng)
                    resolved.append(
                        ArmedFault(
                            fault_index=fault_index,
                            fault=fault,
                            stream=side,
                            trigger=fault.after,
                            delta=delta,
                        ),
                    )
                    continue
                if fault.after_bytes is None:
                    raise RuntimeError("discrete fault has no trigger")
                start = _draw_offset(fault.after_bytes, rng)
                end = start
                params = None

            resolved.append(StaticFault(fault_index, fault, side, start, end, params))

    return ConnectionPlan(seed, proxy_name, conn_index, key, tuple(resolved))
