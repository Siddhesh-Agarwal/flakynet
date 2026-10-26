from __future__ import annotations

import random
from dataclasses import dataclass
from typing import TypeAlias, cast

from .faults import (
    Bandwidth,
    Fault,
    Fragment,
    Latency,
    RefuseConnect,
    Window,
)
from .plan import Between, Plan, Saw, Side, StreamSide


def rng_key(seed: int | str, proxy_name: str, conn_index: int) -> str:
    return f"{seed}:{proxy_name}:{conn_index}"


@dataclass(frozen=True, slots=True)
class LatencyParams:
    jitter_key: int


@dataclass(frozen=True, slots=True)
class BandwidthParams:
    bytes_per_second: int


@dataclass(frozen=True, slots=True)
class FragmentParams:
    max_bytes: int


ResolvedParams: TypeAlias = LatencyParams | BandwidthParams | FragmentParams | None


@dataclass(frozen=True, slots=True)
class StaticFault:
    fault_index: int
    fault: Fault
    stream: StreamSide | None
    start: int
    end: int | None
    params: ResolvedParams


@dataclass(frozen=True, slots=True)
class ArmedFault:
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
    seed: int | str
    proxy_name: str
    conn_index: int
    rng_key: str
    faults: tuple[StaticFault | ArmedFault, ...]


def _sides(side: Side) -> tuple[StreamSide, ...]:
    if side == "both":
        return ("client", "server")
    return (cast(StreamSide, side),)


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
    if conn_index < 0:
        raise ValueError("conn_index must be non-negative")

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
                assert jitter_key is not None
                start, end = _resolve_window(fault.window, rng)
                params: ResolvedParams = LatencyParams(jitter_key)
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
                        )
                    )
                    continue
                assert fault.after_bytes is not None
                start = _draw_offset(fault.after_bytes, rng)
                end = start
                params = None

            resolved.append(
                StaticFault(fault_index, fault, side, start, end, params)
            )

    return ConnectionPlan(seed, proxy_name, conn_index, key, tuple(resolved))
