from __future__ import annotations

import random
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias

if TYPE_CHECKING:
    from .faults import Fault

Side: TypeAlias = Literal["client", "server", "both"]
StreamSide: TypeAlias = Literal["client", "server"]


@dataclass(frozen=True, slots=True)
class Between:
    low: int
    high: int

    def __post_init__(self) -> None:
        if self.low < 0 or self.high < self.low:
            raise ValueError("Between requires 0 <= low <= high")

    def draw(self, rng: random.Random) -> int:
        return rng.randint(self.low, self.high)


@dataclass(frozen=True, slots=True)
class Nth:
    index: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError("Nth index must be non-negative")

    def matches(self, conn_index: int) -> bool:
        return conn_index == self.index


@dataclass(frozen=True, slots=True)
class Every:
    def matches(self, conn_index: int) -> bool:
        return True


ALL = Every()
ConnSelector: TypeAlias = Nth | Every


@dataclass(frozen=True, slots=True)
class Saw:
    pattern: bytes
    side: StreamSide
    delta: int | Between = 0

    def __post_init__(self) -> None:
        if not isinstance(self.pattern, bytes) or not self.pattern:
            raise ValueError("Saw pattern must be non-empty bytes")
        if self.side not in ("client", "server"):
            raise ValueError("Saw side must be 'client' or 'server'")
        if isinstance(self.delta, Between):
            if self.delta.low < 0:
                raise ValueError("Saw delta must be non-negative")
        elif not isinstance(self.delta, int) or self.delta < 0:
            raise ValueError("Saw delta must be a non-negative int or Between")


def between(low: int, high: int) -> Between:
    return Between(low, high)


def nth(index: int) -> Nth:
    return Nth(index)


def every() -> Every:
    return Every()


def saw(
    pattern: bytes,
    *,
    side: StreamSide,
    delta: int | Between = 0,
) -> Saw:
    return Saw(pattern, side, delta)


@dataclass(frozen=True, slots=True, init=False)
class Plan:
    faults: tuple[Fault, ...]

    def __init__(self, *faults: Fault) -> None:
        from .faults import FAULT_TYPES

        if not all(isinstance(fault, FAULT_TYPES) for fault in faults):
            raise TypeError("Plan() expected Fault instances")
        object.__setattr__(self, "faults", tuple(faults))

    def __iter__(self) -> Iterator[Fault]:
        return iter(self.faults)

    @classmethod
    def of(cls, value: Plan | Fault | Iterable[Fault]) -> Plan:
        from .faults import FAULT_TYPES

        if isinstance(value, Plan):
            return value
        if isinstance(value, FAULT_TYPES):
            return cls(value)
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            return cls(*value)
        raise TypeError("Plan.of() expected Plan, Fault, or iterable of Fault")
