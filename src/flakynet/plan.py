from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias

if TYPE_CHECKING:
    import random

    from .faults import Fault

Side: TypeAlias = Literal["client", "server", "both"]
StreamSide: TypeAlias = Literal["client", "server"]


def _require_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    return value


def _require_conn_index(conn_index: object) -> int:
    value = _require_int(conn_index, "conn_index")
    if value < 0:
        raise ValueError("conn_index must be non-negative")
    return value


@dataclass(frozen=True, slots=True)
class Between:
    """An inclusive uniform integer range drawn during phase-1 resolution."""

    low: int
    high: int

    def __post_init__(self) -> None:
        _require_int(self.low, "low")
        _require_int(self.high, "high")
        if self.low < 0 or self.high < self.low:
            raise ValueError("Between requires 0 <= low <= high")

    def draw(self, rng: random.Random) -> int:
        """Draw an integer in ``[low, high]`` inclusive."""
        return rng.randint(self.low, self.high)


@dataclass(frozen=True, slots=True)
class Nth:
    """Select one zero-based connection index."""

    index: int

    def __post_init__(self) -> None:
        _require_int(self.index, "index")
        if self.index < 0:
            raise ValueError("Nth index must be non-negative")

    def matches(self, conn_index: int) -> bool:
        """Return whether ``conn_index`` is this selector's index."""
        return _require_conn_index(conn_index) == self.index


@dataclass(frozen=True, slots=True)
class Every:
    """Select every accepted connection."""

    def matches(self, conn_index: int) -> bool:
        """Return True for every valid connection index."""
        _require_conn_index(conn_index)
        return True


ALL = Every()
ConnSelector: TypeAlias = Nth | Every


@dataclass(frozen=True, slots=True)
class Saw:
    """A byte pattern observed on one stream; it arms a fault at runtime."""

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
        else:
            _require_int(self.delta, "delta")
            if self.delta < 0:
                raise ValueError("Saw delta must be a non-negative int or Between")


def between(low: int, high: int) -> Between:
    """Return an inclusive uniform integer range for phase-1 resolution."""
    return Between(low, high)


def nth(index: int) -> Nth:
    """Return a selector matching the zero-based connection index."""
    return Nth(index)


def every() -> Every:
    """Return a selector matching every accepted connection."""
    return Every()


def saw(
    pattern: bytes,
    *,
    side: StreamSide,
    delta: int | Between = 0,
) -> Saw:
    """Return a trigger that arms a fault after ``pattern`` on ``side``.

    ``delta`` is pre-drawn extra bytes after the exclusive match end.
    """
    return Saw(pattern, side, delta)


@dataclass(frozen=True, slots=True, init=False)
class Plan:
    """An immutable, ordered collection of faults."""

    faults: tuple[Fault, ...]

    def __init__(self, *faults: Fault) -> None:
        from .faults import FAULT_TYPES  # noqa: PLC0415 - avoids a circular import

        if not all(isinstance(fault, FAULT_TYPES) for fault in faults):
            raise TypeError("Plan() expected Fault instances")
        object.__setattr__(self, "faults", tuple(faults))

    def __iter__(self) -> Iterator[Fault]:
        return iter(self.faults)

    @classmethod
    def of(cls, value: Plan | Fault | Iterable[Fault]) -> Plan:
        """Normalize a Plan, one fault, or an iterable of faults."""
        from .faults import FAULT_TYPES  # noqa: PLC0415 - avoids a circular import

        if isinstance(value, Plan):
            return value
        if isinstance(value, FAULT_TYPES):
            return cls(value)
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            return cls(*value)
        raise TypeError("Plan.of() expected Plan, Fault, or iterable of Fault")
