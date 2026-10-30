from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TypeAlias

from .events import Event
from .faults import Fault
from .plan import Plan, StreamSide

FIRED = "FIRED"
PREEMPTED = "PREEMPTED"
ENDED_EARLY = "ENDED_EARLY"
MISSED = "MISSED"
PENDING = "PENDING"
UNMATCHED = "UNMATCHED"

FaultState: TypeAlias = str
DEFAULT_EXCUSED = frozenset({PREEMPTED, ENDED_EARLY})


@dataclass(frozen=True, slots=True)
class ConnectionSnapshot:
    """A point-in-time copy of one connection's report inputs."""

    conn_index: int
    bytes_client_to_server: int
    bytes_server_to_client: int
    client_end: str
    server_end: str
    end_reason: str
    terminal_fired: bool


@dataclass(frozen=True, slots=True)
class FaultStatus:
    """The outcome of one fault on one accepted connection.

    ``conn_index`` is ``None`` for a fault-level ``UNMATCHED`` entry, meaning
    no accepted connection matched the fault's ``conn`` selector.
    """

    fault_index: int
    fault: str
    fault_repr: str
    conn_index: int | None
    status: FaultState
    bytes_client_to_server: int
    bytes_server_to_client: int
    ended: str

    def describe(self) -> str:
        """Return one readable report line without repeating the fault repr."""
        if self.conn_index is None:
            return f"conn -: {self.status} (no accepted connection matched)"
        return (
            f"conn {self.conn_index}: {self.status} "
            f"(client={self.bytes_client_to_server}, "
            f"server={self.bytes_server_to_client}, ended={self.ended})"
        )


def _target_streams(fault: Fault) -> tuple[StreamSide, ...]:
    side = getattr(fault, "side", "both")
    if side == "client":
        return ("client",)
    if side == "server":
        return ("server",)
    return ("client", "server")


def _stream_end(connection: ConnectionSnapshot, stream: StreamSide) -> str:
    return connection.client_end if stream == "client" else connection.server_end


def _status_for(
    fault: Fault,
    fault_index: int,
    connection: ConnectionSnapshot,
    fired: set[tuple[int, int]],
) -> FaultState:
    if (connection.conn_index, fault_index) in fired:
        return FIRED
    if connection.end_reason == "pending":
        return PENDING

    after = getattr(fault, "after", None)
    if after is not None:
        relevant_ends = {_stream_end(connection, after.side)}
    else:
        relevant_ends = {_stream_end(connection, stream) for stream in _target_streams(fault)}
    if "graceful" in relevant_ends:
        return MISSED
    if connection.terminal_fired:
        return PREEMPTED
    if connection.end_reason in {"client_abort", "server_abort", "error"}:
        return ENDED_EARLY
    return MISSED


def build_report(
    plan: Plan,
    events: Iterable[Event],
    connections: Iterable[ConnectionSnapshot],
) -> tuple[FaultStatus, ...]:
    """Build the per-(fault, connection) report."""
    fired = {
        (event.conn_index, event.fault_index)
        for event in events
        if event.kind in {"fired", "window_entered"}
    }
    snapshots = sorted(connections, key=lambda item: item.conn_index)
    statuses: list[FaultStatus] = []

    for fault_index, fault in enumerate(plan):
        matched = [item for item in snapshots if fault.conn.matches(item.conn_index)]
        if not matched:
            statuses.append(
                FaultStatus(
                    fault_index=fault_index,
                    fault=type(fault).__name__,
                    fault_repr=repr(fault),
                    conn_index=None,
                    status=UNMATCHED,
                    bytes_client_to_server=0,
                    bytes_server_to_client=0,
                    ended="not accepted",
                )
            )
            continue

        for connection in matched:
            statuses.append(
                FaultStatus(
                    fault_index=fault_index,
                    fault=type(fault).__name__,
                    fault_repr=repr(fault),
                    conn_index=connection.conn_index,
                    status=_status_for(fault, fault_index, connection, fired),
                    bytes_client_to_server=connection.bytes_client_to_server,
                    bytes_server_to_client=connection.bytes_server_to_client,
                    ended=connection.end_reason,
                )
            )

    return tuple(statuses)


def all_fired_failure(
    report: Iterable[FaultStatus],
    allow: Iterable[str] = frozenset(),
) -> str | None:
    """Return a readable failure message, or ``None`` when all faults pass."""
    allowed = {allow.upper()} if isinstance(allow, str) else {str(item).upper() for item in allow}
    excused = DEFAULT_EXCUSED | allowed

    by_fault: dict[int, list[FaultStatus]] = {}
    for status in report:
        by_fault.setdefault(status.fault_index, []).append(status)

    failures: list[list[FaultStatus]] = []
    for fault_index in sorted(by_fault):
        entries = by_fault[fault_index]
        if any(entry.status == FIRED for entry in entries):
            continue
        if all(entry.status in excused for entry in entries):
            continue
        failures.append(entries)

    if not failures:
        return None

    lines = ["assert_all_fired() failed: fault(s) did not fire and were not excused."]
    for entries in failures:
        lines.append(f"- {entries[0].fault_repr}")
        lines.extend(f"    {entry.describe()}" for entry in entries)
    lines.append(
        "Excused by default: PREEMPTED, ENDED_EARLY. "
        "Pass allow={'MISSED', 'PENDING', 'UNMATCHED'} to opt out per status."
    )
    return "\n".join(lines)
