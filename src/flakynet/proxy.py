from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import struct
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Literal

from .events import CanonicalEvent, Event, EventKind
from .faults import (
    Bandwidth,
    Blackhole,
    CloseAfter,
    Fragment,
    Latency,
    RefuseConnect,
    Reset,
)
from .matcher import Matcher
from .plan import Plan, StreamSide
from .report import (
    ConnectionSnapshot,
    FaultStatus,
    all_fired_failure,
    build_report,
)
from .resolve import (
    ArmedFault,
    BandwidthParams,
    ConnectionPlan,
    FragmentParams,
    LatencyParams,
    ResolvedParams,
    StaticFault,
    latency_delay_ms,
    resolve,
)

if TYPE_CHECKING:
    from .faults import Fault

logger = logging.getLogger(__name__)

_Direction = Literal["client_to_server", "server_to_client"]
_TerminalAction = Literal["close", "reset"]


class _TerminalFault(Exception):
    """Internal signal that a pump applied a terminal fault."""


def _close_socket(sock: socket.socket) -> None:
    with contextlib.suppress(OSError):
        sock.close()


def _reset_socket(sock: socket.socket) -> None:
    """Abort ``sock`` with RST by setting ``SO_LINGER (1, 0)`` then closing."""
    linger = struct.pack("ii", 1, 0)
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
    _close_socket(sock)


def _graceful_close_sockets(first: socket.socket, second: socket.socket) -> None:
    for sock in (first, second):
        with contextlib.suppress(OSError):
            sock.shutdown(socket.SHUT_WR)
    _close_socket(first)
    _close_socket(second)


def _reset_sockets(first: socket.socket, second: socket.socket) -> None:
    _reset_socket(first)
    _reset_socket(second)


def _terminal_action(fault: Fault) -> _TerminalAction:
    return "reset" if isinstance(fault, Reset) else "close"


def _action_for(fault: StaticFault) -> _TerminalAction:
    return _terminal_action(fault.fault)


def _anchored(armed: ArmedFault, start: int) -> StaticFault:
    """Turn an armed discrete fault into a static one anchored at ``start``."""
    return StaticFault(
        fault_index=armed.fault_index,
        fault=armed.fault,
        stream=armed.stream,
        start=start,
        end=start,
        params=armed.params,
    )


async def _drain(
    loop: asyncio.AbstractEventLoop,
    source: socket.socket,
    on_chunk: Callable[[bytes], None] | None = None,
) -> None:
    """Read and discard until EOF without writing anything to the peer."""
    while True:
        data = await loop.sock_recv(source, 65536)
        if not data:
            return
        if on_chunk is not None:
            on_chunk(data)


def _window_faults(
    faults: tuple[StaticFault | ArmedFault, ...],
    stream: StreamSide,
) -> tuple[StaticFault, ...]:
    applicable = (
        fault
        for fault in faults
        if isinstance(fault, StaticFault)
        and fault.stream == stream
        and isinstance(fault.fault, (Latency, Bandwidth, Fragment))
    )
    return tuple(sorted(applicable, key=lambda fault: (fault.start, fault.fault_index)))


def _discrete_faults(
    faults: tuple[StaticFault | ArmedFault, ...],
    stream: StreamSide,
) -> tuple[StaticFault, ...]:
    applicable = (
        fault
        for fault in faults
        if isinstance(fault, StaticFault)
        and fault.stream == stream
        and isinstance(fault.fault, (Reset, CloseAfter, Blackhole))
    )
    return tuple(sorted(applicable, key=lambda fault: (fault.start, fault.fault_index)))


def _in_window(fault: StaticFault, offset: int) -> bool:
    return fault.start <= offset and (fault.end is None or offset < fault.end)


def _next_boundary(
    windows: tuple[StaticFault, ...],
    discrete: Sequence[StaticFault],
    fired: set[int],
    offset: int,
) -> int | None:
    """Return the next offset at which forwarding must stop and re-evaluate."""
    candidates: list[int] = []
    for fault in windows:
        if offset < fault.start:
            candidates.append(fault.start)
        elif fault.end is not None and offset < fault.end:
            candidates.append(fault.end)
    for fault in discrete:
        if fault.fault_index not in fired and offset < fault.start:
            candidates.append(fault.start)
    return min(candidates, default=None)


def _fragment_cap(windows: tuple[StaticFault, ...], offset: int) -> int | None:
    caps: list[int] = []
    for fault in windows:
        if not isinstance(fault.fault, Fragment) or not _in_window(fault, offset):
            continue
        params = fault.params
        if isinstance(params, FragmentParams):
            caps.append(params.max_bytes)
    return min(caps) if caps else None


def _latency_ms(
    windows: tuple[StaticFault, ...],
    offset: int,
    stream: StreamSide,
    ordinal: int,
) -> int:
    total = 0
    for fault in windows:
        if not isinstance(fault.fault, Latency) or not _in_window(fault, offset):
            continue
        params = fault.params
        if isinstance(params, LatencyParams):
            total += latency_delay_ms(params, stream, ordinal)
    return total


def _bandwidth_rate(windows: tuple[StaticFault, ...], offset: int) -> int | None:
    rates: list[int] = []
    for fault in windows:
        if not isinstance(fault.fault, Bandwidth) or not _in_window(fault, offset):
            continue
        params = fault.params
        if isinstance(params, BandwidthParams):
            rates.append(params.bytes_per_second)
    return min(rates) if rates else None


@dataclass(slots=True)
class _ConnectionState:
    index: int
    client_to_server: int = 0
    server_to_client: int = 0
    client_end: str = "pending"
    server_end: str = "pending"
    end_reason: str = "pending"
    terminal_fired: bool = False

    def add(self, direction: _Direction, count: int) -> None:
        if direction == "client_to_server":
            self.client_to_server += count
        else:
            self.server_to_client += count

    def set_end(self, stream: StreamSide, reason: str) -> None:
        if stream == "client":
            self.client_end = reason
        else:
            self.server_end = reason

    def finalize(self) -> None:
        if self.terminal_fired:
            self.end_reason = "flakynet"
        elif self.client_end == "aborted":
            self.end_reason = "client_abort"
        elif self.server_end == "aborted":
            self.end_reason = "server_abort"
        elif self.client_end == "error" or self.server_end == "error":
            self.end_reason = "error"
        elif self.client_end == "graceful" and self.server_end == "graceful":
            self.end_reason = "graceful"
        else:
            self.end_reason = "pending"


class Proxy:
    """A small in-process TCP proxy with seeded fault injection.

    The proxy owns an asyncio loop in a daemon thread. Callers use the
    normal ``host``/``port`` socket address and never need to run a loop.

    All seven faults are wired: ``Latency``, ``Bandwidth``, ``Fragment``,
    ``Reset``, ``CloseAfter``, ``Blackhole``, and ``RefuseConnect``. A
    ``saw`` trigger arms a discrete fault. On the same stream it anchors at
    ``M + delta``, where ``M`` is the exclusive match end. On the other stream
    it activates with the next byte forwarded there and records the actual
    offset with ``offset_exact=False``. No RNG is consumed at runtime.
    """

    def __init__(
        self,
        target: tuple[str, int],
        plan: Plan | Fault | Iterable[Fault] = (),
        *,
        seed: int | str = 0,
        host: str = "127.0.0.1",
        name: str | None = None,
    ) -> None:
        target_host, target_port = target
        if not isinstance(target_host, str) or not target_host:
            raise ValueError("target host must be a non-empty string")
        if not isinstance(target_port, int) or not (1 <= target_port <= 65535):
            raise ValueError("target port must be between 1 and 65535")
        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty string")
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError("name must be a non-empty string or None")

        self._target = (target_host, target_port)
        self._plan = Plan.of(plan)
        self._seed = seed
        self._bind_host = host
        self._name = name or "p0"

        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._listener: socket.socket | None = None
        self._stop_event: asyncio.Event | None = None
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None

        self._host: str | None = None
        self._port: int | None = None
        self._next_index = 0
        self._connection_tasks: set[asyncio.Task[None]] = set()

        self._lock = threading.Lock()
        self._active_connections = 0
        self._client_to_server = 0
        self._server_to_client = 0
        self._events: list[Event] = []
        self._event_seq = 0
        self._connection_states: dict[int, _ConnectionState] = {}

    @property
    def host(self) -> str:
        """The bound listen host, available after the first start."""
        if self._host is None:
            raise RuntimeError("proxy has not been started")
        return self._host

    @property
    def port(self) -> int:
        """The bound listen port, available after the first start."""
        if self._port is None:
            raise RuntimeError("proxy has not been started")
        return self._port

    @property
    def plan(self) -> Plan:
        """The normalized fault plan."""
        return self._plan

    @property
    def seed(self) -> int | str:
        """The seed used for per-connection resolution."""
        return self._seed

    @property
    def name(self) -> str:
        """The stable proxy name used in the resolution RNG key."""
        return self._name

    @property
    def active_connections(self) -> int:
        """Number of accepted connections that have not finished yet."""
        with self._lock:
            return self._active_connections

    @property
    def bytes_client_to_server(self) -> int:
        """Bytes forwarded from clients to the target since the last start."""
        with self._lock:
            return self._client_to_server

    @property
    def bytes_server_to_client(self) -> int:
        """Bytes forwarded from the target to clients since the last start."""
        with self._lock:
            return self._server_to_client

    @property
    def is_running(self) -> bool:
        """Whether the proxy loop thread is alive."""
        thread = self._thread
        return thread is not None and thread.is_alive()

    @property
    def events(self) -> tuple[Event, ...]:
        """Runtime observations in arrival order.

        This order and the event timestamps are for debugging and are not
        stable between runs. Use :meth:`canonical_events` for comparisons.
        Remains readable after ``stop()``.
        """
        with self._lock:
            return tuple(self._events)

    def canonical_events(self) -> tuple[CanonicalEvent, ...]:
        """Return replay-stable events, sorted and stripped of arrival data.

        Sorted by ``(conn_index, stream, offset, fault_index, kind)``.
        ``offset`` is ``None`` when the original event was not exact. Compare
        this, never :attr:`events`, across runs.
        """
        canonical = (
            CanonicalEvent(
                conn_index=event.conn_index,
                stream=event.stream,
                offset=event.offset if event.offset_exact else None,
                fault_index=event.fault_index,
                fault=event.fault,
                kind=event.kind,
                params=event.params,
            )
            for event in self.events
        )
        return tuple(sorted(canonical, key=lambda event: event.sort_key))

    def fault_report(self) -> tuple[FaultStatus, ...]:
        """Return per-(fault, connection) statuses.

        Statuses are ``FIRED``, ``PREEMPTED``, ``ENDED_EARLY``, ``MISSED``,
        ``PENDING``, and the fault-level ``UNMATCHED``. The report remains
        readable after ``stop()`` and includes each connection's forwarded
        byte counts and how it ended.
        """
        with self._lock:
            events = tuple(self._events)
            connections = tuple(
                ConnectionSnapshot(
                    conn_index=state.index,
                    bytes_client_to_server=state.client_to_server,
                    bytes_server_to_client=state.server_to_client,
                    client_end=state.client_end,
                    server_end=state.server_end,
                    end_reason=state.end_reason,
                    terminal_fired=state.terminal_fired,
                )
                for state in self._connection_states.values()
            )
        return build_report(self._plan, events, connections)

    def assert_all_fired(self, allow: Iterable[str] = frozenset()) -> None:
        """Assert that every fault fired or is excused.

        By default ``PREEMPTED`` and ``ENDED_EARLY`` are excused. Passing
        status names in ``allow`` (case-insensitive) excuses them too, for
        example ``allow={"MISSED", "UNMATCHED"}``. Raises ``AssertionError``
        with one line per failing fault and connection.
        """
        message = all_fired_failure(self.fault_report(), allow)
        if message is not None:
            raise AssertionError(message)

    def _record_event(
        self,
        conn_index: int,
        stream: StreamSide | None,
        offset: int,
        fault_index: int,
        fault: Fault,
        params: ResolvedParams,
        kind: EventKind,
        *,
        offset_exact: bool = True,
    ) -> None:
        with self._lock:
            self._events.append(
                Event(
                    conn_index=conn_index,
                    stream=stream,
                    offset=offset,
                    offset_exact=offset_exact,
                    fault_index=fault_index,
                    fault=type(fault).__name__,
                    kind=kind,
                    params=params,
                    seq=self._event_seq,
                    timestamp=time.monotonic(),
                )
            )
            self._event_seq += 1

    def start(self) -> Proxy:
        """Bind an ephemeral port, start the loop thread, and return self."""
        if self.is_running:
            raise RuntimeError("proxy is already running")

        with self._lock:
            self._active_connections = 0
            self._client_to_server = 0
            self._server_to_client = 0
            self._events.clear()
            self._event_seq = 0
            self._connection_states.clear()

        self._next_index = 0
        self._ready.clear()
        self._startup_error = None
        self._thread = threading.Thread(
            target=self._thread_main,
            name=f"flakynet-proxy-{self._name}",
            daemon=True,
        )
        self._thread.start()

        if not self._ready.wait(timeout=10.0):
            self.stop()
            raise RuntimeError("proxy did not start within 10 seconds")
        if self._startup_error is not None:
            error = self._startup_error
            self.stop()
            raise error
        return self

    def stop(self) -> None:
        """Close the listener, cancel pumps, and join the loop thread."""
        thread = self._thread
        if thread is None:
            return
        if threading.current_thread() is thread:
            raise RuntimeError("stop() must not be called from the proxy thread")

        loop = self._loop
        stop_event = self._stop_event
        if loop is not None and stop_event is not None:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(stop_event.set)

        thread.join(timeout=10.0)
        if thread.is_alive():
            raise RuntimeError("proxy thread did not stop within 10 seconds")

        self._close_listener()
        self._thread = None
        self._loop = None
        self._stop_event = None
        with self._lock:
            self._active_connections = 0

    def __enter__(self) -> Proxy:
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._serve(loop))
        except BaseException as exc:
            self._startup_error = exc
            self._ready.set()
        finally:
            with contextlib.suppress(Exception):
                loop.run_until_complete(loop.shutdown_asyncgens())
            with contextlib.suppress(Exception):
                loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()
            self._close_listener()

    async def _serve(self, loop: asyncio.AbstractEventLoop) -> None:
        listener = socket.create_server(
            (self._bind_host, 0),
            family=socket.AF_INET,
            backlog=128,
        )
        listener.setblocking(False)
        self._listener = listener
        host, port = listener.getsockname()[:2]
        self._host = str(host)
        self._port = int(port)

        self._stop_event = asyncio.Event()
        self._connection_tasks.clear()
        accept_task = loop.create_task(self._accept_loop(loop, listener))
        self._ready.set()

        try:
            await self._stop_event.wait()
        finally:
            self._close_listener()
            accept_task.cancel()
            await asyncio.gather(accept_task, return_exceptions=True)
            await self._cancel_connections()

    async def _accept_loop(
        self,
        loop: asyncio.AbstractEventLoop,
        listener: socket.socket,
    ) -> None:
        while True:
            try:
                client, _ = await loop.sock_accept(listener)
            except asyncio.CancelledError:
                raise
            except OSError:
                break

            client.setblocking(False)
            index = self._next_index
            self._next_index += 1
            try:
                connection_plan = resolve(
                    self._plan,
                    self._seed,
                    self._name,
                    index,
                )
            except Exception:
                _close_socket(client)
                continue

            refusals = [
                fault
                for fault in connection_plan.faults
                if isinstance(fault, StaticFault) and isinstance(fault.fault, RefuseConnect)
            ]
            if refusals:
                with self._lock:
                    state = _ConnectionState(index=index, terminal_fired=True)
                    state.set_end("client", "flakynet")
                    state.set_end("server", "flakynet")
                    state.finalize()
                    self._connection_states[index] = state
                for fault in refusals:
                    self._record_event(
                        index,
                        None,
                        0,
                        fault.fault_index,
                        fault.fault,
                        fault.params,
                        "fired",
                    )
                _reset_socket(client)
                continue

            state = _ConnectionState(index=index)
            with self._lock:
                self._connection_states[index] = state
                self._active_connections += 1

            task = loop.create_task(
                self._handle_client(loop, client, index, connection_plan, state)
            )
            self._connection_tasks.add(task)
            task.add_done_callback(self._on_connection_done)

    def _on_connection_done(self, task: asyncio.Task[None]) -> None:
        self._connection_tasks.discard(task)
        with self._lock:
            if self._active_connections > 0:
                self._active_connections -= 1

        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.debug("connection task failed", exc_info=exc)

    async def _handle_client(
        self,
        loop: asyncio.AbstractEventLoop,
        client: socket.socket,
        index: int,
        connection_plan: ConnectionPlan,
        state: _ConnectionState,
    ) -> None:
        upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream.setblocking(False)
        try:
            try:
                await loop.sock_connect(upstream, self._target)
            except OSError:
                with self._lock:
                    state.set_end("client", "error")
                    state.set_end("server", "error")
                    state.finalize()
                return

            try:
                await self._run_pumps(
                    loop,
                    client,
                    upstream,
                    state,
                    connection_plan,
                )
            except _TerminalFault:
                pass
            except asyncio.CancelledError:
                raise
            except (ConnectionResetError, BrokenPipeError, OSError):
                logger.debug(
                    "connection %d ended with an error",
                    index,
                    exc_info=True,
                )
                with self._lock:
                    if state.client_end == "pending" and state.server_end == "pending":
                        state.set_end("client", "error")
                        state.set_end("server", "error")
            except BaseException:
                with self._lock:
                    if state.client_end == "pending":
                        state.set_end("client", "error")
                    if state.server_end == "pending":
                        state.set_end("server", "error")
                raise
        finally:
            with self._lock:
                state.finalize()
            _close_socket(client)
            _close_socket(upstream)

    async def _run_pumps(
        self,
        loop: asyncio.AbstractEventLoop,
        client: socket.socket,
        upstream: socket.socket,
        state: _ConnectionState,
        connection_plan: ConnectionPlan,
    ) -> None:
        armed_events: dict[tuple[int, StreamSide], asyncio.Event] = {
            (fault.fault_index, fault.stream): asyncio.Event()
            for fault in connection_plan.faults
            if isinstance(fault, ArmedFault) and fault.trigger.side != fault.stream
        }
        client_to_server = loop.create_task(
            self._pump(
                loop,
                client,
                upstream,
                "client_to_server",
                state,
                connection_plan.conn_index,
                connection_plan.faults,
                armed_events,
            )
        )
        server_to_client = loop.create_task(
            self._pump(
                loop,
                upstream,
                client,
                "server_to_client",
                state,
                connection_plan.conn_index,
                connection_plan.faults,
                armed_events,
            )
        )
        tasks = (client_to_server, server_to_client)
        try:
            done, _ = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_EXCEPTION,
            )
            for task in done:
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc is not None:
                    raise exc
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _pump(
        self,
        loop: asyncio.AbstractEventLoop,
        source: socket.socket,
        destination: socket.socket,
        direction: _Direction,
        state: _ConnectionState,
        conn_index: int,
        faults: tuple[StaticFault | ArmedFault, ...],
        armed_events: dict[tuple[int, StreamSide], asyncio.Event],
    ) -> None:
        stream: StreamSide = "client" if direction == "client_to_server" else "server"
        terminal = False
        try:
            action = await self._forward_with_faults(
                loop,
                source,
                destination,
                direction,
                state,
                conn_index,
                stream,
                faults,
                armed_events,
            )
            if action is not None:
                terminal = True
                with self._lock:
                    state.terminal_fired = True
                    state.set_end(stream, "flakynet")
                if action == "reset":
                    _reset_sockets(source, destination)
                else:
                    _graceful_close_sockets(source, destination)
                raise _TerminalFault
            with self._lock:
                state.set_end(stream, "graceful")
        except asyncio.CancelledError:
            with self._lock:
                state.set_end(stream, "cancelled")
            raise
        finally:
            if not terminal:
                with contextlib.suppress(OSError):
                    destination.shutdown(socket.SHUT_WR)

    async def _forward_with_faults(
        self,
        loop: asyncio.AbstractEventLoop,
        source: socket.socket,
        destination: socket.socket,
        direction: _Direction,
        state: _ConnectionState,
        conn_index: int,
        stream: StreamSide,
        faults: tuple[StaticFault | ArmedFault, ...],
        armed_events: dict[tuple[int, StreamSide], asyncio.Event],
    ) -> _TerminalAction | None:
        windows = _window_faults(faults, stream)
        discrete = list(_discrete_faults(faults, stream))
        observed = [
            (Matcher(armed.trigger.pattern), armed)
            for armed in faults
            if isinstance(armed, ArmedFault) and armed.trigger.side == stream
        ]
        incoming = [
            armed
            for armed in faults
            if isinstance(armed, ArmedFault)
            and armed.stream == stream
            and armed.trigger.side != stream
        ]
        offset = 0
        ordinal = 0
        fired: set[int] = set()
        entered: set[int] = set()
        exited: set[int] = set()
        data = b""
        position = 0

        def record_matches(chunk: bytes) -> None:
            for matcher, armed in observed:
                match_end = matcher.feed(chunk)
                if match_end is None:
                    continue
                if armed.stream == stream:
                    discrete.append(_anchored(armed, match_end + armed.delta))
                else:
                    armed_events[(armed.fault_index, armed.stream)].set()

        while True:
            # Same-stream faults anchor at an exact offset; cross-stream faults
            # activate only when there is a byte to forward, because their
            # trigger is "the next byte forwarded in the target stream".
            pending: list[tuple[int, StaticFault, bool]] = [
                (fault.fault_index, fault, True)
                for fault in discrete
                if fault.fault_index not in fired and fault.start <= offset
            ]
            if position < len(data):
                for armed in incoming:
                    if armed.fault_index in fired:
                        continue
                    if armed_events[(armed.fault_index, stream)].is_set():
                        pending.append((armed.fault_index, _anchored(armed, offset), False))

            if pending:
                _, fault, exact = min(
                    pending,
                    key=lambda item: (item[0], item[1].start),
                )
                fired.add(fault.fault_index)
                self._record_event(
                    conn_index,
                    stream,
                    fault.start,
                    fault.fault_index,
                    fault.fault,
                    fault.params,
                    "fired",
                    offset_exact=exact,
                )
                self._finish_windows(
                    conn_index,
                    stream,
                    offset,
                    windows,
                    entered,
                    exited,
                )
                if isinstance(fault.fault, Blackhole):
                    position = len(data)
                    try:
                        await _drain(loop, source, record_matches)
                    except OSError:
                        with self._lock:
                            state.set_end(stream, "aborted")
                        raise
                    return None
                return _action_for(fault)

            if position >= len(data):
                try:
                    data = await loop.sock_recv(source, 65536)
                except OSError:
                    with self._lock:
                        state.set_end(stream, "aborted")
                    raise
                position = 0
                if not data:
                    self._finish_windows(conn_index, stream, offset, windows, entered, exited)
                    return None
                record_matches(data)
                continue

            boundary = _next_boundary(windows, discrete, fired, offset)
            count = len(data) - position
            if boundary is not None:
                count = min(count, boundary - offset)
            fragment_cap = _fragment_cap(windows, offset)
            if fragment_cap is not None:
                count = min(count, fragment_cap)

            delay_ms = _latency_ms(windows, offset, stream, ordinal)
            rate = _bandwidth_rate(windows, offset)
            delay_seconds = delay_ms / 1000.0
            if rate is not None:
                delay_seconds += count / rate
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)

            piece_start = offset
            try:
                await loop.sock_sendall(destination, data[position : position + count])
            except OSError:
                destination_stream: StreamSide = (
                    "server" if direction == "client_to_server" else "client"
                )
                with self._lock:
                    state.set_end(destination_stream, "aborted")
                raise
            self._advance(direction, state, count)
            offset += count
            position += count
            ordinal += 1

            self._enter_windows(conn_index, stream, piece_start, windows, entered)
            self._exit_windows(conn_index, stream, offset, windows, entered, exited)

    def _enter_windows(
        self,
        conn_index: int,
        stream: StreamSide,
        offset: int,
        windows: tuple[StaticFault, ...],
        entered: set[int],
    ) -> None:
        for fault in windows:
            if fault.fault_index in entered or not _in_window(fault, offset):
                continue
            entered.add(fault.fault_index)
            self._record_event(
                conn_index,
                stream,
                offset,
                fault.fault_index,
                fault.fault,
                fault.params,
                "window_entered",
            )

    def _exit_windows(
        self,
        conn_index: int,
        stream: StreamSide,
        offset: int,
        windows: tuple[StaticFault, ...],
        entered: set[int],
        exited: set[int],
    ) -> None:
        for fault in windows:
            if fault.fault_index not in entered or fault.fault_index in exited:
                continue
            if fault.end is None or offset < fault.end:
                continue
            exited.add(fault.fault_index)
            self._record_event(
                conn_index,
                stream,
                fault.end,
                fault.fault_index,
                fault.fault,
                fault.params,
                "window_exited",
            )

    def _finish_windows(
        self,
        conn_index: int,
        stream: StreamSide,
        offset: int,
        windows: tuple[StaticFault, ...],
        entered: set[int],
        exited: set[int],
    ) -> None:
        for fault in windows:
            if fault.fault_index not in entered or fault.fault_index in exited:
                continue
            exit_offset = fault.end if fault.end is not None and fault.end <= offset else offset
            exited.add(fault.fault_index)
            self._record_event(
                conn_index,
                stream,
                exit_offset,
                fault.fault_index,
                fault.fault,
                fault.params,
                "window_exited",
            )

    def _advance(
        self,
        direction: _Direction,
        state: _ConnectionState,
        count: int,
    ) -> None:
        with self._lock:
            state.add(direction, count)
            if direction == "client_to_server":
                self._client_to_server += count
            else:
                self._server_to_client += count

    async def _cancel_connections(self) -> None:
        tasks = list(self._connection_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._connection_tasks.clear()
        with self._lock:
            self._active_connections = 0

    def _close_listener(self) -> None:
        listener = self._listener
        if listener is None:
            return
        self._listener = None
        with contextlib.suppress(OSError):
            listener.close()
