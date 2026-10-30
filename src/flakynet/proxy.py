from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import threading
from dataclasses import dataclass
from types import TracebackType
from typing import Literal

logger = logging.getLogger(__name__)

_Direction = Literal["client_to_server", "server_to_client"]


def _close_socket(sock: socket.socket) -> None:
    with contextlib.suppress(OSError):
        sock.close()


@dataclass(slots=True)
class _ConnectionState:
    index: int
    client_to_server: int = 0
    server_to_client: int = 0

    def add(self, direction: _Direction, count: int) -> None:
        if direction == "client_to_server":
            self.client_to_server += count
        else:
            self.server_to_client += count


class Proxy:
    """A small in-process TCP proxy with no fault injection.

    The proxy owns an asyncio loop in a daemon thread. Callers use the
    normal ``host``/``port`` socket address and never need to run a loop.
    """

    def __init__(
        self,
        target: tuple[str, int],
        *,
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
        self._bind_host = host
        self._name = name or f"p{id(self):x}"

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

    def start(self) -> Proxy:
        """Bind an ephemeral port, start the loop thread, and return self."""
        if self.is_running:
            raise RuntimeError("proxy is already running")

        with self._lock:
            self._active_connections = 0
            self._client_to_server = 0
            self._server_to_client = 0

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
            with self._lock:
                self._active_connections += 1

            task = loop.create_task(self._handle_client(loop, client, index))
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
    ) -> None:
        upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream.setblocking(False)
        try:
            try:
                await loop.sock_connect(upstream, self._target)
            except OSError:
                return

            state = _ConnectionState(index=index)
            try:
                await self._run_pumps(loop, client, upstream, state)
            except (ConnectionResetError, BrokenPipeError, OSError):
                logger.debug(
                    "connection %d ended with an error",
                    index,
                    exc_info=True,
                )
        finally:
            _close_socket(client)
            _close_socket(upstream)

    async def _run_pumps(
        self,
        loop: asyncio.AbstractEventLoop,
        client: socket.socket,
        upstream: socket.socket,
        state: _ConnectionState,
    ) -> None:
        client_to_server = loop.create_task(
            self._pump(
                loop,
                client,
                upstream,
                "client_to_server",
                state,
            )
        )
        server_to_client = loop.create_task(
            self._pump(
                loop,
                upstream,
                client,
                "server_to_client",
                state,
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
    ) -> None:
        try:
            while True:
                data = await loop.sock_recv(source, 65536)
                if not data:
                    break
                await loop.sock_sendall(destination, data)

                state.add(direction, len(data))
                with self._lock:
                    if direction == "client_to_server":
                        self._client_to_server += len(data)
                    else:
                        self._server_to_client += len(data)
        finally:
            with contextlib.suppress(OSError):
                destination.shutdown(socket.SHUT_WR)

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
