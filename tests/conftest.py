from __future__ import annotations

import contextlib
import socket
import socketserver
import threading
from collections.abc import Iterator

import pytest

pytest_plugins = ["pytester"]


class _EchoHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        while True:
            try:
                data = self.request.recv(65536)
            except OSError:
                return
            if not data:
                break
            try:
                self.request.sendall(data)
            except OSError:
                return
        with contextlib.suppress(OSError):
            self.request.shutdown(socket.SHUT_WR)


class _EchoServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    block_on_close = False


@pytest.fixture
def echo_server() -> Iterator[tuple[str, int]]:
    server = _EchoServer(("127.0.0.1", 0), _EchoHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="echo-server",
    )
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield str(host), int(port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
