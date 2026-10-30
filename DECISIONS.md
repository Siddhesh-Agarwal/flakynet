## 2026-10-30 — Proxy engine transport

Decision: the proxy engine uses raw non-blocking sockets with asyncio
`loop.sock_accept`, `sock_connect`, `sock_recv`, and `sock_sendall`, and
`shutdown(SHUT_WR)` for half-close, rather than `StreamReader`/`StreamWriter`.

Reason: explicit socket ownership and `shutdown()` give reliable half-close
semantics on Linux, macOS, and Windows, and make cancellation and cleanup
straightforward.

Cost: more manual buffering and error handling than the stream APIs.

## 2026-10-30 — Phase-1 representation

Decision: keep absolute integer stream offsets, inclusive `Between` random
ranges, `(start, end)` window tuples with `None` meaning the whole stream, and
separate `after_bytes` / `after: Saw` trigger fields.

Reason: this is the existing pure-resolution representation and was confirmed
before extending phase 1. It keeps armed deltas pre-drawn at resolution and
avoids introducing a second offset/window abstraction.

Cost: there is no first-class `stream(...)` selector; stream choice remains the
`side` literal on faults and `Saw.side`.

## 2026-10-30 — Proxy lifecycle

Decision: each `Proxy` owns one daemon thread and one asyncio loop. `start()`
binds port 0 and waits for readiness; `stop()` closes the listener, cancels the
accept loop and all connection tasks, closes sockets, and joins the thread.
Same-instance restart is supported, and byte counters reset on each `start()`.

Reason: the caller never owns an event loop, and the lifecycle maps directly to
a context manager.

Cost: one thread per proxy instance for the lifetime of the proxy.
