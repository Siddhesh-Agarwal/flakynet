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

## 2026-10-30 — Static fault wiring in pumps

Decision: wire only static `CloseAfter` and `Reset` into the pumps in this step.
Unsupported fault types and armed `saw` triggers raise `NotImplementedError`
when a `Proxy` is constructed. Offsets are absolute stream offsets; each pump
splits the current read at the next fault boundary before forwarding.

Reason: keeps the first fault-wiring slice honest and testable; a plan that
contains unwired faults never silently runs as if it were fault-free.

Cost: plans with the other five faults cannot run until their pump behavior is
added.

## 2026-10-30 — Reset/CloseAfter platform TODOs

- TODO(Windows): verify `SO_LINGER` packing (`struct.pack("ii", 1, 0)`) and
  whether the peer observes RST rather than FIN after `Reset`.
- TODO(macOS): verify that immediate `shutdown(SHUT_WR)` + close preserves the
  exact prefix for `CloseAfter`, and whether `Reset` produces RST.
- TODO(all): an RST close may discard bytes still in the kernel send buffer.
  The exact-prefix tests run on loopback and do not prove delivery under load.

## 2026-10-30 — Proxy name default

Decision: `Proxy(name=None)` now uses `"p0"` for the resolution RNG key and
thread name, instead of an object-identity-derived name. Standalone users who
create multiple proxies should pass distinct names.

Reason: the same seed must replay the same per-connection plan; `id(self)`
changes between runs.

Cost: two unnamed proxies in one process share the same RNG stream.

## 2026-10-30 — Proxy lifecycle

Decision: each `Proxy` owns one daemon thread and one asyncio loop. `start()`
binds port 0 and waits for readiness; `stop()` closes the listener, cancels the
accept loop and all connection tasks, closes sockets, and joins the thread.
Same-instance restart is supported, and byte counters reset on each `start()`.

Reason: the caller never owns an event loop, and the lifecycle maps directly to
a context manager.

Cost: one thread per proxy instance for the lifetime of the proxy.
