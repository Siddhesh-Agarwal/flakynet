# Fault semantics

flakynet is an in-process TCP proxy. It accepts each client connection, opens
a TCP connection to the target, and runs two independent pumps over raw,
non-blocking sockets. It does not use TLS, UDP, or raw packets.

This page describes the trigger rules and the socket-level behaviour of every
fault, including the behaviour that differs by operating system.

## Offsets and triggers

- Offsets are absolute, zero-based byte offsets in the forwarded stream. They
  count bytes the proxy has written to the peer, not bytes it has read from the
  source.
- `side="client"` means client -> target. `side="server"` means target ->
  client. `side="both"` resolves the fault separately for each stream.
- A window is `[start, end)`: `start` is inclusive and `end` is exclusive.
  `end=None` means the rest of the stream. `between(low, high)` is inclusive at
  both ends and is drawn during phase-1 resolution.
- A discrete fault triggers when the stream offset reaches `after_bytes`.
- `saw(pattern, side=..., delta=...)` triggers on the first match on the
  watched stream. A fault targeting the same stream anchors at `M + delta`,
  where `M` is the exclusive end offset of the match. A fault targeting the
  other stream activates on the next byte forwarded there; that offset is
  recorded with `offset_exact=False`.
- A terminal fault (`Reset` or `CloseAfter`) closes both sockets and cancels
  the sibling pump. The first terminal to fire wins.
- `RefuseConnect` acts at accept time, before any upstream connection is made.
- flakynet never sets `TCP_NODELAY`; Nagle remains at the OS default.

## Socket-level semantics by OS

| Fault | Linux | macOS | Windows |
| --- | --- | --- | --- |
| `Latency` | Sleeps before each `sendall`; no socket option. Uses the process event-loop policy (normally `EpollSelector`). | Same; normally `KqueueSelector`. | Same; normally `ProactorEventLoop` (IOCP). Short sleeps can overshoot because of the default timer granularity (about 15.6 ms). |
| `Bandwidth` | Sleeps `segment_bytes / rate` before each `sendall`; no socket option. | Same. | Same; timer granularity affects observed pacing. |
| `Fragment` | Splits data into separate `sendall` calls of at most N bytes. TCP/Nagle may coalesce writes; the receiver is not guaranteed separate reads. | Same; Nagle and scheduling may make the pieces less observable. | Same; Nagle and scheduling may make the pieces less observable. |
| `CloseAfter` | `shutdown(SHUT_WR)` on both sockets, then `close()`. FIN normally; if the receive buffer still has unread data, the close may emit RST. No `SO_LINGER` change. | Same API. FIN is expected without unread data; RST is possible with unread data. The core suite asserts EOF for its exact-prefix `CloseAfter` cases. | Same API. FIN normally; RST is possible with unread data. The core suite asserts EOF for its exact-prefix `CloseAfter` cases. |
| `Reset` | `setsockopt(SO_LINGER, struct.pack("ii", 1, 0))`, then `close()`. Linux `struct linger` is two ints (8 bytes), so RST is expected and unsent data is discarded. | Same 8-byte layout; RST is expected. CI accepts EOF as well, so RST is not asserted there. | Current code sends 8 bytes, but Winsock expects `struct linger` with two `u_short` values (4 bytes). The `setsockopt` error is suppressed, so the option may not apply and the close can be graceful (FIN). RST is **not guaranteed** on Windows. |
| `Blackhole` | Stops writing, then drains and discards the source with `recv`. No FIN or RST is sent by the fault; the connection stays open until the source reaches EOF or the proxy stops. | Same. | Same. |
| `RefuseConnect` | Accepts the client, sets `SO_LINGER(1, 0)`, and closes it; no upstream connection is made. RST is expected. | Same 8-byte layout; RST is expected. | Same 4-byte `SO_LINGER` limitation as `Reset`; the peer may see FIN instead. |

## Close races and exact prefixes

`CloseAfter` does not drain the receive side. It shuts down the write side and
closes both sockets immediately. Bytes already written are usually delivered,
but if the peer has sent data the proxy has not read, the close can become an
RST. A `Reset` discards unsent bytes still in the kernel send buffer.

Do not assert an exact received prefix under load. Prefer the proxy's forwarded
byte counts and the fault report, which count what the pumps actually wrote.

## Windows `SO_LINGER`

Winsock defines:

```c
struct linger {
    u_short l_onoff;
    u_short l_linger;
};
```

The current implementation packs `struct.pack("ii", 1, 0)` (8 bytes) on every
platform. That layout is correct on Linux and macOS but not on Windows. The
error is intentionally suppressed in `_reset_socket()`, so until this is fixed,
treat `Reset` and `RefuseConnect` on Windows as "abortive close requested", not
as "RST guaranteed". The limitation is recorded in `DECISIONS.md`.

## Verification status

- **Linux:** the core suite exercises `CloseAfter`, `Reset`, and `RefuseConnect`
  on loopback. The exact-prefix `CloseAfter` test observed EOF on Linux.
- **macOS:** CI runs the core suite on `macos-latest`. The exact-prefix
  `CloseAfter` tests assert EOF; the `Reset` and `RefuseConnect` tests accept
  EOF as well as RST on non-Linux.
- **Windows:** CI runs the core suite on `windows-latest`. The same permissive
  `Reset`/`RefuseConnect` assertions apply, so the Windows `SO_LINGER` path is
  not proven by CI.
- The real-client integration suite is Ubuntu-only.

## Event and report implications

A terminal fault records its `fired` event before closing the sockets. The
sibling pump may be cancelled before it records a window exit, so window events
on the other stream can be partial or absent. See
[`determinism.md`](./determinism.md) for the full set of races.
