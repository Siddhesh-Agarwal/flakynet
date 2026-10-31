# Determinism

flakynet separates **phase-1 resolution** from **runtime forwarding**. The seed
controls phase 1 exactly. Runtime event occurrence also depends on the
operating system, the application protocol, and scheduling.

## What the seed pins

For each accepted connection, flakynet builds a private RNG:

```text
key = f"{seed}:{proxy_name}:{conn_index}"
rng = random.Random(key)
```

`proxy_name` defaults to `"p0"`; pass distinct names when several proxies share
a test or process. `conn_index` is zero-based and assigned in accept order.
Refused connections also consume an index.

Faults are visited in plan order. A fault whose `conn` selector does not match
the connection is skipped. The draws are:

1. `RefuseConnect`: no draw. It is resolved as a connection-level fault.
2. `Latency`: one 64-bit `jitter_key`, then for each affected stream, the window
   `start` and `end` if they are `Between`.
3. `Bandwidth`: for each affected stream, the window `start` and `end` if they
   are `Between`, then the rate if it is `Between`.
4. `Fragment`: for each affected stream, the window `start` and `end` if they
   are `Between`.
5. `Reset`, `CloseAfter`, `Blackhole`:
   - with `after=saw(...)`: the `delta` if it is `Between`, then an armed fault
     is created for each affected stream;
   - with `after_bytes`: the offset for each affected stream if it is
     `Between`.

`side="both"` resolves the fault separately for `"client"` and `"server"`, in
that order. `Latency` shares one `jitter_key` across both streams, but the
per-segment delay sequence is keyed by the stream name as well.

The resolved values are stored in `ConnectionPlan`. Runtime forwarding does not
consume this RNG. The process-wide `random` state is irrelevant.

## What replays

Given the same application byte stream and the same close boundaries:

- Static offsets, window bounds, bandwidth rates, and armed deltas replay.
- The byte matcher is chunk-invariant: it retains `len(pattern) - 1` bytes
  between reads, so a `saw` match spanning OS read boundaries is still found.
- The pump splits reads at the next fault boundary, so OS read sizes do not
  move a static offset or a same-stream `saw` anchor.
- `window_entered` records the first forwarded byte inside a window.
  `window_exited` records the exclusive window end, or the stream length when
  the window is open-ended or the stream ends inside it.
- A discrete fault records `fired` at its trigger offset.
- `canonical_events()` sorts by `(conn_index, stream, offset, fault_index,
  kind)`, removes `seq` and timestamps, and replaces an inexact offset with
  `None`.

`Latency` logs only `window_entered` and `window_exited`. It never logs a
per-segment event.

## What does not replay

### Arrival order and timestamps

`proxy.events` is in arrival order and is diagnostic only. Sorting canonical
events cannot make a scheduling-dependent event happen.

### Concurrent connection indices

`conn_index` follows `sock_accept` order. Under simultaneous connects the OS
decides that order, so the mapping from a client to `p0`, `p1`, ... and to its
resolved plan can change. Compare per-connection event sequences, or serialize
connects when client identity matters.

### Cross-stream `saw` faults

A fault whose `saw` watches the other stream activates on the next byte
forwarded on the target stream. The actual offset is recorded with
`offset_exact=False` and canonicalized to `None`. If the target stream closes
first, the fault never fires. Replay is reliable only for request/response
protocols where the target cannot finish before the trigger.

### Competing terminals and sibling cancellation

A `Reset` or `CloseAfter` closes both sockets and cancels the sibling pump. If
terminals race on both streams, the winning stream can vary, and both `fired`
events may or may not appear. A terminal can also cancel the sibling before it
records `window_entered` or `window_exited`, so window events on that stream may
be partial or absent. `stop()` cancels in-flight pumps in the same way.

### Chunking-dependent pacing

Static offsets and same-stream anchors are chunk-invariant, but `Latency`
segment boundaries, and therefore which bytes receive which delay, follow OS
reads. `Bandwidth` segment sizes and observed timing do too. The planned delay
values and rates replay; their placement on the byte stream does not.

### Measured time

Planned values are fixed, but actual elapsed time, throughput, and the
interleaving of the two pumps are not. No fault value is computed from measured
elapsed time. Windows timer granularity can overshoot short sleeps.

### Application and platform behaviour

Different bytes, close timing, or server responses change content-dependent
triggers, stream lengths, and cross-stream races. RST versus FIN and the bytes
in flight at close are OS-dependent. See [`faults.md`](./faults.md).

### Python runtime versions

Seed replay is tested within a Python runtime. Cross-version equality of the
standard-library RNG used for a string seed is not part of the promise.

### Report statuses

`PREEMPTED`, `ENDED_EARLY`, `MISSED`, and `PENDING` can follow the same races.
Take the report after the client has finished. A connection cancelled by
`stop()` remains `PENDING`, because it was never observed to end on its own.

## Comparing runs

Use `canonical_events()`, never `events`, for replay comparisons. For a single
connection with a deterministic byte stream, compare the canonical tuple
directly. For concurrent connections, compare the multiset of per-connection
event sequences, not the mapping from clients to connection indices.

A difference in a cross-stream occurrence, a terminal race, or a sibling window
event is a scheduling difference, not a seed defect. A difference in a static
offset, a window bound, a resolved rate, or a same-stream anchor is a seed or
plan defect.

`tests/test_determinism_limits.py` demonstrates the terminal winner, sibling
window cancellation, cross-stream miss, accept-order mapping, chunk-invariant
window events, global-RNG independence, and pacing-time independence.

## Pytest seed selection

With the pytest fixture, the effective seed is chosen in this order:

1. an explicit `seed=` in the test (pinned);
2. `--flakynet-seed`;
3. the current `--flakynet-sweep` value;
4. `0`.

`--flakynet-sweep=N` parametrizes the fixture with seeds `0..N-1`. In a single
process, it stops the remaining seeds after the first failure and prints the
first failing seed in the terminal summary. Under pytest-xdist, each worker has
its own sweep state; run the sweep in a single process for the global
stop-at-first-failure behaviour.
