# Panel brief: t-3562, terminal control frames that cannot be queued end the connection

You are one of three reviewers from different vendors. Pick a design, say what the
others understate, and quote the code that supports each claim. Read the code; do not
edit; do not build or run anything. Your cwd is a worktree of starh2 at master c30c99b.
The pinned zio source is in `zig-pkg/`.

## Premises (settled; a finding that depends on their negation is out of scope)

- Nothing is in production. Breaking changes are fine.
- A peer is untrusted. Memory per connection must stay bounded no matter what the peer
  sends. A peer that floods may lose its connection, and that is acceptable.
- A well-behaved peer must not lose its connection because the SERVER produced many
  per-stream failures (handler errors, aborts, slow-consumer resets, refused responses)
  during one write stall.
- The terminal control frames that must always be possible are the ones that end a
  flood: a GOAWAY, and an RST_STREAM for a stream the server gives up on.
- Out of scope: t-3593 (`wakeAllSpace`), t-3596 to t-3600, throughput tuning.
- Report the per-connection memory cost of your pick (bytes, at default limits and at
  `max_connections`). The owner decides on memory; do not assume it is free or banned.

## Facts (read them in the code; each was read at c30c99b)

1. **The ring is smaller than the pool.** `Connection.init` and `planBoot`
   (`src/edge/connection.zig`, `term_cap = @max(control_entries_per_connection / 8, 16)`)
   size the terminal ring to 32 entries at defaults. The control pool
   (`src/edge/control_pool.zig`) lets terminal frames use the WHOLE pool:
   `control_entries_per_connection` = 256 entries and `control_bytes_per_connection` =
   64 KiB. The ordinary ring is 256. An RST_STREAM is 13 bytes on the wire. So at
   defaults the ring, not the pool, is the limit for terminal frames.
2. **Overflow ends the connection.** `FairScheduler.enqueueControl` returns `QueueFull`
   when the ring is full (`PoolFull` when the pool is). In `materializeIntents`
   (connection.zig, near "ctrl_terminal_full") a terminal-class failure sets
   `writer_failed` and the connection ends.
3. **Terminal frames cannot drain during a write stall.** TLS: `pause_control` returns
   `stashFull() or outboundMemoryPaused()` (set in `runBody`). h2c: `h2cPause` →
   `h2cPauseFor(0)`: outbound memory, ack-ring room, write-queue room. The scheduler's
   terminal loop (`fair_scheduler.zig`, "Terminal controls first") stops on
   `isControlPaused()` and while a header block is open (`block_open`).
4. **Measured in t-3567** (`tests/t3567_tls_barriers.zig` on that branch): with the TLS
   stash full, 33 per-stream failures in one stall overflow the 32-entry ring and end
   the connection, whatever caused the failures. t-3567 removed one cause (bare flush
   barriers); the class remains.
5. **A queued RST frees the stream at once.** `Session.emitRst` (`src/core/session.zig`)
   queues the RST, then `releaseConcurrency`, so `active_streams` drops when the RST is
   QUEUED, not when it is written. A peer can then open another stream at once. So the
   number of queued RSTs is bounded by peer behaviour (and the frame rate limiter,
   `chargeRate`), not by `max_streams_per_connection`.
6. **Refusals are peer-driven RSTs.** `emitRst(.refused_stream)` when
   `active_streams >= max_streams_per_connection` or the stream hooks refuse
   (`tryAdmit`). Protocol errors on peer frames also RST.
7. **A reset drops the stream's queued HEADERS, but after the RST is queued.**
   `FairScheduler.forgetStream` → `dropQueuedHeaders` frees the queued HEADERS and their
   CONTINUATIONs with their pool occupancy. It runs from `applyStreamResetIntent`, which
   handles the `stream_reset` intent. `emitRst` pushes the `outbound_frame` (the RST)
   BEFORE the `stream_reset` intent, so the RST needs a free entry before the drop frees
   one.
8. **Pool occupancy is held until the write completes**, not until emit
   (`control_pool.zig` header). During a stall, frames already in the stash still hold
   their entries.
9. One `ControlEntry` is about 48 bytes. 256 entries instead of 32 is about +10.7 KiB
   per connection, about 11 MiB at the default `max_connections` (1,024), in the
   connection slot's boot buffer (`Limits.slotBootBytes`).

## Candidates (not exhaustive; propose a better one if you have it)

- **C1. One limit: size the terminal ring to the pool.** The ring holds
  `control_entries_per_connection` entries, so the pool's split (ordinary leaves the
  reserve; terminal may use all of it) is the only policy. Overflow is then `PoolFull`
  only. Smallest change. Does not by itself make a stall drain.
- **C2. C1, and apply the reset before queueing its RST.** Reorder so the stream's
  queued HEADERS are dropped (fact 7) before the RST takes an entry. Then a stream's
  RST can reuse the entry its own unsent HEADERS held.
- **C3. Bound peer-driven RSTs by reading less.** While the terminal backlog is above a
  watermark, the actor stops ingesting peer frames, so refusals and protocol RSTs stop
  being produced. Server-driven RSTs are then bounded by live streams. Check: can that
  deadlock the drain (does the write side ever need a read to make progress: acks,
  WINDOW_UPDATE, TLS records)? What bounds the RSTs produced by ONE read chunk?
- **C4. Hold the stream's concurrency until its RST is written** (fact 5). Then queued
  server RSTs are bounded by `max_streams_per_connection`. Refusals of streams that were
  never admitted are not covered.
- **C5. Keep the 32-entry ring; on overflow, send GOAWAY(ENHANCE_YOUR_CALM) from the
  reserve and close gracefully instead of failing the writer.** Check whether that
  GOAWAY can be written at all during the stall that caused the overflow.

## Questions

1. Which design meets all the premises, on both transports? Pick one (or a combination)
   and say why the others fail a premise. A design that drops a server-driven per-stream
   failure on a well-behaved peer's stall fails premise 3.
2. What is the worst-case number of terminal entries a WELL-BEHAVED peer's connection
   can need during one stall, under your pick? Derive it from the code (live streams,
   in-flight frames holding entries, GOAWAY), and say whether the ring and the pool are
   both at least that.
3. What bounds a FLOODING peer under your pick, and what is the connection's outcome?
4. Fact 3: should terminal frames be allowed past `pause_control` into a reserved slice
   of the stash or write queue? Say what that buys if the stash is full because the
   socket is not writable.
5. The test that would fail today and pass after your pick, on each transport, with an
   oracle that does not depend on timing.

## Output

Every finding MUST carry exactly one label, or it is discarded: **[LOGIC]** (holds
whatever the deployment) or **[PREMISE]** (depends on a deployment fact: name it). Lead
with your pick. Quote at least one line of code for each fact you rely on.
