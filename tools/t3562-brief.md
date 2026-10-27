# Brief: t-3562, terminal control frames that cannot be queued end the connection

You are the implementer. Reviewers from another vendor read your diff, and the
orchestrator reruns every test and measures speed itself. Do not tell the
orchestrator that tests pass; it recomputes them. Do not prompt, message or read any
other agent's pane.

## Why (settled; do not reopen)

Read `tools/t3562-panel.md` (the facts and the five candidates). A three-model panel
(gpt-6-astra, Grok 4.7, Fable) answered it. The owner (Ryan) chose the design below.
The panel corrected the brief in four places, and each correction was checked in the
code:

- HEADERS is never rate-charged (`rates.zig`: `.data, .headers => return true`), so
  peer refusals are not bounded by `chargeRate`.
- There are THREE limits on a terminal frame, not two: the terminal ring (32), the
  control pool's terminal reserve (`TERMINAL_CONTROL_RESERVE_ENTRIES` = 16, bytes 4 KiB),
  and the frame-slab reserve (`Limits.frameSlabReserve` = 16 slabs of 512 B; a terminal
  frame is built in a rented slab before it needs a ring slot). Fixing only the ring
  leaves the 17th RST failing.
- `emitRst` is not idempotent. `applyCommand(.reset_stream)` calls it with no guard,
  and `checkSlowConsumers` calls it even "if the stream already closed". One stream can
  queue two RST frames.
- There are up to three GOAWAYs, not one: graceful phase 1 and phase 2
  (`emitGoawayNoTerminal`), and a connection error (`failConnectionWith`).

## The bound the design rests on

A well-behaved peer never has more than the advertised `SETTINGS_MAX_CONCURRENT_STREAMS`
(= `max_streams_per_connection`, M) streams open in its own view. A queued, unwritten
RST is for a stream the peer still counts as open, because the RST is what would close
it. So if every unwritten RST is for a DISTINCT stream ID that the peer can still count
as open, a well-behaved peer's connection needs at most **M RSTs + 3 GOAWAYs** of
terminal room during any stall, whatever the server's handlers do. Call it
**T = M + 3**. The design makes T true in code and makes every terminal limit equal T.
Anything past T is a misbehaving peer.

## Decided design (if an item cannot work, STOP and show why)

1. **One derived number.** `T = max_streams_per_connection + 3`, computed in one place
   (`Limits`), never a tuning constant. Byte room for terminal frames is derived the
   same way: `M * rst_stream_frame_bytes + 3 * goaway_frame_bytes` (`frame.zig`; GOAWAY
   carries no debug data in this server: keep a comptime or boot check that says so).
2. **Apply T to all three limits.**
   - The terminal ring holds T entries (`planBoot` and `init`, which today use
     `@max(control_entries_per_connection / 8, 16)`).
   - The control pool reserves T entries and the derived bytes for terminal frames.
     Make `control_entries_per_connection` and `control_bytes_per_connection` the
     ORDINARY limits; the pool's totals become ordinary + terminal. Remove
     `TERMINAL_CONTROL_RESERVE_ENTRIES` / `_BYTES` as policy numbers (delete them or
     derive them; no fixed 16 or 4 KiB may remain as a terminal limit). Update the
     `control_pool.zig` header so it says what is true.
   - Terminal frames do not rent frame slabs. Build each RST and GOAWAY in a boot-time
     store of T fixed-size slots (the largest terminal frame is a GOAWAY with no debug
     data, 17 bytes). A slot is taken when the frame is built and returned when the
     frame's payload is no longer needed (find where a slab is released today:
     `releaseWireBytes` after the copy into the emit batch). Then remove the frame-slab
     reserve if nothing else needs it (`frameSlabReserve`, `frame_slab_reserve`); if
     something does, say what and keep it.
3. **One RST per stream, and only for a stream the peer can still count as open.**
   Make `emitRst` queue a frame at most once per stream ID per connection, and not at
   all for a stream that is already closed in both directions. The local cleanup that
   the slow-consumer sweep and the handler paths need (the `stream_reset` intent, the
   scheduler's `forgetStream`/`removePending`, waking waiters) must still happen when
   no frame is queued. Choose the predicate from the session's stream state and its
   tombstones, and write in the doc comment why it guarantees the bound above.
4. **Each GOAWAY role at most once.** Phase 1, phase 2, and the final connection-error
   GOAWAY may each be queued at most once per connection. Check each path
   (`beginGracefulPhase1`, `beginGracefulPhase2`, `failConnectionWith`); add guards
   where one is missing.
5. **Past the bound: GOAWAY then close (owner's choice).** The final GOAWAY always has a
   ring slot, pool room and a store slot: an RST may be admitted only if the final
   GOAWAY's room stays free (unless the final GOAWAY is already queued). An RST that does
   not fit means the peer is misbehaving: call `failConnection(.enhance_your_calm)`
   instead of queueing it, so the connection enters its existing closing phase (reads
   and drops, `close_flush_timeout_ns`). Do NOT set `writer_failed` on this path. If the
   final GOAWAY itself cannot be queued, that is a broken invariant: keep today's
   `writer_failed` fallback, and make it loud in debug builds.
6. **Both transports.** The ring, pool and store are transport-blind; the stall is
   `pause_control` on TLS (`stashFull() or outboundMemoryPaused()`) and h2c
   (`h2cPauseFor(0)`). Do not change the pause rules: terminal frames wait in the ring
   during a stall, by design (all three panelists).
7. **A boot check (rung 3).** `Limits` validation fails with `error.InvalidConfig` when
   the three capacities could disagree, and a test asserts that for M in {1, 256, 1024}
   the planned terminal ring, the pool's terminal reserve and the store all equal T.
   `slotBootBytes` must include the new store.

## Tests (required; oracles must not depend on timing)

- **Server-driven failures in one stall, both transports.** A server with defaults; a
  raw client opens M streams to a handler that starts its response and waits until its
  HEADERS have left the ring. Hold the writer (`wire_pump.test_hold_writes`; on TLS also
  the stash hold used in `tests/t3567_tls_barriers.zig`) and make `pause_control` true.
  Then every handler aborts (M server RSTs) and a graceful shutdown queues its GOAWAYs.
  Oracles: `test_ctrl_terminal_full` unchanged, `writer_failed` false, the terminal ring
  holds every frame; after the hold is released the client reads exactly one RST per
  stream and the GOAWAY(s), then EOF. Show it FAILS at c30c99b (the 33rd abort today).
- **Pool, not only the ring.** Same stall with the ordinary share pinned (frames already
  in flight, which `forgetStream` cannot reclaim). Today the 17th RST fails.
- **Duplicate resets.** A stream reset by its handler and then by the slow-consumer
  sweep queues one RST frame. A reset of a stream closed in both directions queues none,
  and its local cleanup still runs.
- **Flood.** In a stall, more than M peer-driven RSTs (minimal HEADERS for new stream IDs
  past the advertised limit, in one read chunk): the final GOAWAY (ENHANCE_YOUR_CALM) is
  queued, `writer_failed` stays false, the ring never holds more than T, and the
  connection closes by its closing deadline. After the hold is released the client reads
  the GOAWAY with that code.
- **Boot.** The capacity test of item 7, and `InvalidConfig` for a config that breaks it.

Every new test is mutation-tested: break the CODE, not the fixture.

## Mutations (full `./zb build test` each; record what fails)

1. Terminal ring back to `@max(control_entries_per_connection / 8, 16)`.
2. Pool terminal reserve back to 16 entries.
3. Terminal frames back on rented frame slabs (with the ordinary slab share in use).
4. Remove the one-RST-per-stream guard.
5. Remove one GOAWAY role's at-most-once guard.
6. Remove the final-GOAWAY reservation (an RST may take the last slot).
7. On overflow, set `writer_failed` instead of `failConnection` (today's behaviour).

## Report

- `slotBootBytes` at defaults, before (c30c99b) and after, measured by running it (not
  arithmetic), and the difference at `max_connections`.
- Anything in this brief that was wrong, with the quote that shows it.
- Things to file (do not file them).

## Gates

`./zb build test`, `./zb build ci` (the two `macos-libcxx-probe` steps fail on this Mac,
t-3540; every other step must pass), `./zb build tls-smoke`,
`./zb build -Doptimize=ReleaseFast`, `./zb build test-t3567 -Doptimize=ReleaseFast
-Dobserve=true`, `./zb build session-mu-gate`. Zig caches a passing test run; change a
comment in a test file to force a rerun. You cannot run Linux.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-3562` (branch `carmack/t-3562`,
  from master c30c99b). Never touch any other worktree.
- Base tripwire, before any work: `git -C /Users/rsaunderson/Source/mine/starh2-wt-3562
  log --format=%s -3` shows, newest first, "Add the build brief for t-3562", "Add the
  panel brief for t-3562", and "Wake the connection actor on every handler release of
  session_mu...". If not, STOP and report `STATUS: blocked`.
- `zig-pkg/` holds a copy of the pinned zio source (gitignored), already in place.
- Kill only processes you started, by pid. Never `pkill -f`.
- Commit per milestone. Each message is the decision record (problem, changes,
  benefits, alternatives rejected, what it depends on, statistics; markdown; plain ESL
  English). End it with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file.
- Report in `tools/t3562-report.md` (not committed). End with the status line the
  prompt gives, as the last line of your reply, not inside a list.
