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

## Round 2 (review of b018439; gpt-6-astra, Grok 4.7, and the orchestrator)

Orchestrator's runs at b018439: Mac gates pass (ci: only the libcxx probes
fail). The bound held in every run that got past setup (`ring_high` never
above T, `terminal_full` 0, one RST per stream, the right GOAWAY codes). My
mutation (room for 2 GOAWAYs, not 3) fails 11 tests. But the new tests are
FLAKY: the `t3562` binary failed 1 of 10 on the Mac and **6 of 10 on Linux**,
and one full Mac `test` run failed, always at "timed out waiting for every
handler to start its response", on both transports. Fix as new commits on top.
The round 1 rules still apply.

1. **The setup flake (first; evidence before any fix).** Make the setup
   timeout print why: for each handler, whether `resp.start` returned and with
   which error, and the counters that explain a refusal (ordinary `PoolFull`,
   `tryAdmit` refusals, handler-slot refusals). Run until it fails and show
   the line. Both reviewers note the ordinary share is 256 = M, so M HEADERS
   plus an ack can be `PoolFull` (it was 240 at the base: the same ceiling).
   If that is the cause: it is a per-stream failure of a well-behaved peer's
   burst, a pre-existing limit, and NOT this task. The orchestrator files it.
   Set the test's ordinary share explicitly (`control_entries_per_connection`
   = M + the slack you show is needed), with a comment that says why. If the
   cause is anything else, STOP and report it with the evidence.
   Then run the `t3562` binary 30 times and report the count.
2. **The intent queue misclassifies the server's own resets as a flood
   (high; both).** Each RST takes 2 intents, and they pile up for a whole
   `Session.ingest` or a whole batch before `processIntents`. At the default
   512, the 254th RST in one batch hits `intent_room < 5` and sends
   ENHANCE_YOUR_CALM to a well-behaved peer: `finishGraceful` (resets every
   live stream, then materializes once), server-wide `tryAdmit` refusals of
   one chunk's HEADERS, and deferred task-spawn failures. At M = 1024 one
   chunk of 1024 admitted header-only requests already overflows (one
   `dispatch_request` each). **Fix (decided):** derive the minimum intent
   room from M and check it at boot (`InvalidConfig` below it). Count every
   producer that can co-queue in ONE batch for a well-behaved peer: up to M
   stream opens (1 intent admitted, 2 refused), the RSTs and GOAWAYs of the
   terminal bound (2 per RST), and the ack/ordinary intents one chunk can add
   (derive their bound from the rate limiter or the frame sizes; show it).
   Raise the default `intent_entries_per_connection` to meet it. After that,
   the `< 5` rule may fire only for a peer past T. Tests: a well-behaved
   peer at defaults whose M streams are all refused by `tryAdmit` in one
   chunk keeps its connection; the same at M = 1024; a graceful shutdown with
   M live streams sends M RSTs and no ENHANCE_YOUR_CALM. Report the memory of
   the larger intent queue.
3. **A peer's RST does not retire a queued server RST (high; astra).** In a
   stall, the server queues RSTs; the peer cancels those streams itself
   (counts them closed) and opens new ones, which fail and need RSTs. The
   peer RST path (`session.zig` near 1095, "There is nothing to do. The
   stream is already gone") leaves the server's unsent RST queued, so the
   ring can hold RSTs for streams the peer no longer counts open, and a
   conforming peer reaches ENHANCE_YOUR_CALM. **Fix:** when the peer resets
   a stream whose server RST is not yet written, drop that queued RST (the
   ring entry, its terminal share and its store slot; any pending intent for
   it too). Test: M server RSTs queued in a stall, the peer resets all M and
   opens new streams that fail; no ENHANCE_YOUR_CALM, connection kept.
4. **A refused final HEADERS leaves the peer waiting forever (high; astra).**
   An empty response (`cmdHeaders` with end_stream) calls
   `localEndStream` + `releaseConcurrency`, which tombstones the stream with
   `.no_error` before the scheduler accepts the HEADERS. If `enqueueControl`
   then refuses it, the handler's recovery RST (`connection.zig` near 7469)
   is suppressed by `rstOwed` (tombstoned), and the client gets neither.
   **Fix:** a locally planned END_STREAM must not count as closed for
   `rstOwed` until its HEADERS is accepted; on refusal, the recovery RST must
   go. Test first: show it hangs at b018439 (a deadline that FAILS), then
   passes.
5. **Absence of a tombstone must not authorize an RST for an old ID (medium;
   astra).** Tombstones are an overwrite ring; a delayed handler reset can
   arrive after its stream's tombstone was evicted and queue an RST for a
   stream the peer counts closed. **Fix:** an absent, untombstoned ID gets an
   RST only when the caller is refusing a NEW stream (the refusal paths say
   so explicitly); for any other absent ID, `rstOwed` is false. Recheck the
   handler's terminal cause under the lock before a delayed reset.
6. **Ticket table (low; astra).** No terminal frame creates a ticket owner;
   it can only carry a handler's existing ticket. Restore the ticket table to
   ordinary entries + streams in `planBoot` and `init`, unless you find a
   terminal path that creates a new ticket owner (then show it). Expected
   cost after this: about 13.5 KB per connection plus item 2's intents;
   measure `slotBootBytes` again by running it.
7. **The stall tests do not prove the stall (medium; both).** Record that
   `pause_control` was true when the RSTs were queued (an observe counter
   read by the test), and assert the expected stream IDs, not a count over
   all odd IDs. The flood scenarios ingest the flood after the hold is
   released, so they do not test a flood during a stall: either make the
   actor ingest while the writer is still held (if the design allows it;
   astra notes the TLS stash hold parks the actor), or rename them to say
   they test the bound after a stall, not during it. Check that the
   connection closes by its closing deadline (`close_flush_timeout_ns`).

Rerun round 1's mutations 1 to 8, plus: (9) `rstOwed` trusts the `.no_error`
tombstone of an unaccepted END_STREAM (item 4); (10) the peer RST no longer
retires the queued RST (item 3); (11) the intent boot check removed with the
default back at 512 (item 2). Full `./zb build test` each. Report in
`tools/t3562-report.md` (overwrite). End with the status line the prompt
gives, as the last line of your reply, not inside a list.

## Round 3 (review of b6fb950; Fable, gpt-6-astra, and the orchestrator)

Orchestrator's runs at b6fb950. Mac: gates pass on a clean cache (ci: only
the libcxx probes fail); the `t3562` binary passed 30/30 alone and 30/30
three at a time. My mutation (the edge drops the rest of a read chunk after
a short `ingestSome`) fails 7 tests and crashes 1. **Linux (nachos, a fresh
tree): the full suite passed once, but the `t3562` binary failed 20 of 30
alone and 24 of 30 three at a time.** Two failure kinds, quoted from the
logs (`/tmp/t62b-a*.log` on nachos):

- 12 of 20: `flood during a stall` (both transports) reaches the bound
  (`ring_high=259 rst_paused=258 terminal_full=0`), then
  `rst_total=0 goaways=0 goaway_code=0 eof=true close_ms=0 data_frames=0`,
  and `t3562: stream 513 got 0 RST_STREAM, want 1`. The client got NOTHING:
  no RST and no GOAWAY. That is the failure this task exists to remove.
- 8 of 20: `flood during a stall, tls` times out at "waiting for the
  terminal ring to reach T", `T3562-flood-timeout ring_high=251
  rst_paused=45`: the stall did not hold.
- Also seen in a PASSING run: `flood during a stall that never ends, tls`
  closed at `unread_close_ms=4` (h2c in the same run: 999). So the TLS
  connection did not wait for its closing deadline, and the test passed.

Ryan decided two points in this round (items 3 and 4). Fix everything as
new commits on top of b6fb950. The round 1 and 2 rules still apply.
**Linux is a required gate from now on** (recipe at the end).

1. **The peer gets nothing on Linux (first; evidence before any fix).**
   Find why the connection ends within ~1 ms of reaching T with nothing
   delivered. Instrument; do not guess: the `CLOSE_REASON` (by, site), when
   the server closes the socket relative to the GOAWAY being written, how
   many bytes are unread in the server's receive buffer at close, whether
   the test client half-closes, and whether the ReadPump shuts `.send` on
   a read EOF or error (both reviewers point at that path). An unverified
   hypothesis to test, NOT a fact: on Linux, closing a socket with unread
   received bytes sends a TCP RST, and the peer then drops data it has not
   read yet. Show the evidence line, then fix it so a flooding peer
   receives its GOAWAY(ENHANCE_YOUR_CALM) before the close (Ryan's
   decision: "goaway then close"). If the cause is pre-existing, say so
   and fix it anyway: the GOAWAY is this task's deliverable.
2. **The stall tests must hold the stall on both systems.** The TLS flood
   on Linux lost the stall (`rst_paused=45`). Both reviewers: the stall is
   proved by ONE sighting of a pause, and the TLS "every flusher waits"
   counter stays raised during the 2 ms sleep after a SUCCESSFUL flush, so
   it can read "all waiting" while none is blocked; the h2c
   `crossed_resets` and `flood` cases have the same one-sighting shape
   (the recorded 1-in-30 Mac flake). Synchronize on a sustained blocked
   state: count only flushes that are blocked waiting for room, and on h2c
   wait until the write pump is blocked and the queue is full, before the
   aborts or the flood. Then assert exact counts. Gate: the `t3562` binary
   30/30 alone and 30/30 three at a time, on the Mac AND on Linux.
3. **An ordinary refusal inside a batch (Ryan: fix it in this branch).**
   Pre-existing at c30c99b. When `enqueueControl` refuses an ORDINARY frame
   (a PING or SETTINGS ack, a WINDOW_UPDATE, a response HEADERS),
   `materializeBatch` calls `finishIntentsAfterEnqueueFail` on the whole
   rest of the batch, which releases other streams' dispatches, RSTs,
   early-reject HEADERS and even a GOAWAY, unsent and unanswered. On the
   read path it then returns the error up through `ingestAndProcess`
   (`try self.processIntents()`) and `ingestWireChunk`, and `run` tears the
   connection down with no GOAWAY, while the actor's main loop treats the
   same error as per-stream (`failClosedOnIntentError`). **Fix:** drop only
   the refused frame and the CONTINUATION fragments of its own block; keep
   processing the rest of the batch; an ordinary refusal never ends the
   connection from the read path (only a terminal refusal may). Every
   stream whose final frame is dropped gets one RST: the handler path
   already does (633839a); an early-reject HEADERS refused while the
   stream is still in the map has no handler, so the edge must queue it.
   Test: during a stall that pins the ordinary share, the peer sends a PING
   and new requests in one chunk; the connection is kept, and every stream
   gets its response or one RST.
4. **The ticket travels with its frame (Ryan's decision).** Today
   `next_wire_ticket` attaches to whatever outbound frame is materialized
   first. That is right only if the intent queue is empty when a handler's
   lock section starts, and it is not: `sendCb` (`fits_without_wait`
   branch) queues HEADERS, then `enqueuePending` -> `reserveBudget` ->
   `parkForBudget` unlocks `session_mu` with those HEADERS still queued,
   and an error return from `enqueuePending` also leaves them queued. Then
   another handler's ticket goes onto the first handler's frame. The same
   leftover breaks `ingestAndProcess`'s "the queue is empty on every call"
   panic at a small M. **Fix:** carry the ticket in the intent of the frame
   it belongs to, and delete the implicit hand-off; the wrong attachment
   must become impossible to write, not a convention. Keep DATA behind its
   HEADERS (t-482). Test: handler A parks for connection budget with its
   HEADERS queued; handler B sends an empty response with a ticket; B's
   ticket completes with B's frame and A's with A's.
5. **Retire a crossed RST at once, and without the tombstone (astra; Fable
   agrees on the eviction half).** (a) The `retire_rst` intent frees
   nothing until it is materialized, but `queueRst` rents a terminal slot
   during ingest (`self.controlFrame(buf, .terminal)`). So in ONE read
   chunk, a peer that cancels 3 streams the server already reset and opens
   3 new ones that `tryAdmit` refuses at ingest gets ENHANCE_YOUR_CALM with
   at most M streams open. The `crossed_resets` test misses it because its
   new failures come at dispatch. (b) The retire runs only if the stream's
   `server_rst` tombstone is still in the 1,024-entry ring. **Fix:** on a
   peer RST for a stream that is not in the map, retire the unwritten
   server RST for that id during ingest (a callback like `controlFrame`),
   by searching the terminal ring by id; do not consult the tombstone.
   O(T) per peer RST, which is rate limited. Tests: the one-chunk case of
   (a) at defaults; the evicted case if a test can reach it in under 2 s.
6. **The unaccepted END_STREAM is an obligation, not history (astra; Fable
   on the zero case).** `resetRefusedEnd` reconstructs "the edge refused
   this final HEADERS" from a `.closed/.no_error` tombstone. With
   `stream_tombstones = 0` (accepted today) nothing is found and the peer
   waits forever; and if the peer cancels the stream between the queuing
   and the refusal, an RST goes to a stream the peer counts closed. **Fix:**
   track the obligation where it is created (the final HEADERS waiting for
   acceptance) and clear it on acceptance or on a peer RST; do not read it
   back from the tombstone ring. If `Tombstone.ending` is then no longer
   needed, remove it: it widened every tombstone from 8 to 12 bytes, 4,096
   bytes per connection (a fifth of this task's memory, named in no commit
   message). If it stays, name it in the commit message.
7. **Test claims (astra).** The "stall that never ends" tests accept an
   immediate close (see the TLS 4 ms run above): require the closing
   deadline to expire with output blocked and no writer failure, by the
   close reason. `server_failures` allows 16 RSTs outside the pause, and on
   TLS the graceful GOAWAY is only queued after the hold ends: prove M RSTs
   and the graceful GOAWAY wait together, or rename the test to what it
   proves.
8. **Small (Fable).** `fair_scheduler.zig` ~193: `rstStream` was inserted
   under `continuationStream`'s doc comment; move it. The observe-build
   pause probe (`connection.zig` ~5976) calls `outboundMemoryPaused`, which
   bumps the production counter `drain_memory_pauses`; read the pause
   without the side effect.

Rerun mutations 1 to 16 from rounds 1 and 2, plus: (17) the ticket attaches
to the first frame materialized again (item 4); (18) an ordinary refusal
releases the rest of the batch again (item 3); (19) the crossed RST retire
is deferred to materialization again (item 5a); (20) the END_STREAM
obligation is read from the tombstone again (item 6); (21) the item 1 fix
undone. Full `./zb build test` each, on the Mac. Report the
`slotBootBytes` at defaults by running it.

### The Linux gate (nachos)

Copy the committed tree; do not reuse an old directory:

    git archive HEAD | ssh nachos 'rm -rf ~/src/s-t62-r3 && mkdir -p ~/src/s-t62-r3 && tar -x -C ~/src/s-t62-r3'
    ssh nachos 'cp ~/src/s-t62b/testdata/*.pem ~/src/s-t62-r3/testdata/ && cp -R ~/src/s-t62b/zig-pkg ~/src/s-t62-r3/'
    ssh nachos 'export PATH=$HOME/.local/share/mise/shims:$PATH; cd ~/src/s-t62-r3 && ./zb build test'

Then run the built `t3562` binary (`.zig-cache/o/*/t3562`) 30 times alone
and 10 rounds of 3 at once, by exit code and by "All 9 tests passed" (or
the new count). Report the counts and quote one line of any failure.

## Round 4 (review of b18951a; Fable, gpt-6-astra, and the orchestrator)

Orchestrator's runs at b18951a: Mac gates pass on a clean cache (ci: only
the libcxx probes fail). Linux: the full suite passed twice (the second on a
clean cache); the `t3562` binary passed 30/30 alone and 30/30 three at a time
on BOTH systems. The item 1 failure is gone. Perf A/B on nachos (c30c99b vs
b18951a, n = 6 per arm, alternating): every shape within noise; h2c
complete4 median -3.8% with wide overlap (z -1.12), to be re-measured on
the final tree. Memory: +15,832 bytes per connection, matched by both
reviewers from the code.

**My mutation survived:** `closeFlushed` returning `false` instead of
`self.closing_peer_eof` (the server ignores the peer's EOF and every closing
connection waits for the deadline) passes the whole suite. The flood client
still sees `eof=true close_ms=1`, from the server's FIN, and
`test_closing_ended_peer_eof` is counted BEFORE the mutated return. Nothing
checks when the SERVER closes its socket.

Ryan decided two points in this round: the owed-state shape in item 3, and
fixing all four pre-existing defects in item 6. Fix everything as new
commits on top of b18951a. The round 1 to 3 rules still apply, and Linux
remains a required gate.

1. **The END_STREAM obligation misses a final HEADERS the PEER's END_STREAM
   closes (high; both reviewers).** `cmdHeaders` sets
   `owes_rst_if_refused` only if the stream already left the map. An early
   413 (`emitEarlyReject`) is queued while the peer's side is open, then the
   peer's DATA(END_STREAM) in the same chunk closes and removes the stream
   (`onData`, `releaseConcurrency`). If that HEADERS is refused,
   `resetRefusedHeaders` finds neither a map entry nor an obligation: no
   response and no RST, forever. The trailers path (`onHeaders` ~1821) has
   the same gap. **Fix:** every final HEADERS carries the obligation
   (`owes_rst_if_refused = end_stream`), cleared on acceptance, on a peer
   RST, and when a server RST for that stream is queued after it (no second
   RST). Test: `refused_batch` with the POST's body and END_STREAM in the
   same write; and a trailers case.
2. **A refused CONTINUATION still ends the connection (high; both).**
   `materializeBatch` treats `block_queued_open` (a fragment ENQUEUED, not
   written) like a terminal refusal: it drops the rest of the batch, sets
   `writer_failed`, and every stream dies. A two-fragment block is possible
   at defaults (32 KiB `response_header_bytes`, 16 KiB frames). **Fix:**
   admit a header block as one unit: ask the control pool for the block's
   total entries and bytes before the first fragment; if it does not fit,
   refuse the whole block as an ordinary refusal (item 1's RST rule then
   applies). Test: one ordinary entry left, a two-fragment response; the
   connection stays and that stream gets one RST.
3. **Connection-level obligations survive a refusal (Ryan's decision:
   owed state, retry).** Today a refused SETTINGS ack, PING ack or
   WINDOW_UPDATE is dropped for good. RFC 9113 requires the acks (a peer may
   end with SETTINGS_TIMEOUT; Go and gRPC close on a missed PING ack), and a
   dropped WINDOW_UPDATE strands credit when the window is at zero (no DATA
   comes to trigger a new one; `session.zig` ~1063-1074 relies on one).
   **Fix:** Session keeps what it owes: a count of SETTINGS acks; the PING
   ack payloads, bounded (past the bound the peer is flooding:
   ENHANCE_YOUR_CALM, derived and boot-checked like the other bounds); the
   unsent WINDOW_UPDATE credit as a sum per stream and for the connection.
   Re-queue them at the next drain that has room, in the order RFC 9113
   needs (each SETTINGS ack after its SETTINGS was applied). Coalesce, so
   the memory is a few bytes; report it. The graceful PING may stay
   droppable (its deadline still runs): say so in the code. Tests: during a
   stall that pins the ordinary share, the peer sends SETTINGS and PING and
   uploads until its connection window is 0; after the stall it receives
   the SETTINGS ack, the PING ack with its payload, and enough
   WINDOW_UPDATE credit to finish the upload.
4. **Error exits leave intents queued (high; astra, confirmed by reading).**
   `controlFrame(.ordinary)` returns `OutOfMemory` when `rentFrame` finds no
   frame slab or write chunk, and the handlers pass it up with `try`
   (`session.zig` ~903 PING ack; `beginGracefulPhase1` ~930: GOAWAY queued,
   then the PING allocation fails). The lock section then ends with intents
   queued, which hits round 3's `unlockSession` panic in safe builds, and in
   release builds leaves them for the next section. astra's reachable case:
   `frame_slabs_per_connection = 2` and nine PINGs in one read. **Fix:**
   every error exit materializes or releases what it queued before the
   unlock, and keeps any terminal output. With item 3, a failed ordinary
   allocation for an ack should become owed state, not an error. Keep the
   panic. Test: astra's case, and graceful phase 1 with no ordinary frame
   left.
5. **Tests.** (a) My surviving mutation above: assert when the SERVER
   closes the socket after the peer's EOF (well before the closing
   deadline), from a counter written at the actual close, not in
   `closeFlushed`. (b) Mutation 13 (stop at the first failed round)
   survives; Fable shows the later rounds are still reachable: a batch with
   the terminal store full, a refused final HEADERS, then an RST. Add that
   Connection test (with the mutation the unlock panic fires). (c)
   `server_failures` opens the abort gate without waiting for
   `test_stalled` (astra): wait for it, as the flood and crossed cases do.
   (d) Fable: with the h2c writer held by construction, the round 3 item 1
   path (GOAWAY in the kernel queue, a late PING, a TCP RST) no longer runs
   end to end on h2c. Keep one case with the real socket stall (it may run
   on Linux only, where the kernel queue takes the GOAWAY), and say in the
   test comments what each hold proves and what it does not.
6. **Pre-existing, fixed here (Ryan: "why not fix all 4?").**
   (a) TLS never writes close_notify, on any close (`grep -i close_notify
   src` finds only the inbound event; c30c99b too). Before the send-side
   shutdown in the lingering close, stage close_notify through the TLS
   cipher and drain it under the same deadline. Test: a TLS client reads
   the GOAWAY, then close_notify, then EOF.
   (b) The h2c ReadPump shuts the send side on a read EOF or error
   (`wire_pump.zig` ~217-225); TLS calls `failDrain` on EOF/close_notify
   (`tls.zig` ~1431-1442, 1527-1534). A peer that half-closes loses queued
   GOAWAY/RSTs, and a peer EOF before the closing phase skips it (the
   `leaving` branch, `connection.zig` ~4337). Only the actor shuts the send
   side (`closeFlushed`); an EOF is an event, an I/O error is a failure.
   Test: a peer half-closes during a stall; it still receives the GOAWAY.
   (c) A cancellation-shielded handler holds the task, slot and fd past the
   closing deadline, because exhaustive handler teardown runs before the fd
   close (`connection.zig` ~4513). A handler that shields itself cannot be
   stopped from inside the process (zio's cancel waits), so the fix is the
   order: close the socket at the deadline BEFORE joining the handlers;
   the slot and task stay until the handler returns, under the existing
   watchdog. Say that in the code. Test: a shielded handler that sleeps past
   the deadline; the peer sees the close at the deadline.
   (d) `test_hold_writes` (`wire_pump.zig` ~453-463) is not compiled out of
   non-test builds. Gate it like the other hooks (`comptime`), and check
   the rest of the hooks the same way.
7. **Small.** Fable: `waitStalled`'s nudge PINGs add PING acks during the
   stall; with item 3 they become owed, so check the tests still prove what
   they claim.

Rerun mutations 1 to 21, plus: (22) every final HEADERS carries no
obligation unless it left the map (item 1); (23) a block admitted one frame
at a time again (item 2); (24) owed acks dropped instead of retried (item
3); (25) an error exit returns without releasing its intents (item 4);
(26) `closeFlushed` ignores the peer's EOF (my survivor); (27) no
close_notify (6a); (28) the ReadPump shuts the send side on EOF again (6b);
(29) handlers joined before the socket closes (6c). Full `./zb build test`
each, on the Mac. Run the Linux gate as in round 3. Report `slotBootBytes`
at defaults by running it.
