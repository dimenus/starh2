# Brief: slot pool stage 3, no general-allocator call per connection or per request on HTTP/2

You are the implementer. A reviewer from another vendor reads your diff, and
the orchestrator reruns every test and measures RSS and speed itself. Do
not tell the orchestrator that tests pass; it recomputes them. Findings
reach you only through the orchestrator. Do not prompt, message or read any
other agent's pane.

## Why (settled; do not reopen)

Read the commit messages of e7cf701 (stage 1, connection slots) and e3c8186
(stage 2, request pool), and `tools/stage3-panel.md` (this stage's panel
brief). After stage 2, the RSS slope on nachos (THP off, smp) is 6.4-8.0
MB/cycle, about half load and half churn, and a few general-allocator (GPA)
calls remain per connection and per request. Under `smp_allocator` a block
freed on a later turn, often on another thread, strands slab space. This
stage removes the remaining calls on the HTTP/2 path (h2c and TLS).

## Decided design (do not reopen; if an item cannot work, STOP and show why)

Work in this order; commit each as its own milestone.

1. **Outbound frames and write chunks: backpressure, never the GPA.**
   - `rentFrame`: a frame up to `frame_slab_bytes` takes a frame slab, and
     on a miss tries the write pool (today the second branch is an
     `else if`); a larger frame takes a write chunk. `allocFrame` and
     `leaseWriteChunk` lose their GPA fallbacks.
   - When no memory is free, the drain stops, the way a full stash stops it
     (`stashFull`), and resumes when a chunk comes back; the actor waits in
     its existing select (`waitForActivity`: h2c on `write_ack_ch`, TLS on
     the completion queue). Nothing blocks or parks while holding
     `session_mu`; read the comment on `leaseWriteChunk` and keep its rule.
     Before a scheduler pop, require room for every push that pop can make
     (`wire_const.POP_PUSHES`).
   - A WINDOW_UPDATE is credited before its frame is made (`creditRecv`):
     do not retry an allocation in a way that credits twice.
   - A HEADERS block above the 16 KiB outbound frame size is split into
     HEADERS plus CONTINUATION frames, each fitting a write chunk. Today
     `cmdHeaders` allows one frame up to `u24`, which is also a protocol
     defect: the server caps its outbound frame size at 16 KiB.
   - Prove the backpressure path can neither deadlock nor spin: a test with
     a tiny write pool and many streams, on h2c and TLS, finishes every
     response.
2. **Control frames from the frame pool.** Every serialized control frame
   in `session.zig` that is copied with `self.gpa.dupe` today (the server
   preface SETTINGS and WINDOW_UPDATE, SETTINGS ACK, PING and PING ACK,
   WINDOW_UPDATE, RST_STREAM, every GOAWAY) goes through `allocFrame`.
   The preface is emitted after the frame hooks are installed (it must
   still be the first frame out). Terminal frames keep their reserve:
   check how `ControlPool`'s terminal reserve and the frame pool interact,
   and make a terminal frame unable to fail for lack of a slab (reserve
   slabs for it, or fail closed as today; say which and why).
3. **HPACK decoder table in the slot.** A fixed packed table: 4096 bytes of
   name and value storage plus 128 entry descriptors (the RFC's 32-byte
   per-entry overhead bounds the count), planned in `slotBootBytes`,
   compacted on insert after eviction so each name and value stays one
   contiguous slice. No `ArrayList`, no `dupe`. One constant for the table
   size, read by the preface SETTING, `setMaxSize` and `planBoot`. The
   RFC's `+ 32` stays in the size accounting and out of the storage, or
   the table evicts before the peer does. Transient literal decoding
   (`literalField`, `huffDecode`'s upper-bound buffer) uses the slot's
   existing `hpack_scratch`, never the table storage. A table size update
   above the limit stays a connection error.
4. **The rest.**
   - TLS leftover (`serveTlsThenBranch`): no `dupe`. Grok's panel answer
     says `leftover_buf` lives on the stack for the whole connection
     because `serveAccepted` is called from inside that function, and that
     `ingest` copies what it keeps. Verify that by reading before you rely
     on it; if it is not true, use a fixed region in the TLS slot.
   - Response header lists (`startCb`, the SSE / compression / Vary
     branch, `mergeVaryAcceptEncoding`): built in the request's arena, not
     the GPA. Add an outbound response-header bound (field count and
     encoded bytes) as a `Limits` field, checked before encoding; over it,
     the response fails like any other cap (500 before commit).
   - The idle-body expiry list (`stale` in `session.zig`): a fixed `[]u31`
     scratch in the slot plan.
5. **Out of scope:** Brotli (off by default; filed separately), HTTP/1.1
   (t-3537), the slot pool's own `ConnSlot.create` at a new peak.

## Counters and proof (required)

- Each item gets a counter of GPA calls that must stay 0 AND a counter of
  work done that must be positive in the same run (a zero with no traffic
  proves nothing): frames built, write chunks leased, control frames,
  HPACK insertions, TLS leftovers, response header preparations. Show them
  on the bench server's `/mem`.
- The end-to-end steady-state test (extend `tests/request_memory.zig` and
  the boot test in `tests/slot_reuse.zig`): after warm-up, h2c and TLS,
  with requests that carry incrementally indexed headers, a body above half
  the receive window (so WINDOW_UPDATE fires), an SSE start, a response
  header list that needs Vary, and pipelined TLS input, the GPA sees ZERO
  calls of any size from the HTTP/2 path. Say how you separate anything
  that is not the HTTP/2 path (for example the slot pool at a new peak).
  `preface_frames_per_connection` in the boot test goes to 0.

## Tests (required, in addition)

- Backpressure: a tiny write pool, many streams, every response completes,
  on h2c and TLS; the drain does not spin (count drain turns).
- CONTINUATION: a response header block above 16 KiB arrives intact
  through nghttp2 (`h2load` or the repository's h2 client) and decodes.
- HPACK table: fill, evict, compact; sizes and indices match a reference
  model over random insert sequences; a table size update to 0 clears it;
  above the limit is a connection error.
- Response header bound: refused at exactly the limit.

## Mutation tests (required; break the CODE, not the tests)

1. Restore the GPA fallback in `leaseWriteChunk`.
2. Keep the `else if` in `rentFrame` (no write-pool try for a small frame).
3. Let the drain pop without checking room for `POP_PUSHES`.
4. Store the `+ 32` in the HPACK table storage.
5. Skip compaction after eviction.
6. Emit one oversized HEADERS frame instead of CONTINUATION.
7. Put one control frame back on `gpa.dupe`.
8. Count the response-header bound off by one.

For each, run the full `./zb build test` and record which test fails.

## Gates

- `./zb build test`, `./zb build ci` (on this Mac `ci` always fails
  `macos-libcxx-probe`, two runs, t-3540; every other step must pass),
  `./zb build tls-smoke`.
- Zig caches a passing test run; change a comment in a test file to force
  a real rerun.
- The orchestrator measures RSS and speed on nachos.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-stage3` (branch
  `carmack/stage3`, from master e3c8186). Never touch any other worktree.
- Base tripwire, before any work: `git log --format=%s -3` shows, newest
  first, "Add the brief for slot pool stage 3", "Add the panel brief for
  slot pool stage 3", and "Take every HTTP/2 request's memory from a
  server-wide block pool". If not, STOP and report `STATUS: blocked`.
- `testdata/cert.pem` and `key.pem` are in the worktree.
- Kill only processes you started, by pid. Never `pkill -f`.
- If you start a long command in the background, do not end your turn
  without a STATUS line; wait for it, or say in the STATUS line what is
  still running.
- This is a significant refactor. If a decided item cannot work, STOP and
  report it with the code that shows why, before building around it.
- Commit per milestone. Each message is the decision record (problem,
  changes, benefits, alternatives rejected, what it depends on,
  statistics; markdown; plain ESL English). End it with
  `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file
  under "To file".
- Report in `tools/stage3-report.md` (not committed): commits, test
  output, each mutation and the test that caught it, the gate summaries,
  what you could not verify. End with `STATUS: done s3p1`.

## Round 2 (after review of 3d56cd3, and the t-3561 landing)

The orchestrator's gates on 3d56cd3 (Mac, fresh tree) pass. Master now has
t-3561 (233dbe3): an h2c write-ack ledger, pause hooks `h2cPause` /
`h2cPauseData` that hold a pop while the write-ack channel lacks room, a
push check active in every build, and `waitForAckRoom` / `pushBareBarrier`
for bare flush barriers. Read its commit message first.

1. **Rebase onto 233dbe3.** Both branches install h2c pause hooks.
   Merge them into ONE rule per hook: a pop is held when the write pool
   lacks room for `POP_PUSHES` OR the ack channel lacks room for what the
   pop can add (DATA keeps one control pop's reserve, as t-3561 does).
   Keep both sets of tests passing; say in the commit how the two
   conditions combine and why the order cannot starve either.
2. **High: a bare flush barrier can park in `putOne` under `session_mu`.**
   Zero-length barriers take a write-queue slot but no pool chunk; with a
   non-reading peer the 8-slot queue fills and the next `Body.flush()`
   parks in `putOne` holding the lock, so the actor cannot run deadlines or
   graceful shutdown. After the rebase, extend t-3561's `waitForAckRoom`
   shape so a bare barrier waits OUTSIDE `session_mu` until there is room
   for it in BOTH the ack channel and the write queue, then pushes without
   parking. Decide whether the `putOne` park in `pushWriteChunk` is then
   unreachable; if so remove it and assert (t-3564). Test: many streaming
   handlers flush with nothing pending against a non-reading peer; a
   graceful shutdown and a deadline still complete. It must hang or fail
   on 3d56cd3 and pass after.
3. **Medium: accepted minimum frame-pool settings cannot boot.** 9-byte
   slabs, one slab and three chunks pass `resourceUpperBound`, but the
   13-byte preface WINDOW_UPDATE cannot get memory, and small slab counts
   give a terminal reserve of 0. Validate at boot: the slab size and count
   must hold the preface and at least one terminal frame (GOAWAY, RST), with
   the reserve > 0; check encoded frame sizes, not the 9-byte header. Test:
   the bad minimums are InvalidConfig; the smallest valid ones boot.
4. **Medium: a response with more than 32 headers is refused.** The
   32-entry stack in `prepareResponseHeaderFields` returns
   `ResponseRefused` before the configured bound (100 fields by default) is
   checked. Use the request arena when the list exceeds the stack; keep the
   stack as the small-list fast path. Test with the DEFAULT bound: 33 and 99
   application headers are sent; 100 is refused.

Mutations: the merged pause rule with either condition removed; the barrier
pushing without the write-queue wait; the boot validation removed; the
stack-only path restored. Same rules. Add a "Round 2" section to the report.
End with `STATUS: done s3r2`.

## Round 3 (after review of e75f292)

The orchestrator's gates on e75f292 (Mac, clean cache) pass; the nachos
RSS and perf A/Bs are running on e75f292. The review accepted the merged
pause rule, the barrier wake, the refusal paths, the PING change to the
t-3561 test (the hold removes the real wake; production has it), the exact
limits, and all three round-1 fixes. One new defect:

1. **High: the teardown sentinel breaks the queue room check.** On an
   error exit, teardown puts a zero-length sentinel into `write_ch`
   (`putUncancelable`) that is not counted in `acks.owed`, so
   `h2cQueueRoom`'s fast path (`owed + need <= capacity`) can pass while
   the queue is full; a handler flushing at that moment hits the full-queue
   trap in `pushWriteChunk` (safe builds panic; release builds fail the
   connection). A peer can make the shape by stalling writes and provoking
   an ingest error while a task handler flushes. Fix (decide and say why):
   publish an atomic pump-stopping state before the sentinel goes in, and
   while it is set take no fast path and treat a full push as an ending
   connection (no trap); or replace the sentinel with closing the queue and
   keep `Closed` distinct from a full queue through `tryPut`. Test: drive
   that teardown while a handler flushes (test hooks are fine); it must trap
   on e75f292 and pass after. Mutation: remove the new state check.

Same rules. Add a "Round 3" section to the report. End with
`STATUS: done s3r3`.

## Round 4 (the h2c kb=4 regression)

The review of bb6501e found no defect ("land"); the orchestrator's gates on
bb6501e pass. The nachos A/Bs on e75f292 (master 645c0ef as base):

- RSS (THP off, n=3): slope 6.9-7.8 against master's 6.6-7.8 MB/cycle, no
  difference; GPA calls from cycle 2 to 10 fell from 8.77 M to about 0.7-0.8 M
  (the rest is about one per new connection, zio's task allocation). So
  stage 3 removed the calls but the remaining slope is not starh2's HTTP/2
  GPA traffic. That is a separate investigation; not this round.
- Throughput (6 rounds): h2c kb=60 +67 %, TLS mixed +2.4 %, TLS shapes flat,
  but **h2c kb=4 -5.7 %** (2.832 M against 3.003 M req/s, z -2.56; the
  ranges barely touch). This round fixes that.

What the orchestrator measured (bench `--count-gpa`, h2c, `-c 128 -m 32
-n 10000000 /fragment?kb=4`, `/mem` `outbound` deltas):
`frames_from_chunk` 10,000,001 (every response's 4 KiB DATA frame takes a
whole write chunk), `chunks_leased` 2,004,896, `drain_memory_pauses`
11,596,862 (more than one per request), `drain_turns` 5,357,806. A CPU
profile shows more actor turns (`waitForActivity`, `h2cPauseFor`,
`ep_poll_callback` up). With 8 chunks and `POP_PUSHES` kept free, about 6
small frames per connection fit in flight, so the drain keeps pausing; on
master the pool overflow went to the GPA and never paused.

1. **Pack small frames into shared write chunks.** A DATA frame (and HEADERS)
   smaller than a chunk should be framed into the chunk the drain is already
   filling, the way control frames are batched (`EmitBatch`), not take a
   chunk of its own; a chunk goes to the pump when it is full or the drain
   turn ends. Keep every invariant: the ack ledger counts chunks, not frames;
   the pause rule's room per pop still covers the worst case; per-frame
   completions that carry tickets still release their tickets in order
   (check how ticket chains and receipts are attached to a chunk today);
   HEADERS-before-DATA order and CONTINUATION adjacency hold. If packing
   cannot keep one of these, STOP and say which.
2. **Measure before and after** on this Mac with the same `/mem` counters
   (`frames_from_chunk`, `chunks_leased`, `drain_memory_pauses`,
   `drain_turns` per request) and report them; the orchestrator reruns the
   nachos A/B. Target: h2c kb=4 within noise of master, kb=60 gain kept.
3. Tests: a test that sends many small responses on one connection and
   asserts several frames share a chunk (frames per chunk > 1) and that every
   response arrives in order; mutation: one frame per chunk again.

Same rules. Add a "Round 4" section to the report. End with
`STATUS: done s3r4`.

## Round 5 (Linux and the full build)

The review of d121e3a found no defect ("land"); the orchestrator's Mac
gates on d121e3a pass. On nachos (Linux), 10 rounds alternated against
master 645c0ef: h2c kb=60 +57.5 %, TLS kb=4 +15.7 %, h2c kb=4 +2.0 % (the
regression is gone), TLS mixed +1.2 %, the rest flat. The pool-size option
is not needed. Two problems remain:

1. **A full ReleaseFast build fails:** `tools/pipeline_bench.zig:146`
   calls `.decode` on `error{OutOfMemory}!core.hpack.Decoder` (round 1 made
   `Decoder.init` fallible). Fix it. Then make this kind of break
   impossible to miss: check which build artifacts `./zb build ci` does
   NOT compile (`pipeline_bench` is one), and add a ci step that builds
   every installed artifact and every tool, at least for the native target.
   Say what you added.
2. **The barrier test fails on Linux every time:** on nachos, 10 of 10
   direct runs of the test binary fail `outbound_memory: bare flush
   barriers against a non-reading peer ...` with `barrier queue test:
   expected 12, found 1` (tests/outbound_memory.zig:804). Hypothesis (not
   checked): Linux loopback socket buffers are much larger than macOS's,
   so the "non-reading peer" never makes the pump block, and the barriers
   never wait. Make the test create its own back-pressure on every OS (for
   example a small `SO_RCVBUF` on the client socket set before connect, and
   a small `SO_SNDBUF` on the server side if needed; or fill until the pump
   reports a blocked write) and ASSERT that precondition (the pump is
   blocked) before the barriers start, so on an OS where it cannot be
   created the test fails loudly at the precondition, not at the result.
   The orchestrator runs it on Linux; you cannot.

Same rules. Add a "Round 5" section to the report. End with
`STATUS: done s3r5`.

## Round 6 (after review of c784ede)

The orchestrator's checks on c784ede: Mac gates and the full ReleaseFast
build pass; on nachos the Linux suite passes and the barrier test failed 0
of 10 (10 of 10 before). The review found two defects in round 5:

1. **The barrier test's precondition does not prove the pump is held.** It
   proves no write finished for 200 ms; the pump can read
   `test_hold_writes` as false, be descheduled, then write and drain the
   barriers, so the original `putOne` defect could pass. Add a test-only
   acknowledgement (a counter or event the `WritePump` sets when it enters
   the hold loop) and wait for it before releasing the flushers.
2. **`bytes_to_pump` / `chunks_to_pump` add a process-global atomic RMW to
   every pump handoff in release builds** (connection.zig ~6202), after the
   throughput A/B was measured, and they count before the push succeeds.
   Remove them from the hot path: compile them only under the
   test-observation option (and roll back on a failed push), or replace
   their use in the test with the acknowledgement from item 1. Say which.

Same rules. Gates as before, plus the full ReleaseFast build. Add a "Round 6"
section to the report. End with `STATUS: done s3r6`.
