# Brief: option A (t-3538), real backpressure on the connection outbound budget

You are the implementer. A reviewer from another vendor reads your diff, and
the orchestrator reruns every test and measures speed itself. Do not tell
the orchestrator that tests pass; it recomputes them. Findings reach you
only through the orchestrator. Do not prompt, message or read any other
agent's pane.

## Why (settled; do not reopen)

Read `tools/option-a-panel.md` (the problem and the measured failure) and
the commit messages of 233dbe3 (t-3561) and 3ba492c (slot pool stage 3).
A connection's outbound budget (`outbound_bytes_per_connection`, 4 MiB)
FAILS when full: a refused handler write resets its stream; the
pending-to-wire move counts a DATA frame twice and ends the connection
(`data_sink_catch`); on TLS the reset burst overflows the terminal queue.
The owner chose real backpressure. A four-model design panel (GPT-5.6 Sol,
Grok 4.7, gpt-6-astra, Fable) settled the design below; where it split,
the decision is stated.

## Decided design (do not reopen; if an item cannot work, STOP and show why)

1. **The budget counts DATA payload bytes a handler wrote, once.** Frame
   headers, HEADERS, CONTINUATION and every control frame are NOT charged;
   their memory is already bounded by the control pool, the frame pool and
   the write pool. A byte is reserved once, in `enqueuePending`
   (connection + server cap), and travels with the bytes:
   - The drain TRANSFERS pending to wire (`pending -= n; wire += n`; total
     and the server counter untouched). It never reserves. Never implement
     the move as "release, then reserve": another connection could take
     the gap through the server counter and end this connection inside the
     drain.
   - Remove the `.wire` reserve in `sendAccountedWire`
     (`tryReserveOutboundBytes(src.len, .wire)` and its refusal record).
   - A chunk carries the DATA payload bytes it holds, not `src.len`; the
     ack releases exactly that. The emit batch accumulates payload
     separately from length (`EmitBatch`: `adopt`, `copyFrame`, `take`),
     because a batch packs small DATA frames with controls; otherwise every
     ack over-releases and trips the safe-build over-release panic.
   - Both sinks (the drain sink and `drainEmitReceipt`) transfer after the
     queue step succeeded, with the same `wakeStreamSpace`.
   - Keep an assert: `outbound_held == pending_outbound_held +
     wire_outbound_held`.
2. **Three traps the transfer must not fall into** (from gpt-6-astra;
   verify each against the code):
   - A chunk must still post a completion when it carries no payload
     credit (an empty END_STREAM DATA frame, a control-only chunk):
     `postsCompletion` / `countsChunk` must not depend on
     `outbound_release` alone.
   - The h2c closing decision must not use the payload counter to mean
     "everything is written" (`wire_outbound_held != 0` in the closing
     check); use a complete transport count.
   - Already-framed DATA (`Session.cmdData` -> `enqueueFramedData`) has no
     pending reservation. If it has no production producer (Fable found
     only tests use `respond_data`), make a production use fail loudly
     (an assert or a compile-time test-only gate) and say so; otherwise
     give it credit.
3. **A task handler parks in `enqueuePending`**, in place of today's
   refusal, after the stream-slab wait (slab room first, then budget room
   for `take`; `take` never exceeds one stream's cap). Use
   `waitForStreamSpace`'s lock protocol: check the terminal cause and
   `writer_failed`, reset the slot's event, unlock `session_mu`, select on
   the event and `dead`, relock uncancelably, recheck.
4. **The waiter queue: FIFO with a handoff reservation, under its own small
   lock.** Wire releases run WITHOUT `session_mu` on both transports (h2c
   `drainWriteAcks` before the lock block; TLS acks in `drivePending`), so
   the queue cannot be guarded by `session_mu`. Use a dedicated
   `zio.Mutex` (lock order: `session_mu`, then the budget lock; never held
   across a wait). Put the wake INSIDE `applyOutboundRelease`, the one
   function every release already calls (drain transfer, ack, slow
   consumer, reset, writer failure, drain catch), so no release site can
   forget it.
   - Each waiting slot has an entry (stream id, need, granted, next). On a
     release, while the head's need fits, reserve it for the head
     (connection and server cap), unlink it, set its event. The woken
     handler finds its bytes granted and copies them.
   - A newcomer joins the tail whenever the queue is not empty, even if
     room exists (no barging). A large writer parks again at the tail for
     its next piece (round robin by construction).
   - Wake only granted waiters; no wake-all on a release (wake-all stays
     for terminal events).
   - If the handoff fails the SERVER cap, fail that head (wake it with the
     server refusal) instead of leaving it queued: nothing on this
     connection would ever retry it.
   - Every exit (cancel, stream reset, connection end, slow consumer,
     writer failure) unlinks the entry and returns an unused grant exactly
     once.
5. **No budget term in the pause rules.** `h2cPauseFor` and the TLS
   `pause` / `pause_control` closures are unchanged: with count-once the
   drain cannot exceed the budget, and a budget term would stop the only
   thing that empties it.
6. **Inline complete handlers are gated, not parked.** Before the actor
   runs an inline job, require budget room of at least
   `outbound_bytes_per_stream` (reserve it for the job, return the unused
   part on every exit), the same way `runPendingInline` already gates on a
   free ordinary control entry. A job without room stays queued and is
   retried on a later actor turn; the actor never waits. `WriteFailed`
   stays only as the backstop for a gate bug.
7. **No new deadline; a boot check instead.** At the defaults, wire bytes
   are bounded by the write pool (`inbound_wire_chunks_per_connection` x
   `WIRE_CHUNK_SIZE`, about 130 KiB), so the budget cannot be "all wire",
   and every pending byte already has the slow-consumer deadline. Make
   that premise a check in `Limits` validation:
   `inbound_wire_chunks_per_connection * WIRE_CHUNK_SIZE +
   outbound_bytes_per_stream <= outbound_bytes_per_connection`, with a
   test that a violating config is InvalidConfig. Say in the code why.
8. **Server cap and slab exhaustion stay HARD REFUSALS (owner's decision).**
   `outbound_bytes_per_server` exhausted, and the server-wide slab pool
   exhausted (`enqueueDataBytes` returning `SlabsExhausted`, at the
   defaults the same 256 MiB ceiling), refuse the write: the stream is
   reset, never parked. Distinguish them from "connection budget full"
   (today both are a bare `false` / `OutOfMemory`), give each a named
   counter on `/mem`, and say in the commit that server-wide exhaustion is
   overload shedding, not backpressure.
9. **Out of scope:** the terminal queue size (t-3562) and TLS bare flush
   barriers (t-3567) are separate changes after this one. Your tests must
   pass with the terminal queue at 32 and must assert
   `ctrl_terminal_full == 0` where they rely on it not overflowing.

## Tests (required)

- **Park and resume** (a new test file, real server, the raw client in
  `tests/wire_client.zig`), h2c and TLS:
  `outbound_bytes_per_connection = 2 * outbound_bytes_per_stream` (adjust
  so the new boot check passes), three task streams of 64 KiB, large client
  window: the third write parks (a test-observe counter `test_budget_waits
  > 0`), the client sees no RST_STREAM, all bodies arrive intact, all
  budget counters return to 0.
- **FIFO order and no barging:** several parked writers are granted in
  arrival order; a newcomer arriving while the queue is non-empty waits.
- **Exits:** a parked writer that is cancelled, reset by the peer, or ends
  with the connection releases its grant exactly once (counters return to
  0; no leak trapped at deinit).
- **Accounting transition:** enqueue P bytes (total P), the drain moves one
  DATA frame (total still P, not 2P+9), the ack releases it (total 0); on
  TLS with the chunk first in the stash, then in `pending_writes`. A packed
  batch of small DATA frames and controls releases exactly its payload.
- **Inline gate:** with the budget full, an inline complete job waits on
  the actor's queue and runs (with no reset) once room returns.
- **Server cap:** two connections, `outbound_bytes_per_server` equal to one
  connection's budget: the second connection's waiter gets the server
  refusal, not a hang.
- **Empty DATA:** an END_STREAM DATA frame with no payload still completes
  and wakes waiters.
- **Boot check:** a config that violates item 7 is InvalidConfig.

## Mutation tests (required; break the CODE, not the tests)

1. Restore the refusal in `enqueuePending` (no park).
2. Restore the `.wire` reserve in `sendAccountedWire` (count twice).
3. Remove the wake from `applyOutboundRelease`.
4. Wake-all instead of the FIFO handoff.
5. Let a newcomer barge past a non-empty queue.
6. Do not return a cancelled waiter's grant.
7. Release `src.len` instead of the payload on the ack.
8. Remove the inline gate.
9. Remove the boot check.
10. Park on a server-cap miss instead of refusing.

For each, run the full `./zb build test` and record which test fails.

## Proof the orchestrator will run (make it possible)

- `MODES="h2c tls" MS="100 200" KBS="64 128" RUNS=10 tools/t3538-rate.sh`
  must show every cell with 0 failed requests, 0 server-ended runs, 0
  crashes, 0 budget refusals. Add to `--close-reason` a per-connection
  budget high-water so a cell can be shown to have reached the limit (a
  cell that never filled the budget proves nothing).
- Throughput: the standard nachos shapes against master, alternated.

## Gates

- `./zb build test`, `./zb build ci` (the two `macos-libcxx-probe` steps
  fail on this Mac, t-3540; every other step must pass; `ci` now compiles
  every artifact), `./zb build tls-smoke`, and
  `./zb build -Doptimize=ReleaseFast`.
- Zig caches a passing test run; change a comment in a test file to force
  a rerun.
- You cannot run Linux; the orchestrator runs the suite on nachos.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-optA` (branch
  `carmack/option-a`, from master 3ba492c). Never touch any other
  worktree, including the one your pane started in.
- Base tripwire, before any work: `git -C
  /Users/rsaunderson/Source/mine/starh2-wt-optA log --format=%s -3` shows,
  newest first, "Add the brief for option A", "Add the panel brief for
  option A", and "Build every HTTP/2 frame from the connection's pools".
  If not, STOP and report `STATUS: blocked`.
- Copy `testdata/cert.pem` and `key.pem` from
  `/Users/rsaunderson/Source/mine/starh2-wt-stage3/testdata/` (they are
  gitignored).
- Kill only processes you started, by pid. Never `pkill -f`.
- If you start a long command in the background, do not end your turn
  without a STATUS line; wait for it, or say in the STATUS line what is
  still running.
- This is a core write-path change. If a decided item cannot work, STOP
  and report it with the code that shows why, before building around it.
- Commit per milestone (count once; the park and FIFO; the inline gate;
  the boot check and the refusals). Each message is the decision record
  (problem, changes, benefits, alternatives rejected, what it depends on,
  statistics; markdown; plain ESL English). End it with
  `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file
  under "To file".
- Report in `tools/option-a-report.md` (not committed). End with
  `STATUS: done oa1`.

## Round 2 (review of 1a5b0c6; two reviewers, another vendor)

Gates, the Linux suite, and the rate matrix on nachos passed at 1a5b0c6. The
reviewers found the following. Each is checked against the code. Fix all of
them, as new commits on top (do not rewrite 1a5b0c6 or earlier). The round 1
rules still apply.

1. **Inline jobs starve, and can barge (item 6 as built is defective).**
   `runPendingInline` reserves a whole `outbound_bytes_per_stream` outside the
   queue. While task writers keep the queue non-empty, every release goes to
   the head, the free room never reaches one cap, and a 6-byte complete
   response waits for as long as the large bodies keep coming (no
   slow-consumer deadline applies: it has no pending DATA). Separately, the
   gate takes no lock, so it can take room in the window after
   `applyOutboundRelease` lowers `outbound_held` and before
   `wakeBudgetWaiters` hands it to the head.
   **Fix (keeps every decided property: the actor never waits, FIFO, no
   barging):** an inline job that is gated takes a place in the SAME FIFO, as
   an entry in its own handler slot, need = one stream cap, marked inline. The
   admission decision (queue empty and room: reserve now; else join the tail)
   is made under `budget_mu`, like a task writer's. A grant to an inline entry
   reserves the cap and rings the actor (it does not wake a handler). The
   actor runs inline jobs in `inline_sids` order and stops the batch at the
   first one whose entry is not granted. A refused inline entry (server cap)
   runs with `inline_shed`, as today. A stream reset or connection end while
   an inline entry is queued or granted unlinks it and returns the grant
   exactly once.
2. **Removing a queued entry does not hand room to the next waiter.**
   `abandonBudget` on a `.queued` entry unlinks it and calls
   `applyOutboundRelease(0)`, which returns at once. If the removed entry was
   a head whose need did not fit and the next one fits, the next one sleeps
   until some unrelated release (behind closed stream windows, that can be
   the slow-consumer deadline). Fix: after an unlink, call
   `grantBudgetLocked` under `budget_mu`.
3. **Missing tests.** Each must fail under the mutation named:
   - The granted branch of `parkForBudget`: a grant, then a terminal cause
     before the handler resumes; and a grant, then `writer_failed`. Mutation:
     delete each `applyOutboundRelease(need, .pending)` in that branch.
   - A task handler cancelled while parked (not only peer reset and hang-up).
   - Item 2: a blocking head that leaves, with a smaller follower that fits.
     Mutation: remove the new `grantBudgetLocked` call.
   - Item 1: task writers keep the queue non-empty; an inline complete job
     still runs, after a bounded number of grants (assert its place in the
     grant order). Mutation: restore the room-only gate.
   - Required by round 1 and not present: the TLS accounting transition with
     the chunk first in the stash, then in `pending_writes`; and an assert of
     `ctrl_terminal_full == 0` in the tests that rely on it not overflowing.
4. **Mutations:** rerun mutations 1 to 10 against the new code (8 now means
   "remove the inline entry, let the job reserve outside the queue"), plus
   the four new ones above. Same table as round 1.

Not for this round: the request-memory finding (a parked writer holds its
request arena) is with the owner. Leave the rate script's `REQUEST_ARGS`
default as it is.

Report in `tools/option-a-report.md` (overwrite; not committed). End with
`STATUS: done oa2`.

## Round 3 (review of 8cdbead; the same two reviewers)

Gates (Mac and Linux), the budget tests on Linux (5 of 5), and the rate
matrix on nachos (80 of 80 runs, 0 failing) passed at 8cdbead. Both reviewers
found defects inside the round 2 fix. Each is checked against the code. Fix
them as new commits on top.

1. **A terminal inline job keeps its budget entry (major).** `runPendingInline`
   skips `inlineBudget` when the job's stream is terminal, and nothing else
   disposes of an entry that is still queued or granted.
   `handleWriterFailed` (`connection.zig` near 2998-3063) sets every slot's
   terminal cause and releases pending DATA, which can GRANT a queued inline
   entry, but it never calls `wakeHandlerWaiters`. The actor then skips the
   gate, the job ends, and the slot is released with the entry granted (a
   leaked cap) or still linked (a freed slot in the queue).
   **Fix:** in the terminal branch of `runPendingInline`, abandon the entry in
   any state (`abandonBudget`) before the job runs. Then make the invariant
   loud: when a handler slot is released, its budget entry must be idle and
   unlinked. Check it there and panic in safe builds with the stream id and
   state (the same style as the over-release panic). Do not make slot release
   silently return the budget; a leak must show up, not be absorbed.
2. **`inlineBudget` reads `slot.budget.state` without `budget_mu` (a data
   race), and the empty-queue path is slow.** The grant path writes that field
   under `budget_mu` from another thread. Today only the actor moves an inline
   entry into or out of `idle`, so the value read is right, but a plain read
   racing a write is undefined behaviour in Zig. It is also the hot path of
   every complete response: it takes `budget_mu` twice (`queueForBudget`,
   then `takeBudgetOutcome`) and `grantBudgetLocked` rings the actor for an
   entry that was granted inside its own admission call.
   **Fix:** one critical section under `budget_mu` per inline admission: read
   the state, join the tail if idle, run the hand-over, take the outcome
   (locked helpers, no recursive lock). Do not ring the doorbell for the
   entry that is being admitted in that same call; a grant from a later
   release still rings.
3. **Tests.** Each must fail under the mutation named:
   - A GRANTED inline entry whose stream is reset before the actor runs it:
     the cap goes back at once and not a second time when the job finishes.
     Mutation: `wakeHandlerWaiters` abandons only `.queued` inline entries.
   - Item 1's path: an inline entry queued, then a writer failure
     (`handleWriterFailed`) grants it and makes the stream terminal, then the
     actor runs the batch. The budget returns to zero and the slot is released
     cleanly. Mutation: remove the new abandon in the terminal branch (the new
     slot-release check must be what fails).
   - Item 2: with an empty queue, admitting an inline job does not ring the
     doorbell. Use an observe counter on `ring()` from the grant path.
     Mutation: ring for the entry being admitted.
4. **Mutations:** rerun 3, 6, 8, n1a, n1b, n3 and the three above. Same table.
5. **Do not change anything else.**

Report in `tools/option-a-report.md` (overwrite). End with `STATUS: done oa3`.

## Round 4 (review of 5ea000d; the same two reviewers)

Both reviewers found the round 3 production code correct. One test does not
prove its path, and both found it:

- The test "a writer failure grants a queued complete job..." reserves the
  full budget directly and queues no scheduler DATA, so `handleWriterFailed`
  releases nothing; the test's own `applyOutboundRelease(lim, .pending)` makes
  the grant. A broken release inside `handleWriterFailed` would pass it.
  **Fix:** fill the budget with scheduler-owned pending DATA (so the bytes are
  released by `handleWriterFailed` itself), call `handleWriterFailed`, and
  assert the inline entry is `.granted` right after it, with no manual
  release. Then run the batch and check, as now, that the books are zero and
  the slot is released cleanly.
- **Mutation:** delete the `applyOutboundRelease(pw.len, .pending)` inside
  `handleWriterFailed`. This test must fail. Also rerun r3b (remove the
  terminal-branch abandon); it must still fail through the slot-release check.

Change only this test. Report in `tools/option-a-report.md` (overwrite). End
with `STATUS: done oa4`.
