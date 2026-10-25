# Brief: a connection error must deliver its GOAWAY (t-3520)

You are the implementer. A reviewer from another vendor will read your
diff, and the orchestrator reruns every test and counts every number
itself. Do not tell the orchestrator that tests pass; it recomputes them.
Findings reach you only through the orchestrator. Do not prompt, message or
read any other agent's pane.

## Premises: treat these as settled

- The stack is not in real use and has no external consumers. Do not weigh
  backward compatibility. A new `Limits` field is fine.
- Read `AGENTS.md` first; its premises, its "Ownership and concurrency"
  section and its TLS section apply.
- Never guess a dependency's contract: zio is vendored under `zig-pkg/`
  (find the zio directory there) and tls.zig likewise. Read the function you
  call.

## The defect

RFC 9113 section 5.4.1: on a connection error an endpoint SHOULD first send
GOAWAY (with the last stream id it processed and the error code), then
close. starh2 queues the GOAWAY and then closes without writing it, at
least on TLS. The client sees a bare close and cannot tell which streams
were processed.

What is known, and how:

- FACT (test): `tests/tls_preface.zig`, connection-error shape (a DATA
  frame on stream 0 in the first flight). On master before 8a9c99f the client
  received no frames at all. After 8a9c99f the server SETTINGS goes first,
  but the GOAWAY is still not delivered; the test asserts only that
  whatever arrives starts with SETTINGS, and says it tightens here.
- FACT (code): the main loop in `Connection.runBody`
  (`src/edge/connection.zig`) does `if (self.session.terminal != .none) {
  self.freezeAndSweepLocked(); break; }` right after `drainEmit`. On TLS the
  write of the stash happens in `driveTlsTurn`, later in the same pass, so
  it never runs for that GOAWAY. Teardown then calls `tls_pump.failDrain()`,
  which drops the stash.
- FACT (code): the first-flight path in the startup block returns
  `error.ConnectionClosed` when the first flight is terminal, before the
  main loop runs at all.
- UNKNOWN: whether h2c delivers the GOAWAY (its WritePump is a separate
  task and `closeWriterQueues` may or may not let it finish), and whether
  graceful shutdown (`maybeBeginGraceful` / `finishGraceful`, phase 1 and 2)
  delivers its final frames on TLS. Measure these; do not assume.

## Step 1: measure on the base, before any fix (required)

Add tests that send a connection error and assert "SETTINGS, ..., then
GOAWAY with the right error code and last stream id, then close". Cover:

1. TLS, first flight (tighten the existing test in `tests/tls_preface.zig`;
   keep its 24-connection loop and its leftover-path counter).
2. TLS, steady state: a normal request completes first, then the client
   sends a connection error.
3. h2c, steady state, the same shape.
4. Graceful shutdown (`requestShutdown` with a connection open), TLS and
   h2c: the client must receive the GOAWAY. If the suite already proves
   this, cite the test and skip it.

Run them on the base commit with only the tests added and record which
fail. Report that table before you change production code. A test that
passes on the base for a case you believed broken is a finding: report it,
do not delete it.

## Step 2: the fix (design decided; do not reopen)

One closing phase, inside the existing main loop. No second drive loop.

- When Session becomes terminal and has queued a GOAWAY (connection error,
  or the end of graceful shutdown), the actor does not `break` at once. It
  enters a closing state in the same loop. In that state it:
  - stops dispatching new work and stops ingesting application frames
    (inbound bytes may be read and dropped, or reading may stop; pick one
    and say why),
  - keeps driving writes the normal way (TLS: the same `driveTlsTurn` path;
    h2c: the WritePump), and
  - leaves the loop when the GOAWAY has been written (use the existing
    write-completion accounting or a flush ticket, not a new mechanism), or
    when a deadline passes, or when the writer fails, whichever is first.
- The deadline is a new `Limits` field (name it, document it inline, count
  nothing new in `resourceUpperBound` unless it holds memory). Pick a small
  default and justify it. A peer that never reads must not hold the
  connection longer than this deadline.
- The first-flight path must use the same closing phase. Make the startup
  block fall into the loop in the closing state instead of returning early.
  There must be exactly one place that decides "written or timed out".
- Cancellation must be preserved: a cancel during the closing phase ends it
  at once and `run` still sees `error.Canceled` (it calls
  `zio.recancel()`). Test this: cancel a connection that is in the closing
  phase with a peer that never reads, and assert it ends well before the
  deadline.
- After the closing phase, teardown runs exactly as today
  (`freezeAndSweepLocked`, `teardownExhaustive`, `failDrain`). Do not add a
  release path; anything the closing phase did not write is released by the
  existing owners, exactly once.

## Tests (required, beyond step 1)

- A peer that never reads (TLS and h2c): the connection closes within the
  deadline plus a small margin. Measure and assert the duration.
- The cancel test above.
- Every step-1 test passes after the fix.

## Mutation tests (required; break the CODE, not the tests)

1. Restore the immediate `break` on terminal.
2. Make the closing phase ignore the deadline.
3. Make the closing phase swallow `error.Canceled`.
4. Make the first-flight path return early again.

For each, run the full `./zb build test` and record which test fails. Do
not regenerate or loosen any existing test expectation; a changed
expectation in an existing test is a finding the reviewer will ask about.

## Gates

- `./zb build test` and `./zb build ci`. On this Mac `ci` always fails one
  step, `macos-libcxx-probe` (two runs, environment). Every other step must
  pass. Report the full step list.
- `./zb build tls-smoke` passes.
- Zig caches a passing test run. To repeat a run, change something in the
  test file first (a comment), or the second run is answered from cache.

## Rules

- Work only in your worktree. Never touch `/Users/rsaunderson/Source/mine/starh2`.
- Base tripwire, before any work: `git log -1 --format=%s` must be the
  commit that added this brief, and its parent's subject must start with
  "Send the server SETTINGS before ingesting the TLS first flight". If not,
  STOP and report `STATUS: blocked`; do not rebase.
- `testdata/cert.pem` and `key.pem` are gitignored and already copied into
  your worktree. If they are missing, generate them with Homebrew OpenSSL 3
  (system LibreSSL makes a v1 certificate that tls.zig rejects).
- Commit per milestone (step-1 tests, then the fix, then the extra tests).
  Each commit message must describe its own tree.
- The commit message is the decision record (see `~/.claude/CLAUDE.md`
  "Commits"): problem, changes, benefits, alternatives rejected, what it
  depends on, statistics, in markdown, plain ESL English. End it with
  `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; put anything you would
  file in the report under "To file".
- Write a short report to `tools/goaway-delivery-report.md` in your
  worktree (not committed): commits, the step-1 table, each mutation and the
  test that caught it, anything you could not verify.

## Round 2 (review findings on 1d8e5e1; each one confirmed in the code by the orchestrator)

Start by reading round 1's fixes again: in earlier tasks, most new defects
were inside the previous round's fix.

### R2-1: the h2c room check runs once, but a drain can push more than it reserves (fix)

`closeEmitHasRoom` checks room for `Pump.pushes_per_pop + Pump.tail_pushes_per_drain`
ONCE, before `drainEmit`. `FairScheduler.drain` runs until the scheduler is
empty (terminal loop, ordinary loop, DATA loop), and on h2c nothing pauses it.
TLS is bounded only because its pause hook re-checks room before EVERY pop
(`Pump` "isPaused": `!self.hasRoomFor(pushes_per_pop + tail_pushes_per_drain)`).
So with 3 free slots and more than one pop's worth of queued entries, a later
`putOne` parks the actor under `session_mu` and the closing deadline cannot
fire, which is the defect `closeEmitHasRoom` was added for.

Fix: give h2c the same per-pop check while the connection is ending (closing
phase, or Session terminal with a GOAWAY), through the scheduler's existing
pause hooks, and remove the one-time check. Outside the ending state the h2c
hot path must not change (t-3521 is the general h2c case; do not fix it here).
If the pause hooks cannot express this, say why before you build anything
else.

Test first: a test that FAILS on 1d8e5e1 because more than one pop's worth of
entries are queued while the queue has exactly the reserved room. Show the
failure output in the report. Then the fix. Then a mutation: restore the
one-time check, and the new test must fail.

### R2-2: two places swallow error.Canceled, and the closing phase reaches both (fix)

zio delivers each cancel once: returning `error.Canceled` decrements
`pending_errors` (zio `task.zig`, see `recancel`). Code that catches it and
does not return it loses the cancel, and `run` then never calls
`zio.recancel()`. Both sites are older than this task, but the closing phase
runs through them:

- `self.config.io.sleep(.zero, .awake) catch {};` in the TLS inbound batch
  (it runs while closing, because dropped chunks still count in `held_n`).
- `pushWriteChunk`: `self.write_ch.putOne(self.config.io, chunk) catch return error.WriteFailed;`
  turns a cancel into `WriteFailed`.

Fix both: `error.Canceled` must leave as `error.Canceled`; other errors keep
their current handling. Read zio's `sleep` and the queue's `putOne` to list
their error sets first. Check whether any other `catch` on the actor's path
can swallow a cancel and list what you find in the report (fix only these two
unless another one is reachable from the closing phase; then say so).

Test: if you can build a test that cancels the actor while it is in one of
these two calls, do. If you cannot make it deterministic, say why in the
report, and instead give a mutation argument: which existing test fails when
you swallow the cancel again at each site.

### Out of scope this round (filed as t-3522)

A peer that half-closes (TCP FIN) right after the bad frame: the TLS pump's
EOF branch calls `failDrain`, and the h2c read pump shuts down the send side,
so the GOAWAY is lost on both transports. That changes the EOF rules of both
pumps for every connection, so it is a separate change. Do not touch it.

### Round 2 rules

Same rules as above. New commits on top of 1d8e5e1 (no rebase, no squash).
Update `tools/goaway-delivery-report.md` with a "Round 2" section: the
failing-test output, the fix, the mutations, the full `./zb build test`
summary line. End with `STATUS: done <token>` as before.

## Round 3 (review findings on afd37f4; both confirmed in the code by the orchestrator)

Again, start by re-reading round 2's fixes; these two findings are both inside them.

### R3-1: an h2c queue smaller than one pop's reservation is accepted (fix by shape)

`Limits.resourceUpperBound` rejects only `inbound_wire_chunks_per_connection == 0`
(`src/core/limits.zig`). The h2c write queue is that size
(`write_ch_buf`), and `h2cEndingHasRoom` reserves
`@min(Pump.pushes_per_pop + Pump.tail_pushes_per_drain, capacity)`. With 1 or 2
slots, one pop can push more than is free, and the actor parks in `putOne`
under `session_mu`: the defect R2-1 fixed, back for small queues.

Fix: reject `inbound_wire_chunks_per_connection < Pump.pushes_per_pop + Pump.tail_pushes_per_drain`
in validation, with a comment that says why, and drop the `@min` (the bound then
always holds; assert it). Test: a `Limits` validation test for the boundary
value and the one below it. If an existing test or default uses a smaller
value, report it; do not silently raise it.

### R3-2: `runPendingInline` swallows the new error.Canceled (fix)

`runPendingInline` returns `void` and does
`self.processIntentsWithReceipt(...) catch self.failClosedOnIntentError();`.
Since afd37f4, `processIntentsWithReceipt` can return `error.Canceled` (through
`sendAccountedWire` -> `pushWriteChunk` when an h2c inline drain parks on a full
queue). That catch eats it, so `run` never reaches `zio.recancel()`.

Fix: `error.Canceled` must leave `runPendingInline` as `error.Canceled` to its
actor callers; every other error keeps today's `failClosedOnIntentError`
handling. Change the callers to propagate. List every caller in the report and
say for each whether it can run under a cancel shield (teardown) and what you
did there.

Test: the reviewer says the main-loop `drainEmit` catch and the
`pushWriteChunk` going-away `shutdown` site ARE reachable with a full-queue
cancel (the writer held with `wire_pump.test_hold_writes`, the queue filled,
then the server canceled). Try to build one test that cancels the server
while the actor is parked in `putOne` from an inline drain, and assert the
connection task ends with `error.Canceled` reaching `run` (if the task result
is not observable, find a counter or hook that is, and say what you used).
If you cannot make it deterministic, say why and give the mutation argument
as before.

### Round 3 rules

Same rules. New commits on top of afd37f4. Add a "Round 3" section to the
report with the failing-test output, the caller list, the mutations, and the
full `./zb build test` summary line.

## Round 4 (one review finding on 24898a4, test only; confirmed by the orchestrator)

### R4-1: the inline-drain cancel test does not prove it went through `runPendingInline`

`test_run_canceled` counts any `error.Canceled` that reaches `run`. If timing
puts the actor in another park (the main-loop drain, a select), the inline
test still sees 1 and passes with the R3-2 swallow restored. The main-drain
test passes with that swallow by design.

Fix: a `test_observe` counter incremented inside `runPendingInline`'s
`error.Canceled` branch (the place that sets `canceled = true`). The inline
test asserts it went up by exactly 1, AND keeps the `test_run_canceled`
assertion. The main-drain test asserts it did NOT go up (so the two tests
prove different paths). No production behaviour change.

Mutation: in `runPendingInline`, turn the Canceled branch back into
`failClosedOnIntentError()` (keep `canceled` written so it compiles, for
example `canceled = false;` before the call). Run the full suite 3 times; the
inline test must fail all 3.

Same rules. One commit on top of 24898a4. Add a short "Round 4" section to the
report.
