# Brief: fix t-3567, TLS bare flush barriers wait for stash room

You are the implementer. A reviewer from another vendor reads your diff, and
the orchestrator reruns every test and measures speed itself. Do not tell
the orchestrator that tests pass; it recomputes them. Do not prompt,
message or read any other agent's pane.

## Why (settled; do not reopen)

Read `tools/t3567-brief.md` and the commit message of 740f3c6 (the
reproduction, `tests/t3567_tls_barriers.zig`, `./zb build test-t3567`). The
orchestrator reran it 3 of 3 on macOS: on TLS, n=60 bare flush barriers
while the writer is held end the connection (33-39 `tls_write_overflow`,
`ctrl_terminal_full`); n=20 crashes a Debug build ("TLS stash wrote chunk 28
after chunk 17"); h2c passes (the barriers wait in `waitForAckRoom`).

Two defects:

1. `waitForAckRoom` is armed only on h2c (`ack_room_armed = true` on the h2c
   start path), and `sendFlushTicket` pushes straight to the stash, outside
   the scheduler's `pause` / `pause_control`. So on TLS a bare barrier has no
   check before a full stash.
2. `Pump.push` (`src/edge/tls.zig`) stamps `stash_seq` (`stampPush`) before
   `write_ch.trySend`, and a failed `trySend` does not undo it. After k failed
   pushes, the safe-build order check in `popPending` panics at the next
   write.

## Decided design (do not reopen; if an item cannot work, STOP and show why)

1. **One barrier wait for both transports.** Arm `waitForAckRoom` on TLS
   too. Keep one function body; make the room predicate transport-aware:
   h2c unchanged (`barrierRoom`: ack room and queue room); TLS:
   `!pump.stashFull()`, the same test a drain asks before a pop, so a barrier
   pushed then leaves the drain's tail push its slot (`assertTailRoom`). The
   lock protocol is unchanged: check under `session_mu`, W1, re-check,
   unlock, select on the slot's event and `dead`, relock uncancelably.
   Rename what is now misnamed (for example the `ack_room_*` names) only if
   the new name is plainly better; keep the diff small.
2. **The waker on TLS is the driver.** The TLS actor posts no write acks, so
   `wakeAckRoomWaiters` never runs there. In `driveTlsTurn`, after chunks are
   taken off the stash, wake the waiters when there are any and the stash
   has room. Write the no-lost-wake argument at the function, like the h2c
   one (the take and the re-check lock the same channel mutex; say which
   operations order W1 against the driver's read). The other wake sources
   stay as on h2c: a failed write, `handleWriterFailed`, teardown (`dead`).
3. **Undo the stamp on a failed push** in `Pump.push`, safe builds only.
   Every push holds `session_mu` (one producer), so the undo is exact; say so
   in a comment.
4. **Fix the comment in `pushWriteChunk` (TLS branch)** that says the pause
   hooks stop every push before the stash fills: after this change, drains
   are stopped by the pause hooks and bare barriers by the barrier wait.
5. **Inline handlers:** `waitForAckRoom` already returns `WriteFailed` for
   `hctx.runs_inline`; keep it. Check, with a quote, that no complete
   (inline) handler can reach a bare barrier on TLS.
6. **Out of scope:** the terminal queue size and the fact that terminal RSTs
   cannot drain while the stash is full (t-3562). Do not touch them.

## Tests (required)

- `tests/t3567_tls_barriers.zig` joins `./zb build test` (add it to the test
  step), and its assertions do not change: every case must pass on both
  transports with no `tls_write_overflow`, no failed flush, no RST_STREAM, no
  server close, every response ended. On TLS also assert the barriers
  WAITED (the wait counter moved while held), as the h2c case already does.
  Make sure each case has a deadline, so a lost wake fails the test instead
  of hanging the suite.
- A unit test in `tls.zig`: fill the stash, a push fails, a chunk is taken,
  the next push succeeds, and the pops come out in order with no panic.

## Mutation tests (required; break the CODE, not the tests)

1. Do not arm the wait on TLS.
2. Remove the driver's wake.
3. Remove the stamp undo.
4. The TLS predicate checks for one free slot instead of `!stashFull()`.

For each, run the full `./zb build test` and record which test fails (or
that it hung and was killed, which counts as detected).

## Gates

`./zb build test`, `./zb build ci` (the two `macos-libcxx-probe` steps fail
on this Mac, t-3540; every other step must pass), `./zb build tls-smoke`,
`./zb build -Doptimize=ReleaseFast`, and
`./zb build test-t3567 -Doptimize=ReleaseFast -Dobserve=true`. Zig caches a
passing test run; change a comment in a test file to force a rerun. You
cannot run Linux.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-3567` (branch
  `carmack/t-3567`). Never touch any other worktree, including the one your
  pane started in.
- Base tripwire, before any work: `git -C
  /Users/rsaunderson/Source/mine/starh2-wt-3567 log --format=%s -3` shows,
  newest first, "Add the fix brief for t-3567", "Add a t-3567 reproduction",
  "Add the triage brief for t-3567". If not, STOP and report
  `STATUS: blocked`.
- Kill only processes you started, by pid. Never `pkill -f`.
- Commit per milestone. Each message is the decision record (problem,
  changes, benefits, alternatives rejected, what it depends on, statistics;
  markdown; plain ESL English). End it with
  `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file.
- Report in `tools/t3567-fix-report.md` (not committed). End with
  `STATUS: done t67b`.

## Round 2 (review of 214e347; gpt-6-astra and Grok 4.7)

Gates (Mac and Linux), the t-3567 binary 10 of 10 on Linux, and a nachos
throughput A/B (no shape separable from noise) passed at 214e347. One
reviewer found the following; each is checked against the code. Fix them as
new commits on top; the round 1 rules still apply.

1. **The driver's wake can be postponed for ever (major).** In
   `driveTlsTurn`, `wakeBarrierWaitersTls` runs once, after
   `while (pump.pending_n == 0) { ... tryTakeWrite ... writeChunks ... }`.
   A bare barrier makes no pending ciphertext, so its chunk leaves
   `pending_n == 0` and the loop goes on. While handlers keep pushing empty
   barriers (each one acked inside the loop, then flushing again), the loop
   need not end, and a handler that parked earlier is not woken although the
   stash has room. **Fix:** wake inside the loop, after each take that freed
   room (or after each `writeChunks` call), not only after the loop. Keep the
   cost to one atomic load when nobody waits. Update the no-lost-wake comment
   so it states the progress guarantee too: every take that frees room is
   followed by a wake check before the next take.
   **Test:** a parked barrier waiter is woken while other handlers keep
   flushing repeatedly (they do not stop until the parked one has finished).
   Mutation: move the wake back after the loop; the test must fail by its
   deadline, not hang the suite.
2. **The room assertion is weaker than the predicate (minor).**
   `test_tls_barrier_min_room >= tail_pushes_per_drain` accepts a
   `hasRoomFor(2)` predicate, and it passes on the initial `maxInt` if
   nothing is recorded. **Fix:** require a recorded value (not the sentinel)
   and `min_room >= pushes_per_pop + tail_pushes_per_drain - 1` (a barrier
   admitted at the threshold leaves one fewer). Mutation: `hasRoomFor(2)`.
3. **A reset trigger response counts as ended (minor).** In
   `tests/t3567_tls_barriers.zig`, the `/hello` branch sets `trigger_ended` on
   RST_STREAM (`s.ftype == 3`) and skips the reset count. **Fix:** count every
   RST_STREAM, and accept the trigger's end only from END_STREAM on HEADERS or
   DATA.

Rerun mutations 1 to 4 of round 1 and the two new ones. Report in
`tools/t3567-fix-report.md` (overwrite). End with `STATUS: done t67c`.

## Round 3 (review of 293e906; gpt-6-astra and Grok 4.7)

Mac gates, the t-3567 binary 10 of 10 on Linux, and a nachos A/B (no loss;
tls mixed +2.1%) passed at 293e906. The Linux full suite failed once on
`compression: I4 SSE flush...`, an h2c timing flake already filed (t-3547):
0 of 80 at master and 0 of 80 at 293e906 when rerun. Not this change.
2424557 (ring after a bare barrier) is accepted: the orchestrator removed the
ring and the keeper case got 0 of 36 one-shots through.

1. **A task handler's empty-body `send` on TLS rings nobody (major; both
   reviewers).** `sendCb`, the branch with no body and not inline:
   `processIntents` drains on the handler's task and pushes HEADERS into the
   TLS stash, then `waitTicket` parks the handler. The actor selects on the
   CQ, the completion channel, the doorbell, shutdown and the timer, not on
   the stash, and a GET arms no timer, so both can stay parked. This is the
   same defect class as 2424557: ANY push to the TLS stash from a handler's
   task, followed by a park that is not the task's exit, leaves the actor
   asleep. Other such callers named by the reviewers: `abortCb`, the
   compression-failure path, the handler-error RST (each calls
   `processIntents` with no ring; today they are rescued only when the task
   then exits).
   **Fix (one place, not a second scattered `ring()`):** make every push to
   the TLS stash from a task that is not the actor ring the actor, in ONE
   place that every such path passes through, and remove the `ring()` that
   2424557 added in `pushBareBarrier` if the new place covers it. Do not add a
   doorbell ring per chunk on the actor's own drains (that is the hot path);
   if the place you pick cannot tell the actor from a handler, ring once per
   handler-side drain, not per chunk. List every caller you checked.
   **Test:** a TLS task handler that sends an empty body (for example 204)
   on an otherwise idle connection gets its response, by a deadline. It must
   FAIL on master's behaviour (show it fails with the fix reverted), then pass.
   Mutation: remove the new ring.
2. **The progress comment at `waitForAckRoom` overstates the guarantee.** The
   takes inside `writeChunks`' batch gather share the wake after
   `writeChunks`; they are not each followed by a wake. Grok showed a parked
   barrier still progresses (barriers never gather; one outer take moves the
   stash across the threshold and the wake before the write sees it). Make
   the comment say exactly that. No code change for this item.
3. **The keeper case's power.** One reviewer argued the keeper / one-shot
   case could pass with the wake moved after the loop under some scheduling.
   You measured it detected once. Rerun mutation n1 (wake only after the
   loop) 3 times and report each result. If any run passes, make the case
   deterministic (a designated one-shot parked while keepers are already
   flushing, checked by a counter, not by timing).

Rerun mutations 1 to 4, n1 (3 times) and n2, plus the new one. Report in
`tools/t3567-fix-report.md` (overwrite). End with `STATUS: done t67d`.
