# Brief: t-3594, a handler's release of `session_mu` wakes the actor

You are the implementer. Reviewers from another vendor read your diff, and
the orchestrator reruns every test and measures speed itself. Do not tell
the orchestrator that tests pass; it recomputes them. Do not prompt, message
or read any other agent's pane.

## Why (settled; do not reopen)

Read `tools/t3594-panel.md` (the defect class and the four candidates) and
the commit message of 58b88c9 (t-3567). A three-model panel (gpt-6-astra,
Grok 4.7, Fable) picked D1 unanimously. Where it split, the decision is
stated below.

## Decided design (if an item cannot work, STOP and show why)

1. **The ring goes in `unlockSession`, after the unlock, when the unlocking
   task is not this connection's actor:**
   `self.session_held = false; self.session_mu.unlock(io); if (actor_marker.get() != self) self.ring();`
   Both transports, one code path. Ring AFTER the unlock (the producer's last
   act; a ring before it wakes the actor into a park on the mutex the
   handler still holds). Compare against `self`, not against null.
2. **The mid-operation unlocks ring too, by design.** `waitForAckRoom`,
   `waitForStreamSpace` and `parkForBudget` all release through
   `unlockSession`. The ring there is REQUIRED: `enqueuePending` copies piece
   k, then parks in `waitForStreamSpace` for piece k+1; only that unlock can
   tell the actor about piece k. Fix the comments that say
   `waitForStreamSpace` is the ONLY mid-operation release (the file header
   near lines 51-55 and the comment near 5431): there are three.
3. **Remove the handler-side rings this covers:** both in `enqueuePending`
   (near 5546 and 5554), the TLS branch of `pushWriteChunk` (near 6570, with
   its marker test), and `startCb` (near 8174). Keep the comments' content
   where it explains a flag; the wake now comes from the release.
4. **Keep, unchanged and NOT marker-suppressed:** the ring in `tryDispatch`
   near 7190 (`inline_n += 1`) and the inline-entry ring in
   `grantBudgetLocked` near 2721. These are the actor ringing ITSELF for work
   it produced after this turn's check (`dispatch` also runs on the actor
   inside `runPendingInline`; wire releases grant budget with no
   `session_mu`). D1 does not replace them. Say so in a comment at each, so
   nobody later removes them as "covered by the unlock".
5. **Item 3 of the panel brief is fixed by this:** `handleWriterFailed`,
   reached from a handler under the lock, is followed by that handler's
   unlock, which now rings. Do not add a ring inside `handleWriterFailed`.

## Tests (required)

- **A build gate (ci, rung 3).** Mirror `tools/std-io-gate/` (its gate
  script and its wiring in `build.zig`): `session_mu.` may be used
  (`lock`, `lockUncancelable`, `unlock`, `tryLock`, or any alias taken of the
  field) only inside `lockSession`, `lockSessionUncancelable` and
  `unlockSession`, and `doorbell.trySend` only inside `ring`. Show the gate
  fails on a planted direct `self.session_mu.unlock(...)` elsewhere, then
  remove the plant.
- **Unit tests on the boundary, not on any named path.** (a) A task that is
  not the actor does `lockSession`, stores a value no ring site writes,
  `unlockSession`, and never calls `ring`: a parked actor (a select on the
  doorbell plus a timeout that FAILS the test if it wins) wakes. Run it with
  both acquisition wrappers and with an error-return (deferred) unlock.
  (b) With `actor_marker` bound to this connection, an unlock leaves the
  doorbell empty; bound to ANOTHER connection, it rings. (c) Coalescing:
  repeated releases with the doorbell full return; after the token is
  consumed, the next release makes a fresh one.
- **A park-time assertion (observe and debug builds).** Add a counter of
  non-actor releases that is incremented AFTER the ring in `unlockSession`
  (ring first, then count, or the check below can fire falsely). The actor
  records the count it has seen at the top of each turn. Just before each
  `zio.select` in `waitForActivity` (TLS and h2c), assert that either the
  count equals the recorded one or the doorbell is not empty; and that
  `inline_n == 0` or the doorbell is not empty. Mutation: remove the ring
  from `unlockSession`; the idle cases below must then panic at the park,
  not time out.
- **The failure regression (the third member of the class).** On an idle
  TLS connection, a task handler whose response is refused while it is
  materialized (`ctrl_fragment_full`: size the control pool so the
  CONTINUATION of a large header block is refused) catches the error and
  STAYS alive (waits on a gate the test opens later). The oracle is that the
  ACTOR reaches its teardown, by a deadline, while the handler is still
  alive. Not `test_observed_writer_fail_handled`: `handleWriterFailed` sets
  that itself, on the handler's task. Run it at 58b88c9 first (revert your
  ring) and show it FAILS by its deadline, then passes.
- **An idle table on both transports:** each `Response` operation on an
  otherwise idle connection with no timer armed, each with a deadline: send
  with an empty body, send with a body, start + write + end, a bare flush,
  abort. Extend the idle-204 case in `tests/t3567_tls_barriers.zig` or put
  it in a new file; it runs in `./zb build test`.

## Mutations (break the CODE; full `./zb build test` each; record what fails)

1. Remove the ring from `unlockSession`.
2. Ring before the unlock instead of after (it should still pass; say so if
   it does, that is a liveness-neutral move, and say what it costs).
3. Drop the marker check (every unlock rings, the actor too).
4. Remove the `tryDispatch` self-ring (near 7190).
5. Remove the inline-grant self-ring (near 2721).
6. Plant a direct `session_mu.unlock` outside the wrappers (the gate).

## Report only, do not fix

- `finishHandlerJob` posts the completion (`completion_ch.trySend`) BEFORE it
  decrements `live_handlers`; the graceful-finish check reads
  `live_handlers == 0`. Check whether the actor can take the completion and
  park between the two, so a graceful finish waits for the grace timer.
  Check whether the comment there depends on the current order (teardown
  also reads `live_handlers`). Say what you found, with quotes; do not
  change it.

## Gates

`./zb build test`, `./zb build ci` (the two `macos-libcxx-probe` steps fail
on this Mac, t-3540; every other step must pass), `./zb build tls-smoke`,
`./zb build -Doptimize=ReleaseFast`, `./zb build test-t3567
-Doptimize=ReleaseFast -Dobserve=true`. Zig caches a passing test run;
change a comment in a test file to force a rerun. You cannot run Linux.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-3594` (branch
  `carmack/t-3594`, from master 58b88c9). Never touch any other worktree,
  including the one your pane started in.
- Base tripwire, before any work: `git -C
  /Users/rsaunderson/Source/mine/starh2-wt-3594 log --format=%s -3` shows,
  newest first, "Add the build brief for t-3594", "Add the panel brief for
  t-3594", and "Make TLS bare flush barriers wait for stash room...". If
  not, STOP and report `STATUS: blocked`.
- `zig-pkg/` holds a copy of the pinned zio source (gitignored).
- Kill only processes you started, by pid. Never `pkill -f`.
- Commit per milestone (the release ring and the removed site rings; the
  gate; the tests and the park assertion). Each message is the decision
  record (problem, changes, benefits, alternatives rejected, what it depends
  on, statistics; markdown; plain ESL English). End it with
  `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file.
- Report in `tools/t3594-report.md` (not committed). End with
  `STATUS: done t94a`.

## Round 2 (review of 74aa83b; gpt-6-astra, Grok 4.7, and the orchestrator)

Gates at 74aa83b, run by the orchestrator: Mac (clean cache) `test`, `ci` (only
the two libcxx probes fail), `tls-smoke`, ReleaseFast, `test-t3567`
ReleaseFast+observe and `session-mu-gate` pass. Linux `test` passes; the
t3567 binary 10 of 10 and the unit binary 10 of 10. A nachos A/B shows no
cost on `h2c task4` or `tls mixed`. The release ring itself (e34fa1e) had no
finding from either reviewer. Fix the following as new commits on top; the
round 1 rules still apply.

1. **The park audit panics on a legal schedule (high; found by the
   orchestrator and by astra independently).** `unlockSession` rings, then
   counts. `trySend` can hand the token straight to the parked actor
   (zio `sync/channel.zig` 299-305) before the handler's `fetchAdd`. Then:
   the actor takes `.bell`, snapshots the OLD count at the top of its turn,
   the handler's increment lands, and `assertNoLostWake` sees "count moved,
   doorbell empty" and panics. The same late increment can make `auditWake`
   fire on a later unrelated wake. Count-before-ring has the opposite race,
   so one counter cannot be made right by moving it. **Fix (decided):** two
   counters, both `seq_cst`. `releases_started` is incremented after the
   unlock and BEFORE the ring; `releases_rung` AFTER the ring. The actor
   snapshots `releases_started` (not the other one) at the top of the turn
   and again at the park. Panic only if `releases_rung > snapshot` and the
   doorbell is empty. Why that is sound: `rung > snapshot` means at least
   one release that STARTED after the snapshot has finished its ring; that
   ring came after the actor's last take of a token (the actor takes one
   only in a `.bell` arm, before the turn top), so a token must be there.
   A release that started before the snapshot did its unlock before it, so
   the turn that follows reads its state under the lock. Write this argument
   in the doc comment. **Test:** a deterministic interleaving of the race
   (unit test): ring, the stand-in consumes the token and snapshots, THEN the
   count lands, then the audit runs; it must not panic. And the converse: a
   release whose ring is skipped (call the counting without the ring through
   a test-only path, not by editing `unlockSession`) after the snapshot must
   panic, or return an error you can assert; choose a shape a test can
   observe without crashing the test binary.
2. **Drop the `inline_n` half of the park audit (decided).** astra found a
   false panic on TLS: `runPendingInline` runs BEFORE `driveTlsTurn`, and
   `drivePending` can free a control entry (`releaseChunk` -> `ack_apply`)
   after the inline attempt found none, so the actor parks with a runnable
   job and no token, legally (the send completion is the next wake). astra
   also found the `budget_waiters != 0` skip too broad: any waiter, not this
   job's, hides a missing grant ring. Mutation 4 never tripped this half in
   round 1. The two self-rings are guarded by their unit tests (mutations 4
   and 5). Remove the half and its skips, and say why in the commit. The
   latency side is filed as t-3596; do not fix it here.
3. **The gate fails toward silence (medium; both reviewers).** Inputs that
   pass today: `@field(self, "session_mu").unlock(io)`,
   `self.@"session_mu".unlock(io)` (the string stripper erases both names);
   `_ = '"'; self.session_mu.unlock(io); // "` (a char literal opens a fake
   string that eats real code); `const ch = self.doorbell; ch.trySend(0)` and
   `.{ .bell = &(self.doorbell) }` (aliases the patterns do not match); a
   `.zig` file outside `src/` and `tests/`. **Fix:** make every check an
   allowlist that fails toward noise. (a) Strip char literals before strings.
   (b) Check the CONTENT of every string literal, quoted identifier (`@"..."`)
   and `\\` line string too: a `session_mu` or `doorbell` in any of them is a
   failure (reword the panic messages that say `session_mu`; it is cheap).
   (c) Every `doorbell` token outside a comment must match one of a short
   list of exact allowed forms (the field declaration, `ring`'s send, the
   `asyncReceive()` arms, `chanLen(u8, &<x>.doorbell)`,
   `tryRecv(u8, &<x>.doorbell)`); anything else fails, aliases included.
   (d) Scan every `.zig` file in the repo except `zig-pkg/`, `.zig-cache/`
   and `zig-out/`. Show each of the inputs above failing the gate (one plant
   at a time, removed after), and the real tree passing.
4. **The idle rows and the TLS failure regression assume the actor is
   parked (medium; both reviewers).** A 150 ms sleep is not a happens-before
   with the park. If the actor is still in a turn, it reads the handler's
   change at its next snapshot and the row passes with the ring removed.
   **Fix:** before each operation, the handler waits (with a deadline that
   FAILS the test) until the actor is observed inside its select: an
   observe-build signal set immediately before `zio.select` in
   `waitForActivity` and cleared right after it returns (reuse `actor_site`
   only if it is exactly that; otherwise add one). Keep the deadline that
   fails the row.
5. **The h2c failure regression does not discriminate (low; both).** On h2c
   `handleWriterFailed` closes `write_ch`; the pump posts `fail_all` and the
   actor wakes by `.acks`, ring or not. Keep the test, but name it a control
   in its name and comment, and do not count it as a guard of the class.
6. **Stale doc on `finishHandlerJob` (both reviewers and you).** Item 2 says
   "then wake the actor" (the send IS the wake), and the comment still says
   the request, arena and job are released there (astra: reclamation is in
   `releaseSlot` after the join). Correct both lines; change no code there.

Rerun round 1 mutations 1 to 6 on the new head, plus: (7) count the release
before the ring in one counter (the round 1 shape) and show the new
interleaving test catches it; (8) each gate plant from item 3. Full
`./zb build test` for each code mutation. Report in `tools/t3594-report.md`
(overwrite). End with `STATUS: done t94b`.

## Round 3 (review of 2f1c7cc; gpt-6-astra and Grok 4.7)

Gates at 2f1c7cc, run by the orchestrator: Mac (clean cache) all pass except
the two libcxx probes; Linux `test` passes, the t3567 binary 10 of 10, the
unit binary 10 of 10. The orchestrator's own mutations: no ring in
`unlockSession` gives 203 lost-wake panics; one counter bumped after the ring
fails exactly the new interleaving test. A nachos A/B (10 rounds, all shapes,
then 20 rounds of the two h2c complete shapes) shows no cost. Both reviewers
found the two-counter predicate free of false panics. Fix the following as
new commits on top; the earlier rules still apply.

1. **The park snapshot absorbs an unrung release (medium; astra).** In
   `assertNoLostWake`, `releases_at_park` is loaded AFTER the pre-park check.
   A release that starts between the check and that load, and skips its
   ring, is inside the park snapshot, so `auditWake` never sees it. **Fix
   (decided):** `auditWake` checks against the turn-top snapshot
   (`releases_at_turn_top`); remove `releases_at_park`. That is sound because
   no token is taken between the turn top and the select on the audited
   path. Extend the interleaving unit test with this schedule (pre-park check
   passes, then a release with no ring, then a non-bell wake) and show it is
   caught, and show it is NOT caught with the round 2 snapshot.
2. **Correct the `lostWakeSince` doc (low; astra).** "The actor takes a
   token only in a `.bell` arm" is false: `waitTicketDraining` takes tokens
   too (3183, 3199). Say why that cannot reach an audited park:
   it selects only when the ticket is dark, which needs
   `complete_receipts_fail_all`, which is set with `writer_failed`, and that
   turn breaks to teardown before either check.
3. **The interleaving test passes with its hooks removed (low; Grok).**
   Cases 1 and 2 do not prove the forced schedule ran. **Fix:** count hook
   entries and assert them, and assert the doorbell length after case 1
   (0, the actor took the token) and after case 2 (1).
4. **The gate still has holes (medium; both).** (a) It checks the source
   spelling of a literal, not its decoded value:
   `self.@"session_\x6du".unlock(io)`, `self.@"\x64oorbell".trySend(0)`, and
   `\u{73}` forms pass. Decode `\xNN`, `\u{...}` and the one-character escapes
   before the name test; a literal that does not decode is a failure.
   (b) `@field (self, ...)` with a space defeats `/@field\(/`: allow
   whitespace. (c) Zig line strings (`\\`) start anywhere a token can, not
   only at the start of a line (`const t = \\//doorbell`): recognize them
   anywhere outside other literals and comments, and check their content.
   (d) The allowed `chanLen`/`tryRecv` forms are removed as substrings, so a
   helper named `aliaschanLen(u8, &self.doorbell)` that returns the channel
   passes. Match the allowed forms as whole qualified calls with an
   identifier boundary before them (the exact spellings the tree uses, such
   as `io_queue.chanLen(u8, &self.doorbell)`), not as substrings.
   Zig has no block comments, so there is nothing to do for `/* */`. Show
   each new plant failing, one at a time, and the real tree passing.
5. **The two empty-body idle rows never wait for the park (medium; astra).**
   `/empty204` sleeps 150 ms and sends; it does not call `idleSleep`, so
   their `park_missed == 0` assertion is vacuous. Route them through the same
   parked wait.
6. **The parked wait samples, it does not prove (medium; astra). Decided:
   do not build a rendezvous.** A row can pass without the ring if an
   unrelated wake ends the select between the handler's sample and its
   release; that schedule is legal (the actor's next turn reads the state
   under the lock), so it is not a lost wake and no audit should fire. An
   idle GET has no other wake source, and mutation 1 failed every row, so
   the gap is statistical. State this limit in the comment on `idleSleep`,
   in plain words, so nobody reads the row as a proof.

Rerun mutations 1 to 7 and every gate plant (old and new). Full
`./zb build test` for each code mutation. Report in `tools/t3594-report.md`
(overwrite). End with the status line the prompt gives.

## Round 4 (review of 1298d6b; gpt-6-astra and Grok 4.7) — the last gate round

Gates at 1298d6b, run by the orchestrator: Mac (clean cache) all pass except
the two libcxx probes; Linux `test` passes, the t3567 binary 10 of 10, the
unit binary 10 of 10; no ring in `unlockSession` gives 199 lost-wake panics;
the escaped-name plants fail the gate. Both reviewers found NO defect in the
turn-top audit, the interleaving test, or the release ring. What remains is
four more ways around the text gate, the third round in a row with new ones.
So this round changes the gate's approach instead of adding patterns. Fix as
new commits on top.

1. **`session_mu`: drop function tracking (decided).** The scope tracker is
   the source of two holes: a second `fn unlockSession` (file level) is
   "allowed" by name (Grok), and a closing brace with a trailing comment plus
   a `noinline fn` keeps `cur == "unlockSession"` for the next function
   (astra). Instead: the five allowed lines are matched as EXACT trimmed text
   in `src/edge/connection.zig`, and each must occur EXACTLY once there. Any
   other line anywhere with a `session_mu` token fails. No `cur`, no indent
   rules for this check.
2. **The doorbell: nothing may chain off an allowed call (decided).** The
   send is allowed only as the exact trimmed text of `ring`'s line, exactly
   once. Every other `doorbell` use must be one of the allowed calls, and
   the gate fails if (a) anything other than whitespace and `(`, `,` or `=`
   precedes the call's first identifier (so `alias. io_queue.chanLen(...)`,
   with or without a space or newline after the dot, fails), or (b) the next
   non-space character after the call's closing `)` is `.` (so
   `io_queue.tryRecv(u8, &self.doorbell).trySend(0)` with a same-name local
   helper, and `self.doorbell.asyncReceive().impl` for `@fieldParentPtr`,
   fail). Keep `@fieldParentPtr` naming `doorbell` or `session_mu` in any
   literal as already covered by the literal check, and say so.
3. **State the gate's limit in its header, in plain words.** It catches an
   accidental direct use (the shape every past hang had). It does not stop
   a deliberate bypass: a name built at comptime outside `connection.zig`, a
   pointer to the field obtained through another type, or code that defeats
   a text scan on purpose. The fix for that is a type that owns the mutex
   (t-3600); do not build it here.
4. **The idle-row comment overstates the quiet case (low; Grok).** The
   actor stores `releases_at_turn_top` BEFORE it locks. So an unrung release
   that starts after the new snapshot still trips the park check on that
   same turn. The audit is quiet only when the release starts before that
   snapshot. Say that narrow race, not the wide one.

Plants (each must fail, one at a time, removed after; the real tree passes):
a file-level `fn unlockSession` holding `self.session_mu.unlock(io);`;
`} // end of unlockSession` followed by a `noinline fn` that unlocks
directly; `alias. io_queue.chanLen(u8, &conn.doorbell)` and the same with a
newline after the dot; `io_queue.tryRecv(u8, &self.doorbell).trySend(0)`;
`self.doorbell.asyncReceive().impl.channel`; and every plant of rounds 2 and
3. Plant with a tool that writes bytes exactly (not `awk -v` or `echo`, both
rewrite backslashes). Rerun mutation 1 and the gate. Report in
`tools/t3594-report.md` (overwrite). End with the status line the prompt
gives.

## Round 5 (final review of 3267cf4; gpt-6-astra and Grok 4.7)

Gates at 3267cf4, run by the orchestrator: Mac `test` 3 of 3 on a clean
cache, `ci` (only the libcxx probes fail), the other gates, and 4 more
byte-exact plants; Linux `test` and both binaries 10 of 10. A 20-round
latency A/B shows no cost (the earlier tls complete4 p95 rise was noise).
Both reviewers: no defect in the production change and no false panic.
Fix as new commits on top.

1. **The audit can miss an unrung release when two releases overlap
   (medium; astra).** Release A bumps `releases_started`, rings, and is
   descheduled before it bumps `releases_rung`. The actor takes A's token
   and snapshots 1. Release B bumps `started` to 2, skips its ring, bumps
   `rung` to 1. The check `rung (1) <= snapshot (1)` passes, and A's late
   increment then hides B for good. **Fix (decided):** each release takes
   its own number, `seq = releases_started.fetchAdd(1, .seq_cst) + 1`, and
   after the ring publishes `releases_rung_max.fetchMax(seq, .seq_cst)`.
   `lostWakeSince(snapshot)` is `rung_max > snapshot and doorbell empty`.
   Update the soundness argument: `rung_max > snapshot` means a release
   whose number is above the snapshot, so it started after it, has finished
   its ring. Add case 5 to the interleaving test (this exact overlap, with
   B's ring skipped): caught. Show that the anonymous count misses it
   (mutation 9).
2. **A helper extraction moves an allowed line out of its wrapper (medium;
   both).** The exact-once counts do not say WHERE the line is. Moving
   `self.session_mu.unlock(io);` into a new helper that `unlockSession`
   calls, and then calling that helper from elsewhere, unlocks without a
   ring and passes. The same for moving `ring`'s send line out of `ring`.
   **Fix:** anchor each allowed line to its wrapper. The nearest preceding
   `fn` declaration line (with any of `pub`, `inline`, `noinline`, `export`
   before it) must name the expected wrapper (`unlockSession`,
   `lockSession`, `lockSessionUncancelable`, `ring`), and the next code
   line after the unlock line must be the `actor_marker` check. Plants:
   both extractions, each must fail.
3. **`lockUncancelable` count (low; Grok).** Collapsing the two branches in
   `lockSessionUncancelable` to one call is a legal refactor. Accept 1 or 2
   for that line only; the unlock and the send stay at exactly 1.
4. **Multi-line formatting of an allowed call fails the gate (low; both).
   Decided: do not widen the matcher.** It fails toward noise. Instead make
   the failure message for a `doorbell` or `session_mu` line say: the gate
   matches exact one-line text; keep this call on one line with no trailing
   comma (`zig fmt` keeps a call on one line when there is no trailing
   comma). Check that claim about `zig fmt` by running it on a copy.

Rerun mutation 1, mutation 7a/7b, the new mutation 9, every gate plant, and
the gates. Report in `tools/t3594-report.md` (overwrite). End with the
status line the prompt gives, on a line of its own with no indent.

## Round 6 (last review of 6a12afb; gpt-6-astra and Grok 4.7) — tests only

Gates at 6a12afb, run by the orchestrator: Mac `test` 2 of 2 on a clean
cache, `ci` (only the libcxx probes fail), the other gates; the helper
extraction plant fails the gate; mutation 9 fails the interleaving test
only. Both reviewers: no release-build defect, no false panic. Two tests can
pass without doing what their names say. Fix them, with NO change to
production code or to the audit, as new commits on top.

1. **Idle rows accept a fallback response (medium; astra).**
   `tests/t3567_tls_barriers.zig` around 741-752 checks only
   `c.ended[0]`. If `resp.start` is refused in `idleStartWriteEnd` or
   `idleBareFlush`, `runHandlerJobBody` sends its fallback 500, the client
   sees END_STREAM, and the row passes without the write, end or flush.
   **Fix:** each idle handler records that every operation it was asked to
   do returned success (a per-row counter of completed operations, checked
   against the expected count), and the client checks the status and body it
   expects (not any END_STREAM; abort expects RST_STREAM). Mutation: make
   `resp.start` fail in the idle start/write/end handler (test code); the row
   must fail. Then the same for the bare-flush handler.
2. **"wakes a parked actor" does not prove it parked (low; Grok).** The
   stand-in sets `stand_in_parked` before `zio.select`; a release can ring
   into an empty channel before the receive is queued, and the select takes
   the token on its fast path. The test still fails with the ring removed
   (no token, the 5 s timer wins), so it guards the ring, but its name
   claims more. **Fix (decided):** rename the three tests to say what they
   prove: a release by a non-actor task leaves a wake that a parked or
   parking actor receives. Keep the existing comment that a ring before the
   park passes too; do not build a park probe.

Rerun mutation 1 and the gates. Report in `tools/t3594-report.md`
(overwrite). End with the status line the prompt gives, as the last line,
not inside a list.
