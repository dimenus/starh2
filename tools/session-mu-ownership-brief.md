# Brief: teardown must not touch actor-owned state from outside the actor

`wakeHandlerWaiters` mutates the scheduler and the outbound byte ledger without
`session_mu`. That is the cause of a rare `ci` failure, and it is proven, not
suspected. This brief is the fix.

Read section 4 and section 6 before you write code. Four previous attempts
failed here. Each one drove every named check to zero and left something worse
than it found, and section 6 is the ledger of exactly how.

## 0. Tripwire — run first, STOP on any mismatch

```sh
git rev-parse --short HEAD        # must match the SHA in your prompt
git rev-list --count HEAD..origin/master   # expected: 0
zig version                       # must be 0.16.0
```

Report a non-zero count and stop. Do not rebase and continue: a quiet self-fix
hides the launch defect, so the next launch repeats it.

On macOS, build with `DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer`
or every C++ step dies inside libc++.

## 1. The established facts

Each of these was measured. Do not re-derive them.

**The violation.** `wakeHandlerWaiters` (`src/edge/connection.zig:3307`) calls
`sched.removePending` and `applyOutboundRelease`. It is the ONLY path touching
that state with no `assertSessionHeld`. The SEVEN that have it: 2902, 3643,
3829, 4110, 4188, 4194, 5193.

**The proof.** Add `self.assertSessionHeld("wakeHandlerWaiters");` to the top of
it. `lifecycle: DebugAllocator clean under live SSE reset/shutdown` then panics
immediately and deterministically:

```
session_mu not held at wakeHandlerWaiters
  connection.zig:3308 wakeHandlerWaiters
  connection.zig:3376 shutdownHandlers
  connection.zig:2987 run
```

**The scope, measured.** Tag all three callers and print on unlocked entry:
7774 unlocked entries across 40 runs, ALL of them `shutdownHandlers`, ZERO from
either `cancelHandler` site (3280, 3285). Those two reach it only through
`applyStreamResetIntent` inside `materializeIntents`, which asserts the lock. So
this is one call site, and it fires on every shutdown rather than rarely.

**The two faults it produces.** An over-release driving `pending_outbound_held`
negative, and a leak where `held != 0` at `deinit`.

## 2. Refuted — do not re-derive any of these

1. **The skip-shrink story.** `buildData` sets `pending_data_outbound_release`
   unconditionally while the shrink in `emitOneData` is guarded by
   `pw.stream_id == sid`. Reads as a clean double-release. A probe on that guard
   fired 0 times in 360 concurrent runs while the panic reproduced. Also refuted
   by code order: the shrink at `fair_scheduler.zig:652` runs before the sink at
   668, so the quantum has already left `pw.len` at every reachable failure.
2. **`drainEmit`'s first catch double-paying with `handleWriterFailed`.** Same
   refutation, plus both run under the same lock on one actor turn, so it would
   be a same-thread double-pay rather than a race.

3. **A mutex deadlock as the cause of the hang.** A live stack sample of a hung
   process showed the main thread and all three workers parked in `kevent` with
   nothing runnable. Nobody was blocked on a lock. It is a lost wakeup: the join
   waits for a completion nobody posts, with `live=1 slots=1 reaper=1`.

## 2b. Settled std.Io vs zio — do not re-derive

Measured in this tree. A finding that needs the negation of one of these is
out of scope.

1. **`zio.select` keeps a committed item across a cancel. `std.Io.Select` does
   not.** `std.Io.Select.cancelDiscard` drops results of tasks that finish
   while cancelation is in flight. lalinsky/zio closed the twin hole with a
   claim-before-consume protocol (issues #700, #701, #706), and all three are
   in the pinned revision. t-1760 hit the std side twice on H1: lost socket
   bytes, then the handler's only completion token. The fix that stuck
   (t-1785) was to stop using `std.Io.Select` there and wait with `zio.select`.
   If this change waits on a mix of things, use `zio.select`.
2. **`error.ReadFailed` is terminal in std.** Public `Io.Reader.readVec` has
   two callers in std (`Io/net.zig` `streamImpl`, `compress/flate/Decompress.zig`).
   Both use `try`. Nothing in std survives a failed `readVec` to notice that
   its partial count was dropped. Do not recover from `ReadFailed` and keep
   using the connection.
3. **A cancelation arrives as `ReadFailed`, not as `Canceled`.**
   `lib/std/Io/net.zig`: `Stream.Reader.Error` is `Io.Operation.NetRead.Error ||
   Io.Cancelable`, and its `readVec` does `r.err = err; return error.ReadFailed`.
   The real cause sits in the reader's `.err`. Zig issue #30910, closed.
   starh2 never reads `.err` (t-1792). If you branch on a reader error, this
   is the trap.
4. **`Io.Reader.readVec` reports its partial count on `EndOfStream` and
   discards it on `ReadFailed`.** It copies what the reader already holds and
   advances seek BEFORE the vtable, then the `ReadFailed` arm returns the
   error without those bytes. Live at Zig master. Unreported upstream. It
   cost 11 of 12 failing runs on a 936-byte pipelined request.
5. **Never cancel a read.** zio issue #668: race a socket read against a
   wakeup with `zio.CompletionQueue`, "prefer NetRecv in your own code and
   leave it armed". That is what `tls_edge.Pump` always did. TLS measured 0
   of 24 failures while the cleartext twin failed 16 of 24 in the same runs.
   Cancel a WAIT. Do not cancel a READ.
6. **`session_mu` is a `std.Io.Mutex`, not a `zio.Mutex`.** `connection.zig`
   field init. zio's `Mutex.lock()` takes no `io`. `lockUncancelable(m, io)`
   is real std API and std uses it internally. "Wrap the sweep in the lock"
   is idiomatic FOR std.Io. It is still not this task: Axis B exists because
   that wrap satisfies Axis A and keeps the defect's shape. Teardown then
   contends for a mutex it used to own.
7. **Method.** Zig lives on Codeberg (`codeberg.org/ziglang/zig`). Local
   clone: `~/Source/oss/zig`. GitHub `ziglang/zig` moved and was last pushed
   2025-11-27; zero hits there are not evidence. `zig build` caches a
   successful test run and re-executes a failing one, so repeated `zig build
   test` counts are biased toward green. Count per-run test lines. That is
   why section 5 greps for them.
8. **Open: `cad283d`.** `AGENTS.md` cites it for earlier `session_mu`
   ownership fixes. That SHA is not an object in this clone (t-1809). Do not
   treat the house answer last time as "add the lock" or "move the
   ownership". Treat the citation as the document's, not as verified.

## 3. The constraint that makes this hard

`connection.zig:5020`: "A wait inside `session_mu` is a deadlock: the actor
completes the ticket only after dropping the mutex to apply write acks, and it
needs the mutex to produce bytes."

Everything in the current sweep that CAN wait, with line numbers, so you do not
have to find them again:

- `enqueueReaperOrFail` else-branch: `h.cancel` joins the handler (3252).
- `enqueueReaperOrFail` `!queued` fallback: `releaseSlot` awaits (3215).
- `drainPendingCompleteReceipts(true)`: waits for in-flight acks (2246).
- `runPendingInline`: takes `session_mu` itself (4858, 4879). Self-deadlock.
- `completion_ch.receive()` (3431).

Everything in the cause-and-ledger pass does NOT wait: `setCause` is an atomic
store, `findPending`/`removePending` are pure data structure, `tickets.wake` is
`event.set` which makes a waiter runnable without parking, `applyOutboundRelease`
is atomics only.

One ordering fact is load-bearing: `connection.zig` says a holder can park
inside the critical section on `write_ch.putOne` in `sendAccountedWire`. That
holder is the actor, not a handler (`wire_pump.zig` names the same park).
Cancel of a handler does not wake that wait. `write_ch.close` does. Evict that
putter before anything tries to acquire the mutex behind it.

## 4. Acceptance. SIX axes, all hard, all graded together

A build fails if it fails ANY axis. There is no averaging and no trading one
against another. Four previous attempts each drove the named failures to zero
and left something worse, so read section 6 before you decide anything.

**Axis A — ownership is honored, and the guard means what it says.**
`self.assertSessionHeld("wakeHandlerWaiters");` is present at the top of
`wakeHandlerWaiters`, and the full suite passes with it in place.

That alone is spoofable, so it is not the whole axis. `assertSessionHeld`
(connection.zig:5014) reads a plain `session_held: bool`. Writing
`self.session_held = true` at the top of `shutdownHandlers` passes the assert,
passes the lock-site count, changes no waiting, and renames the violation as
ownership. The mechanical guards against that:

- `session_held = true` appears in EXACTLY two places, both inside the lock
  wrappers, and `session_held = false` in exactly one. The grader counts them.
- The guard is ARMED once against the final diff: in a scratch build, drop the
  lock before the sweep and confirm the panic fires. A guard that has never
  fired under your change is a dormant guard, and a dormant guard is an
  untested guard.

**Axis B — teardown acquires no lock.**
`shutdownHandlers`, and everything it reaches TRANSITIVELY, acquires no lock.
The count of `lockSession` plus `lockSessionUncancelable` call sites in
`src/edge/connection.zig` does not increase. Transitively is the word that
matters: a previous attempt passed a grep because `shutdownHandlers` did not
lock, while calling `runPendingInline`, which did.

A count is not a mechanism, and there are three ways through it: call
`session_mu.lockUncancelable` raw, put the acquisition in another file, or
delete one site and add one. So the axis is enforced at runtime, not by grep:
an `in_shutdown_sweep` flag set by `shutdownHandlers`, asserted false in both
lock wrappers and at any raw mutex use. That makes "transitively" a trap that
fires on any interleaving reaching it, at any n and regardless of grep scope.
The grader also counts raw `session_mu.lock` occurrences repo-wide.

**Axis C — the sweep still happens.**
Deleting the sweep passes A and B. `lifecycle: 100x write-fail ticket wake
(no hang)` and `lifecycle: global stream cap and cancellation storm` are the
guards: handlers are still woken and their pending bytes still released.

**Axis D — no run hangs, and the axis does not depend on n.**
Every run of the lifecycle binary finishes. Graded by DURATION, not output: a
normal run is 9 to 11 seconds. Any run beyond 60 seconds fails.

Counting alone cannot grade this and you should not pretend otherwise. At the
measured base rate of about 1 in 400, a 320-run batch shows zero hangs 45% of
the time WITH THE DEFECT UNCHANGED, and a fix that only makes it 10x rarer
passes 92% of the time. Rejecting "unchanged" at 95% would need about 1200
runs, and no affordable n separates "1 in 4000" from "zero".

So the axis must be made n-independent, by one of:
1. A conservation counter: completions the join expects equals completions
   posted, asserted at `deinit`. A lost wakeup then fails loudly on every run
   that reaches the window.
2. An in-binary watchdog: teardown records what it waits for and, past a
   deadline, prints `live/slots/reaper` and panics. This converts a silent
   unbounded outcome into a loud deterministic one and removes the external
   reaper from the trust chain.

**The cheap wrong pass, which is attempt 4 with a timer on it:** wait up to a
few seconds, then proceed. Every run finishes, D passes, and the hang becomes
an abandonment, landing in E and F. So D also requires: the MEDIAN stays 9 to
11 seconds, and any run past 20 seconds needs a named explanation in your
report. A second mode around 14 to 16 seconds is the timeout-fallback
signature.

**Axis E — every handler slot is finalized EXACTLY ONCE, and it is counted.**
Not at least once, which permits a double free. Not at most once, which permits
a leak. `finishHandlerJob` and every other finalizer decrement counters, reset
the arena and release the request body, so a second call double-frees. A
previous attempt called it twice on one slot because a complete handler with an
attached pending receipt legitimately has `join == null`.

Prose cannot grade this, and the text checks provably cannot see it. The
mechanism: a per-slot `finalize_count`, incremented UNCONDITIONALLY in every
finalizer, asserted `== 1` at slot release, plus one conservation sum at
`deinit` — slots ever `in_use` equals slots finalized. That single number grades
E and F together, on every path, at any n. Arm it once with a debug test that
forces a double call and must trap.

**An idempotency flag is NOT a fix and is banned.** `if (slot.finalized) return;`
passes E, F, D and every text check while leaving the ownership confusion that
caused the double call in place. It does not fix the defect; it blinds the only
instrument that can see it. The counter must increment before any such guard.

**Axis F — nothing is abandoned.**
`accounting not zero` never appears. A slot that is `in_use` at teardown has
exactly one party responsible for posting its completion and awaiting its
handle. Abandoning a handler to avoid waiting for it is the failure this axis
exists to catch, and it produces a use-after-free rather than a hang.

### Why six, and what the laziest passing output looks like

Each axis exists because a previous attempt satisfied the others without it.

- A alone: wrap the sweep in `lockSessionUncancelable`. Passes A, defeats the point.
- A and B: delete the sweep. Hence C.
- A, B and C: the sweep runs under the actor's hold and a handler is never woken,
  so teardown waits forever. Hence D.
- A through D: stop waiting for handlers at all. No hang, and a use-after-free.
  Hence F.
- A through D and F: finalize defensively in more than one place so nothing is
  missed. No leak, and a double free. Hence E.

E and F are a PAIR and must be graded together. E alone invites leaking, F alone
invites double finalization. If you find yourself adding a second finalization
site "to be safe", that is the exact move that failed.

**Axis G — memory safety is its own gate.**
Two of the last three defects were a use-after-free and a double free, and the
text checks pass both. Detection cannot stay incidental. The full lifecycle
binary runs under DebugAllocator with `never_unmap`, and the grade is the
allocator's own trap and leak output — an oracle you did not write. The
finalize-count conservation above is the same axis expressed as an invariant.

## 5. Grading. Nothing you report is evidence

Do not tell me the tests pass. Every number below is recomputed here from your
worktree, with a source you did not write.

```sh
DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer zig build ci   # exit 0
```

Get the test binary from zig, never by searching the cache. A previous grading
round measured an hour-old binary found by mtime, because a cached build does
not touch the file:

```sh
BIN=$(zig build test-lifecycle --verbose 2>&1 | grep -oE '\.zig-cache/o/[a-f0-9]+/test' | tail -1)
N=$(grep -c '^test ' tests/lifecycle.zig)   # recompute; do NOT hardcode 17
$BIN | grep -cE "^[0-9]+/$N "      # must print $N BEFORE any batch is trusted
```

Then 320 runs, eight concurrent, recording the duration of each:

```sh
run_one() {
  local s=$(date +%s); "$1" > "$2" 2>&1 & local p=$!
  ( for i in $(seq 1 40); do sleep 5; kill -0 $p 2>/dev/null || exit 0; done
    ps -p $p -o comm= 2>/dev/null | grep -q test && kill -9 $p ) & local w=$!
  wait $p 2>/dev/null; local rc=$?; kill $w 2>/dev/null
  echo "$rc $(( $(date +%s) - s ))" > "${2%.txt}.meta"
}
```

The reaper checks the pid is still the test binary before killing it. An earlier
reaper did not, and a plausible theory that it had killed an innocent run caused
a real hang to be retracted for an hour.

Three numbers decide it, and the first two are not text matches:

1. **Duration histogram.** `cut -d' ' -f2 *.meta | sort -n | uniq -c`. Any run
   past 60 s fails axis D.
2. **Test lines per run.** `grep -cE '^[0-9]+/17 '` on every run. 17 means the
   run executed. Anything else is a run that died or hung. A batch reporting
   zero failures with no test lines is a batch that did not run, and that
   happened here: 160 runs of nothing once read as a perfect score.
3. Only then the text checks: `panic`, `not held`, `accounting not zero`,
   `cap-crossing run failed`.

**Every text check is armed before it is trusted.** `accounting not zero` is
printed by `tests/lifecycle.zig:105`, a file you can edit, so a grep for a
string the code no longer emits passes vacuously. For each detector the grader
builds one forced-failure variant and requires the string to appear. A check
that has not been shown to fire is not a check.

**Test changes are recomputed, not accepted.** The grader runs
`git diff origin/master -- tests/` and fails any hunk your report does not name
and justify. Strengthening a precondition is legitimate; relaxing an assertion
is not.

**The grader reads your diff's functions first.** Your report lists every
function the diff touches. Rounds 2, 3 and 4 each planted the new defect inside
the previous round's fix, so that list is where the next defect is.

**Do not count with `zig build`.** It caches a successful test run and
re-executes a failing one, so repeated invocations are not repeated trials and
the tally is biased toward green.

**A use-after-free and a double free pass every text check in step 3.** So does
a hang. Steps 1 and 2 are the instrument; step 3 is a convenience.

## 6. The defect ledger. Four attempts, what each one taught

Do not rediscover these. Each line is a real failure that was measured.

| attempt | fixed | introduced or left |
|---|---|---|
| sweep on last actor turn | panic gone, 0 in 360 | outbound leaked 947; wake-fail 4 in 160 |
| close queues, freeze inline off the mutex | outbound 947 to 0; wake-fail to 0 | axis B failed again via a close-then-relock helper; slots and reaper residue 4 to 71 |
| freeze-and-sweep under existing hold | slots residue gone; all text checks clean | a HANG, about 1 in 400, in the original test |
| do not park on teardown; wake before waiting | ungated test hook gated | hang rate unchanged; plus a use-after-free and a double free |

Read the last two rows together. Both drove every named failure to zero. Both
were worse than what they replaced. That is what this brief is now shaped to
prevent.

### Sound under the OLD ordering — now your obligation to re-establish

These held before your change and are conditional on code order you are about to
restructure. "Do not undo" would be the wrong instruction: preserving the lines
does not preserve the properties. After your change, demonstrate each still
holds and say how you demonstrated it.

- The wake-to-receive ordering. `completion_ch` is buffered, so a completion
  posted between the wakes and the receive stays consumable, and terminal cause
  and `writer_failed` are published before waking.
- Two sweep sites do not double-pay: the first removes active-slot pending
  entries before the second collects scheduler-only leftovers, and `findPending`
  guards a repeated visit.

## 7. What a CONFORMING build can still destroy

All six axes can pass while something else breaks. Before you call it done, ask
what your design lets through that no axis measures. The three that were found
this way, each by reading rather than by a check:

- A test hook left ungated, so a release build pays for an observability store.
- A test weakened so a fix passes. Strengthening a precondition is legitimate;
  relaxing an assertion is not. Every changed line under `tests/` must be
  justified in your report, by name.
- A finalizer that is correct on every path you considered and non-idempotent on
  one you did not.

## 8. Out of scope. File it, do not fix it here

`cancelHandler` calls `enqueueReaperOrFail` UNDER the lock, and its no-reaper
branch `h.cancel` waits for a handler while holding `session_mu`. Pre-existing
deadlock shape on the live-cancel path. Not this change.
