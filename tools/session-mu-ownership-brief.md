# Brief: teardown must not touch actor-owned state from outside the actor

`wakeHandlerWaiters` mutates the scheduler and the outbound byte ledger without
`session_mu`. That is the cause of a rare `ci` failure, and it is proven, not
suspected. This brief is the fix.

Read section 4 before you write code. The obvious fix passes one acceptance axis
and fails another on purpose.

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
that state with no `assertSessionHeld`. The eight that have it: 2902, 3643,
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

## 2. Refuted — do not re-derive either of these

1. **The skip-shrink story.** `buildData` sets `pending_data_outbound_release`
   unconditionally while the shrink in `emitOneData` is guarded by
   `pw.stream_id == sid`. Reads as a clean double-release. A probe on that guard
   fired 0 times in 360 concurrent runs while the panic reproduced. Also refuted
   by code order: the shrink at `fair_scheduler.zig:652` runs before the sink at
   668, so the quantum has already left `pw.len` at every reachable failure.
2. **`drainEmit`'s first catch double-paying with `handleWriterFailed`.** Same
   refutation, plus both run under the same lock on one actor turn, so it would
   be a same-thread double-pay rather than a race.

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

One ordering fact is load-bearing: `connection.zig:1013-1014` says a holder can
park inside the critical section on `write_ch.putOne` in `sendAccountedWire`.
Whatever you build, a handler parked there must be evicted before anything tries
to acquire the mutex behind it.

## 4. The two acceptance axes. Both are hard. Read this twice

**Axis A — ownership is honored.**
`self.assertSessionHeld("wakeHandlerWaiters");` is present at the top of
`wakeHandlerWaiters` in your diff, and the full suite passes with it in place.

**Axis B — teardown does not acquire `session_mu`.**
`shutdownHandlers`, and anything it calls on the teardown path, acquires no
lock. The count of `lockSession` plus `lockSessionUncancelable` call sites in
`src/edge/connection.zig` does not increase.

**Axis B exists because Axis A alone has a degenerate pass.** Wrapping the sweep
in `lockSessionUncancelable` satisfies Axis A completely and is NOT this task.
It also carries the failure mode this brief is trying to avoid: teardown then
CONTENDS for a mutex it used to own, and one handler parked inside that mutex
which the channel closes do not wake makes the acquisition wait forever.

The shape being asked for: the terminal-cause and ledger sweep happens where the
actor ALREADY owns the turn, so no acquisition is needed and no parked holder
can block it. `run`'s loop has several `break` sites; giving it one defined exit
that runs the sweep under existing ownership is the likely shape, but the shape
is yours to choose. `AGENTS.md`: "When a design needs a new mutex, that is the
signal to ask which task should have owned the state instead."

**Axis C — the sweep still happens.** Deleting the sweep passes A and B and is a
failure. `lifecycle: 100x write-fail ticket wake (no hang)` and
`lifecycle: global stream cap and cancellation storm` are the guards: handlers
must still be woken and their pending bytes still released.

## 5. Grading — I recompute everything; report nothing as evidence

Do not tell me the tests pass. Tell me what you changed and why. I run these.

```sh
DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer zig build ci   # exit 0
```

The concurrency instrument, because this does not reproduce standalone
(30 of 30 clean) and `zig build` CACHES a successful test run, which biases any
`zig build` tally toward green:

```sh
BIN=$(ls -t .zig-cache/o/*/test | head -1)   # the 17-test lifecycle binary
for round in $(seq 1 20); do
  for j in $(seq 1 8); do $BIN > /tmp/lc/r${round}_$j.txt 2>&1 & done
  wait
done
grep -l panic /tmp/lc/r*.txt | wc -l                          # must be 0
for f in /tmp/lc/r*.txt; do grep -cE '^[0-9]+/17 ' $f; done | sort -u   # must be 17
```

That last line is the validity check: 17 means the run executed, 0 means it was
a cached or crashed no-op. A batch reporting zero panics with zero test lines is
a batch that did not run.

Baseline to beat: 1 panic in 80 runs, and 1 in 160, at the current HEAD.

**No hang.** Every run above must finish. Wrap each in a kill after 240 s. A fix
that deadlocks looks exactly like a fix that is slow.

## 6. What will shift, so you are not surprised

- Shutdown latency. The sweep may now queue behind in-flight work.
- Reordering per-slot cause-wake-handoff into all-causes-then-all-handoffs
  raises the frequency of the `prev == reported` branch (3391).
- Any gate counting reaper enqueues or ticket stats moves.

## 7. Out of scope — file it, do not fix it here

`cancelHandler` calls `enqueueReaperOrFail` UNDER the lock, and its no-reaper
branch `h.cancel` (3252) waits for a handler while holding `session_mu`. That is
a pre-existing deadlock shape on the live-cancel path. It is not this change.
