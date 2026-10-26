# Brief: t-3538, the server ends connections at 100 streams on the task route

You are the investigator. The orchestrator reruns every number you report,
and a reviewer from another vendor reads any fix. Do not tell the
orchestrator that something passes; it recomputes it. Do not prompt, message
or read any other agent's pane.

## The defect

Found during t-3514 (Mac, master 913b395 plus the t-3514 branch), and not
retested on current master:

```sh
h2load -c 1 -m 100 -n 5000 "$B/fragment-task?kb=64"                     # 3/3 rounds: 2791-4132 failed
h2load -c 1 -m 100 -n 5000 "$B/fragment?kb=4"                           # 3/3: 0 failed
h2load -c 1 -m 100 -n 5000 "$B/fragment?kb=4" "$B/fragment-task?kb=64"  # 3/3: 0 failed
```

- Both h2c and TLS fail (h2c: 8 of 20 runs before the t-3514 fix, 10 of 20
  after, so it is not the HEADERS-before-DATA defect).
- The t-3514 scheduler detector printed nothing, and the frame ring logged
  few peer closes, so the SERVER ends these connections.
- Hypothesis from that investigation, NOT checked: 100 streams x 64 KiB is
  more than `outbound_bytes_per_connection` (4 MiB), and a handler's wait
  for outbound space fails the connection instead of applying backpressure.
  Note `outbound_bytes_per_stream` is 64 KiB and the response is 64 KiB.

## Your job, in order

1. **Reproduce on current master (e3c8186) with a rate.** A script (absolute
   paths; macOS `/bin/bash` is 3.2; extend `tools/t3514-mixed-rate.sh` or
   write a sibling) that runs the shape N times on a fresh server each and
   prints failed per run; every run must show its work. h2c and TLS, n large
   enough to tell 50 % from 10 %. Vary `-m` (16, 50, 64, 100, 200) and the
   response size (kb=16, 64, 128) to find where it starts; that boundary is
   evidence for or against the hypothesis.
2. **Make the server say why it ended the connection.** The frame ring
   (`-Dwire-ring=true`) does not log a connection the server ends first
   (t-3541): extend it, or add a counter or a log line at each site that
   ends a connection (GOAWAY code and reason, a `failConnection` call site,
   a wait that returns an error). Keep it off by default.
3. **Name the cause with a reproduction chain**: the exact commands and the
   observation, defensible if pushed on. A hypothesis is labelled as one.
4. **Stop and report before you fix**, unless the fix is under about 30 lines
   and obviously local. A fix that changes a limit's meaning, or the
   backpressure design, is a decision for the orchestrator and the owner.

## Premises (settled)

- Read `AGENTS.md` first, and the commit messages of e7cf701, 4dfd2da and
  e3c8186 (slot pool, HEADERS before DATA, request pool), which changed this
  code recently.
- zio owns concurrency; `std.Io` abstractions are banned. zio and tls.zig are
  vendored under `zig-pkg/`.
- No external users; breaking changes are free.
- Use the Mac. nachos may be used by the orchestrator for A/Bs; do not use it.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-3538` (branch
  `carmack/t-3538`, from master e3c8186). Never touch any other worktree.
- Base tripwire, before any work: `git log -1 --format=%s` is the commit that
  added this brief, and its parent's subject starts with "Take every HTTP/2
  request's memory from a server-wide block pool". If not, STOP and report
  `STATUS: blocked`.
- `testdata/cert.pem` and `key.pem` are in the worktree.
- Kill only processes you started, by pid. Never `pkill -f`.
- If you start a long command in the background, do not end your turn
  without a STATUS line; wait for it, or say in the STATUS line what is
  still running.
- Commit per milestone. Each message is the decision record: problem,
  changes, alternatives rejected, what it depends on; markdown; plain ESL
  English. End it with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file
  under "To file".
- Report in `tools/t3538-report.md` (not committed): the repro script, the
  rates with n and the boundary you found, what the server logged when it
  ended a connection, the cause or the strongest hypothesis with its
  evidence, and what you could not verify. End with `STATUS: done t38a`.

## Round 2: t-3561, the write-ack channel panic (the owner wants it fixed now)

Your report is accepted, and the orchestrator reproduced its two key cells
(h2c and TLS `-m 100 kb=64`: 10 of 10 failing, server ended; h2c
`-m 200 kb=64`: 3 of 10 runs panicked with `write ack channel over proven
capacity`). The owner chose option A for the budget itself (real
backpressure); that is a SEPARATE change, built later on top of stage 3. Do
not start it. This round is only the panic, t-3561:

1. **Count first.** Instrument the number of outstanding write
   completions (chunks handed to the pump whose ack the actor has not yet
   applied) and its high-water per connection, and log it at the panic.
   Reproduce the panic with `tools/t3538-rate.sh` (h2c `-m 200 kb=64`, and
   `-m 100` with `OUTBOUND=8388608`) and report the counted high-water
   against the channel capacity
   (`control_entries_per_connection + max_streams_per_connection`).
2. **Derive the real bound from the code**, not from a measurement: what
   limits how many chunks can be outstanding at once (the outbound budget
   in wire bytes divided by the smallest chunk? the write pool size plus GPA
   fallback chunks? control chunks?). Quote the lines that enforce it. Say
   whether the bound still holds when stage 3 lands (master's write pool
   gains backpressure and small frames can take write chunks; read
   `/Users/rsaunderson/Source/mine/starh2-wt-stage3/tools/stage3-brief.md`,
   read-only).
3. **Fix** so the channel can never overflow: size it from the derived bound
   (planned in `slotBootBytes`), or make the producer unable to exceed it
   (count outstanding chunks and stop handing chunks to the pump at the
   bound). Say which and why. A panic that a client can trigger is not an
   acceptable outcome at any limit setting.
4. **Tests:** a test that panics on the current code and passes after the
   fix, at default limits and at a doubled outbound budget; the rate script
   shows 0 crashes in those cells (n=10 each, h2c and TLS).
5. **Mutation:** restore the old capacity (or the missing check); record
   which test fails.

Rebase this branch onto current master (fe1258e) first; resolve by hand.
Gates: `./zb build test`, `./zb build ci` (only the two
`macos-libcxx-probe` steps may fail, t-3540), `./zb build tls-smoke`. Same
rules as round 1 (stop before a fix larger than about 100 lines and say
why; no push; "To file" list). Add a "Round 2 (t-3561)" section to the
report. End with `STATUS: done t61a`.

## Round 3: t-3561 review fixes (after review of c776ed2)

The orchestrator's gates on c776ed2 (Mac, clean cache) pass. A cross-vendor
review found:

1. **High: a bare flush barrier bypasses the pause hooks.** `flushCb` can
   push a flush ticket directly (`sendFlushTicket`) before `processIntents`
   runs the scheduler, so many streaming handlers that call `Body.flush()`
   with nothing pending can push past the channel. The push check that
   catches it is `std.debug.runtime_safety` only, so ReleaseFast can still
   overflow and crash. Fix both layers:
   - Route a bare flush barrier through the same gate (queue it as
     scheduler work, or check `h2cPause` before the direct push and hold it
     the same way). Say which and why.
   - Make the push check active in EVERY build: past the bound it fails the
     write (`WriteFailed`, the connection fails closed) instead of handing
     the pump a chunk it cannot ack. A crash a client can trigger must not
     depend on the build mode.
   - Test: many streams that each `Body.flush()` with nothing pending, with
     `test_hold_ack_receive`, at default limits; it must fail (panic) on
     c776ed2 and pass after. Mutation: restore the direct push.
2. **Low: `test_hold_ack_receive` is compiled into release builds**
   (an acquire load per actor turn). Gate it at comptime behind the
   repository's test-observation option, like the other test hooks.

Also: before round 3, check whether any OTHER path pushes to the h2c pump
outside the scheduler's pause hooks (a direct `pushWriteChunk` caller), and
list each with its gate.

Same rules. Gates as before. Add a "Round 3 (t-3561 review)" section to the
report. End with `STATUS: done t61b`.

## Round 4: t-3561, a lost wake in waitForAckRoom (after review of bb41aac)

The orchestrator's checks on bb41aac: Mac gates pass; the ReleaseFast crash
cell (h2c `-m 200 kb=64`) has 0 crashes in 10 on nachos. The Linux suite
failed one test, `edge.request_pool ... alignment above a page ...`, which
is a pre-existing flaky test on master (6 of 40 runs on master's tree, 10
of 40 on yours; filed as t-3568). Do not fix that here.

The review found one defect, inside round 3:

1. **The waiter handshake is not sequentially consistent.** The report says
   both sides are `seq_cst`, but `AckLedger.sub` uses `fetchSub(.acq_rel)`
   and `h2cAckRoom` reads `owed` with `.acquire`. This is the store-buffer
   pattern: the actor can subtract and read the old waiter count (0, no
   wake) while the handler increments the waiter count and reads the old
   `owed` (no room) and parks with room available, with no later wake if no
   other completion comes. Fix: make the subtraction `seq_cst` and re-read
   `owed` with a dedicated `seq_cst` load after registering the waiter (the
   ordinary pause checks may stay `acquire`). Say why it is now correct in
   a comment beside the code.
   - A test cannot reliably show a memory-order bug on this hardware; say
     so, and instead add a comment-level proof and keep the existing tests
     passing. If you can build a deterministic interleaving with test hooks
     (the actor's subtract and waiter read separated by a hook), do it.
2. Correct the round-3 report's claim (both sides seq_cst) where it is
   recorded, including the commit message if you amend; otherwise say it in
   the new commit.

Same rules. Gates as before. Add a "Round 4" section to the report. End
with `STATUS: done t61c`.
