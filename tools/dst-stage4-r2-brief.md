# Brief: DST stage 4, round 2: make the TSan lane green by fixing zio

Same seat, worktree and branch (`starh2-wt-dst-tsan`, `carmack/dst-tsan`).
Tripwire: HEAD is the commit adding this brief, first parent `f686a27`. The
zio fork worktree `/Users/rsaunderson/Source/mine/zio-wt-sim-port` (branch
`sim/dst-m2`, HEAD `2f727e3`, pushed) is yours for this round. Push only the
`sim/dst-m2` branch to dimenus/zio; never touch `main` or other branches.

## Orchestrator's decisions on your round-1 question

- The lane stays in Debug. No suppression for class A: a TSan suppression
  matches any frame, so it would hide every real race in zio's wake and wait
  paths. Fix the cause instead.
- ReleaseSafe runs come later, after t-3837 (mremap) has a lane allocator that
  never remaps.
- t-3831 (the starh2 actor reads FairScheduler without session_mu) is NOT
  yours: it is on the t-3759 series, which another seat is editing. Leave it.

## Do, in order

1. **t-3832** (zio): the three Debug sites that copy a whole tagged union to
   read its tag across threads (`wake`, `Waiter.wait`, `Waiter.signal`; the
   task note has the exact sites). Read the tag without copying the union
   (an atomic or a pointer read of the tag field; whatever the field's real
   synchronisation already is). Show each report gone under TSan, and the
   bug-2 positive control still firing on `182b0f6` code paths (the control
   uses its own pinned zio, so it is unaffected; re-run it once anyway).
2. **t-3836** (zio): the work-stealing ring's plain loads and stores. First
   decide from the code and the Chase-Lev / tokio-style protocol it follows
   whether this is a real race (a torn or stale read that is then used) or a
   benign one that only needs atomic accessors. Fix with the right atomic
   orderings either way; a test that fails before if one can be made (say if
   not and why).
3. **t-3835** (zio, inference): sockreg detach reading queue links the poller
   writes after dropping the lock. First a reproduction (a zio test, a sim
   fixture, or a TSan run that shows the double-complete or its precondition);
   only then a fix. If it cannot be reproduced, write why and leave it filed.
4. For each zio fix: zio's native suite on both machines, `dst-zio` D1-D5 and
   mutants (in a scratch starh2 export pointing at your new zio), a commit
   with a decision-record message, push `sim/dst-m2`.
5. Move this branch's zio pin (root and `tools/zio-dst`) to the new zio, run
   the TSan lane 3 times on nachos, and report every remaining report with its
   triage. The goal is a green lane with no suppressions; if one remains that
   is truly a false positive, it needs the written happens-before edge.

Rules as before: time limits, fresh `--cache-dir`, never `rm -rf` a path built
from variables, never write the shared Zig cache by hand, nachos only in
`~/src/dst-tsan`. Do not tell the orchestrator anything passes. Append a
"Round 2" section to `tools/dst-stage4-report.md`. Stop and report on any
design question.
