# Brief: t-3818, zio registerTask reads a task after publishing it

Same seat and worktrees as the zio port (`zio-wt-sim-port`, branch
`sim/dst-m2`; `starh2-wt-dst-port`, branch `carmack/dst-zio-port`). Tripwire:
the zio worktree HEAD is `182b0f6` and the starh2 HEAD is the commit adding
this brief, first parent `dffd776`. If not, STOP.

## The bug (root-caused; evidence in
`/Users/rsaunderson/Source/mine/starh2-wt-dst/tools/bug2-core-report.md`)

`registerTask` (`src/task.zig:793-796` on the pin) calls
`Executor.scheduleTask(task)` and then reads `task.runtime`. For a group task
the spawner holds no reference (spawnTask's +1 is the group's), so another
executor can run the task to completion and free it in between: a read of
freed memory. A starh2 ci core showed it faulting (SEGV_MAPERR on
`task.runtime`, task pointer unmapped). The orchestrator reproduced it in zio
alone on nachos: 1,000,000 group spawns on 2 executors read a freed task 1
time stock, 499,766 times with a ~20 us spin after `scheduleTask`, and 0 with
the fix.

## Do

1. A regression test in zio that FAILS on `182b0f6` and passes after the fix,
   deterministically or at a high rate, without a production hook that
   survives in release builds. Options: a test-only comptime window (the
   `sim_mutant`/`options` shape, compiled out otherwise) plus a check that the
   task was not read after publication; or a sim fixture if the simulator can
   express it. Say which and why.
2. The fix: compare `current_executor.runtime == rt` (or read the runtime
   before `scheduleTask`). Then read the rest of `spawnTask`/`registerTask`
   and every spawn path (`spawnInto`, `JoinHandle`, `std.Io` group path) for
   any other read of a task after it is published; fix each the same way, each
   with a test.
3. zio's native suite on both machines (compare with your earlier baseline),
   `dst-zio` D1-D5 and mutants.
4. Push `sim/dst-m2` to dimenus/zio, then in starh2 move the pin (root and
   `tools/zio-dst`) to the new commit; full starh2 ci once on each machine on
   a git archive.

Commits with decision-record messages. Never write the shared Zig cache by
hand. Append a "t-3818" section to `tools/dst-port-report.md`. Stop and report
on any design question.
