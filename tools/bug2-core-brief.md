# Brief: read the bug-2 core (a segfault in zio registerTask during teardown)

Same seat, worktree `/Users/rsaunderson/Source/mine/starh2-wt-dst`. This is an
investigation: no production change unless the cause is proven and the fix is
small; on a design question, STOP and report.

## What is known (measured by the orchestrator)

- Once in about 54 full-ci runs on nachos, never in 600 isolated runs of its
  binary, the test `admission: an h1c client queued behind a full connection
  limit is served when a slot frees` died. The test's own h1c case had already
  printed "queued client served 0 ms after the slot freed".
- A core of it is preserved on nachos: `~/src/s-3759-h1grade/bug2-core/`
  (`core.323926`, 2.2 GB) with a binary that matches it, `test` (rebuilt from
  the same tree, `~/src/s-3759-h1grade/diag-tip`, which is `d33e615` plus a
  t3567 test patch; gdb resolves every frame of the core to source lines with
  it). gdb is on nachos (`gdb -batch -q -ex ... test core.323926`); there is
  no lldb there. Do not delete or move these files; copy what you need.
- Thread 1 (LWP 323926): SIGSEGV in `task.spawnTask` called from
  `Io.Group.concurrent` in `edge.server.Server.acceptLoop`; the fault went
  through zio's `coro.stack.stackFaultHandler`, which passed it on. Zig's
  handler then aborted. Earlier output named `task.zig:796`:
  `if (current_executor.runtime == task.runtime)`.
- Thread 3 (LWP 323930): pc = 0, called from `ev.loop.LoopState.markCompleted`
  (`markCompletedFromBackend`, `io_uring.poll`): a completion callback pointer
  read as null.
- Thread 2 was printing the segfault report.

## Questions, in order

1. Thread 1: which load faulted (instruction, registers, both operands:
   `current_executor`, `task`, `task.runtime`)? Is the faulting address in a
   coroutine stack's guard region, freed heap, or unmapped?
2. Thread 3: which completion was being delivered (its address, its op type,
   its owner), and why was its callback null: freed, zeroed, never set, or
   reused? Which task or object owned it?
3. Did both faults come from one cause (for example a runtime or executor
   torn down, or an I/O op's owner freed, while io_uring still held the op)?
   What was the test doing at that moment (serve shutting down, runtime
   deinit, the queued client's connection ending)?
4. A deterministic or high-rate reproduction: the in-process churn the
   designers agreed on (create, serve, stop and destroy a runtime and server
   many times in one process, keeping the full-suite predecessor order if it
   matters), aimed at the window the core shows. Report the rate.

Label every claim [MEASURED] with the gdb command and output, or [INFERENCE].
Never conclude from a single stack alone; read live state (registers,
memory) as the memory note "wedge-lldb-live-state" in this repo's history
advises.

## Rules

nachos only in `~/src/s-3759-h1grade/bug2-core` (read) and a new
`~/src/dst-witness/bug2` (work). Time limits on every run. Kill only your own
PIDs. Never write the shared Zig cache by hand. Report to
`tools/bug2-core-report.md` (not committed).
