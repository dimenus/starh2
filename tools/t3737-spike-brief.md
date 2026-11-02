# t-3737 spike: a handler must not hold its connection

This is a SPIKE. The goal is to learn whether the design below works in this
code, what it costs, and where it does not fit. It is not the landing commit.
Correct and measured beats complete and polished. Cut corners only where this
brief says you may, and list every corner you cut in your report.

## Premises — settled, do not argue them

- starh2 has no external users. Breaking changes are free now. Do not add a
  compatible shim beside the new design.
- Prefer share-nothing (AGENTS.md, "Premises"). A task owns its state and
  exchanges messages; a new lock is a signal that the wrong task owns the state.
- zio owns concurrency. No `std.Io` concurrency abstraction (AGENTS.md,
  "Ownership and concurrency"; `./zb build std-io-gate`).
- Read a dependency's source before you use it: `zig-pkg/zio-*/src`.

## The problem, measured

On master (9547164), when a connection ends while an HTTP/2 task handler is
still running, `waitTeardownCompletions` waits for the handler, and after 5 s
with no progress `panicTeardownWatchdog` kills the WHOLE PROCESS. Measured on
the Mac, 3 of 3 runs each, h2c and TLS, the peer disconnecting after the first
DATA frame:

- a handler that does `try zio.sleep(10 s)`, which honours cancellation: panics
- a handler in a shielded sleep: panics
- a handler that swallows `error.Canceled` in a loop: panics
- the same handlers for 3 s: the connection ends cleanly

The first row panics because the HTTP/2 path never cancels a handler task.
`1f84147` (t-1802) removed the reaper cancel, because `JoinHandle.cancel` asks
AND waits, and the reaper hung on uncancelable waits. Read that commit message.

## The hard constraints

These are the only real ones. Everything else in the current teardown is a
choice.

1. zio is cooperative. A task can be asked to cancel (`AnyTask.cancel`:
   setCanceled + wake, returns at once; `zig-pkg/zio-*/src/task.zig`), never
   forced. A handler that never yields keeps running.
2. Memory a running task can reach must not be freed or reused.
3. Per-connection memory is bounded, so a connection's slot must come back.

## Why the pain exists

A handler can reach the whole connection:

- `HandlerCtx.conn` is a `*Connection`; `writeCb`, `sendCb`, `startCb`,
  `flushCb`, `waitUntilCb`, `abortCb` lock `session_mu`, enqueue into
  `self.sched`, and wait on tickets and space events that live in the
  connection.
- Outside those calls the handler still reads memory in the slot:
  `HandlerJob` (with `req`, `resp`, `hctx`) is in `Connection.handler_jobs`,
  boot-allocated from the slot; `HandlerSlot.terminal` is in the slot.
- The actor frees the request arena (`releaseLease`, in `releaseSlot`, after
  the handler's completion).

So by constraint 2 the connection must outlive every handler, and teardown has
to wait. The wait loop, the watchdog panic, the `completion_owner` handshake,
the reaper remnants and the quarantine all exist to manage that wait.

## The design to spike

The handler reaches only a small per-stream object with its own lifetime.
Call it the stream link here; name it as you like.

- **Outside starh2 calls:** everything a handler touches (its job, request,
  response, terminal state, request arena) lives in the stream link or in the
  request arena, not in the slot. The link is freed by whoever drops the last
  reference: the actor or the handler.
- **Inside starh2 calls:** a handler callback enters the connection only through
  the link, and only while the link says the connection is alive. Two shapes:
  - (a) **Gate.** The link holds the connection pointer, a dead flag and a
    count of handler calls inside the connection. Teardown sets dead, wakes
    every starh2 wait, requests cancel (`AnyTask.cancel`, not
    `JoinHandle.cancel`), and waits only for the in-call count to reach zero.
    That wait is bounded by starh2 code, not user code, because starh2's own
    waits already wake on dead (`1f84147` lists them). Then the slot is
    released.
  - (b) **Share-nothing.** The stream's pending bytes, tickets and space events
    live in the link; the actor pulls from links. The handler never enters
    connection memory at all.

  Build (a) first. It is the smaller change and removes the panic. Then report
  what (b) would add and cost, from what you learned; build it only if (a)
  turns out not to work, and say why.
- **Teardown** no longer waits for handler code. A stuck handler holds only its
  link, its request arena and its own stack.
- **RST_STREAM** on a stream whose handler is running also requests cancel.
- **Bounding stuck handlers:** they now outlive connections, so the number of
  live handler tasks must be bounded server-wide, not per connection. Find
  what bounds it today (`live_task_handlers`, the balancer, the request pool's
  caps) and say whether that is enough.
- **Server shutdown and deinit** must not free the request pool or anything a
  live handler can reach. For the spike, `deinit` may wait for live handlers
  with no limit; say so in the report. Do not panic.

Out of scope: HTTP/1.1 handlers (`src/edge/h1.zig`, which still uses the
reaper). Do not change them; note anything you see.

## Acceptance

Branch `carmack/t-3737`, worktree `/Users/rsaunderson/Source/mine/starh2-wt-3737`.
Its first commit adds six tests at the end of `tests/goaway_delivery.zig`,
`outlive: ...` (h2c and TLS, three handler kinds). On master all six crash with
the watchdog panic and the other 44 tests in that binary pass.

1. The six `outlive` tests pass, 3 of 3 runs, Mac. Do not weaken them. If one
   is wrong, stop and report why instead of editing it.
2. `./zb build ci` on the Mac, run in a fresh `--cache-dir`. Known failures on
   master, not yours: the two `macos-libcxx-probe` steps (t-3540), and
   occasional `deadlines T2`/`T3`, `compression I4`, `t3567` idle table. Any
   other failure is yours until shown otherwise; run master in the same way to
   compare.
3. Mutation check: break the new code in at least three ways that matter
   (teardown waits for handlers again; the link is freed while the handler
   holds it; the late call enters the connection after dead) and show which
   tests fail. A mutation no test catches is a finding, not a failure; report it.
4. Bench, Mac, alternating master and the spike, at least 6 pairs each:
   `./zb build bench -Doptimize=fast -- -n 100000 -c 50 -m 10 -t 4 --rounds 3`.
   Report the median pair ratio for TLS and h2c. Do not bench on nachos.

## How to work

- Tripwire before any work: `git -C /Users/rsaunderson/Source/mine/starh2-wt-3737 rev-list --count HEAD..origin/master`
  must be 0. If it is not, STOP and report; do not rebase.
- Commit per milestone on `carmack/t-3737`. Never push. Never touch other
  worktrees or branches.
- Build every test binary with its own `--cache-dir` and check there is exactly
  one binary before you run it. Wrap every build and every binary in a time
  limit (`perl -e 'alarm shift; exec @ARGV' <s> ...`). A time limit on
  `zb build` kills the build, not the test it started: before any bench, check
  `pgrep -fl 'zc-'` and kill leftovers by exact PID.
- Read why a test failed, not only that it failed.
- If the design does not fit somewhere, or you reach a choice this brief does
  not settle, stop and report with the options. Do not choose a different
  design quietly.

## Report

Write `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/t3737-spike-report.md`
(not committed) with:

- what moved where, and what the handler can still reach;
- what was removed (watchdog, reaper remnants, quarantine use, wait loop) and
  what had to stay;
- the acceptance results with commands and counts, and the mutation table;
- the bench table;
- every corner cut, and every open question for the real change;
- your view on (b): what it would add, and whether it is worth it.
