# Brief: t-3826 (close the std-io gate's holes) and t-3854 (did starh2 count the swallowed accept EINVAL?)

Worktree `/Users/rsaunderson/Source/mine/starh2-wt-3826`, branch
`carmack/t-3826`, based on `26ea1d9` (the pushed t-3759 series tip). Tripwire:
HEAD is the commit adding this brief, first parent `26ea1d9`. If not, STOP.
Never touch other worktrees or branches; never push.

## t-3826: the gate

The rule: zio owns concurrency in starh2; the `std.Io` concurrency types
(`Event`, `Queue`, `Group`, `Future`, `Select`, `Mutex`, `Condition`, and any
others of that kind) are banned; `std.Io` the vtable (`self.io` for sockets,
files, clocks) stays. `./zb build std-io-gate` enforces it with a text scan
(`tools/std-io-gate/`) and an empty baseline under `src/`.

Measured holes (task t-3826): an alias passes (`const Io = std.Io; var x:
Io.Event = .unset;` counts 0); `io.concurrent` / `io.async` / `io.select`
(which use `Future` / `Select` without naming them) are invisible; only `src/`
is scanned, and `tests/{http1,backend_parity,h1_battery}.zig`,
`examples/bench_server.zig` and `tools/{h1_smoke,h1_go_smoke,tls_smoke,pipeline_bench}.zig`
still use std.Io concurrency.

Decided (orchestrator): close (a) and (b), and convert the remaining uses.
- Scan `src/`, `tests/`, `examples/` and `tools/` (exclude `zig-pkg/` and any
  vendored tree).
- Catch an alias: any identifier bound to `std.Io` (direct or through another
  alias), then `<alias>.<Banned>`. Catch `io.async`, `io.concurrent`,
  `io.select` and their `Io.`-qualified forms, whatever the receiver is
  called. Say what you cannot catch with a text scan, and fail toward noise
  (a false positive the author can rewrite) rather than silence.
- Convert every remaining use outside `src/` to the zio type or call, one
  commit per area if it keeps commits readable. The baseline ends empty, or
  the baseline mechanism is deleted if nothing needs it.
- Mutation-test the gate: plant each hole you closed (an alias, `io.async`, a
  banned type in `tests/`) in a scratch export and show the gate fails on each,
  then show the clean tree passes. Also plant one legal vtable use
  (`self.io` sockets / clock) and show it passes.
- If a type-level (comptime) ban turns out to be clean and robust in Zig 0.17,
  you may propose it in the report; do not build it without asking.

## t-3854: the accept EINVAL

On zio before lalinsky/zio#870 (our pin is before it), a reset-before-accept
connection makes zio's accept call `setNoDelay`, which fails with EINVAL on
macOS; zio prints `unexpected error: .INVAL` in Debug and swallows the error,
and `accept` still returns the socket (measured, task note). t-3810 claimed
starh2's accept loop counted an accept error and backed off 10 ms. Check, by
running, on the Mac with the series' pinned zio: does starh2 count an accept
error, back off, or neither, for a client that resets right after connect?
Read `server.zig`'s accept loop and its counters, then drive it (a real-socket
test or a scratch program; SO_LINGER 0 client). Report what happens. If
starh2 does count or back off on that path, decide whether that is wrong
(a reset client is not a server accept failure) and fix it with a failing test
first; if it does neither, say so and close the question.

## Gates

Mac and nachos `./zb build ci` once each on a `git archive` export at the end
(nachos only in a new `~/src/s-3759-t3826`). Time limits on every build and
run; fresh `--cache-dir`; never `rm -rf` a path built from variables (a hook
now refuses it); never write the shared Zig cache by hand. Do not tell the
orchestrator anything passes; the orchestrator recomputes.

## Commits and report

Commits with full decision-record messages. Never push. Report to
`tools/t3826-report.md` (not committed). Stop and report on any design
question.
