# Brief: t-3763, retire every std.Io concurrency use in starh2

The owner's rule: zio owns concurrency in starh2. The std.Io concurrency types
(`Group`, `Event`, `Queue`, `Mutex`, `Future`, `Select`) are banned by
`./zb build std-io-gate`; the uses below predate the ban and are allowed only
by `tools/std-io-gate/baseline.txt`. Remove all of them, so the baseline is
empty and the gate admits nothing. `std.Io` the vtable (`self.io` for sockets,
files, clocks) stays.

Worktree `/Users/rsaunderson/Source/mine/starh2-wt-3737`, branch
`carmack/t-3737`. Tripwire: HEAD is the commit adding this brief; going down
its first parents you reach `0fdbc59` (t-3759 step 3, endpoint fields) with
only brief commits ("Add the ...") between. If not, STOP. Untracked
`tools/dst-r*/`, `tools/pcap-r1/` and report files are not yours.

## The uses (baseline at 0fdbc59, 26)

server.zig Group 3 + Event 1; http1/oneshot.zig Group 3 + Event 2;
connection.zig Event 2 + Mutex 1 + Queue 2; h1.zig Queue 1; io_queue.zig
Queue 3; ticket_table.zig Event 4; tls.zig Queue 2; wire_pump.zig Queue 2.

Today `server.zig` mixes both APIs on the same groups: `std.Io.Group`
declared (`:363-364`), spawned both by `concurrent(self.io, ...)` and by
`zio.Group.fromStd(...).spawnInto`, cancelled both by
`accept_group.cancel(self.io)` and by `zio.Group.cancel` in `cancelOnSignal`.

## Rules

- **One commit per type**, in this order: Group, Event, Mutex, Queue. Each
  commit ratchets the baseline down (`tools/std-io-gate/gate.sh --update`) and
  is green on its own, so the series bisects. Rebase the brief commits on top,
  unchanged.
- **Read the zio source first** (`zig-pkg/zio-*/src`, the pin at this
  commit) for each type and write down, in the commit message, every semantic
  difference the call site could depend on: cancel behaviour, waiting,
  shielding, wake-ups, capacity and blocking of queues versus channels, what a
  closed queue returns, fairness. If a call site relies on a std.Io behaviour
  zio does not have, STOP and report the shape you would need.
- **Known difference, settled:** `zio.Group.cancel` cancels and then waits
  shielded, with no time limit and no logging (`group.zig`). `serve` must keep
  logging while it waits on stuck handlers (the t-3759 shutdown contract,
  `waitForHandlers`, `cancelOnSignal`). Keep that contract; the step-2
  shutdown tests must pass unchanged.
- No behaviour change a client or a handler can see. No scheduling hooks in
  the actor. No shims or aliases.
- After the last commit the baseline file is empty (or deleted, with the gate
  then rejecting any use), and `./zb build std-io-gate` passes.

## Gates and evidence

- Mac: `./zb build ci` on a `git archive` export with a fresh `--cache-dir`,
  after each commit if it is cheap, at least after the last. Known: the two
  `macos-libcxx-probe` steps (t-3540); occasional `compression I4`,
  `deadlines T2/T3`, the t3567 idle table (fixed on another branch, not here).
- nachos: only in a new directory `~/src/s-3759-t3763` (copy `zig-pkg` and
  `testdata/*.pem` from `~/src/s-3759-spike`; `export
  PATH="$HOME/.local/share/mise/shims:$PATH"`). Full ci at least after the
  last commit. Touch no other directory; kill only your own PIDs.
- The shutdown, goaway_delivery, lifecycle, slot_reuse, accounts and h1
  binaries run directly 5 times per machine after the last commit.
- Time limits on every build and run; fresh `--cache-dir` per binary; never
  `rm -rf` a path built from variables; never write the shared Zig cache by
  hand.
- Bench: Queue and Event sit on hot paths. After the last commit, 6
  alternating pairs of `0fdbc59` against the tip on the Mac (h2c, TLS, h1c,
  `-Doptimize=fast`), `failed == 0` read first, and the same on nachos in
  your directory. Report medians and per-pair ratios.
- Do not tell the orchestrator the tests pass; the orchestrator recomputes.

## Report

`tools/t3763-report.md` (not committed): the commits; per type the semantic
differences found and how each call site was checked; ci on both machines;
the bench tables; every place a zio type did not fit. Stop and report on any
design question.
