# t-3759: HTTP/1.1 onto links and the request pool; the reaper deleted

You implement design step 7 of `tools/hardening-design.md` (v3), moved forward
by Ryan's decision so it lands directly after steps 1 and 2. Read, in order:
`AGENTS.md`; the design, above all "The rule everything follows", "Stuck
handlers", "Shutdown (Q5)" and "Order of work"; the full commit messages of
3fca452 (step 1) and 6ea2fcf (step 2), which hold the decision record and the
review rounds; the spike commit 839dd69 (`StreamLink`, `gate.zig`,
`orphanSlot`). The design is settled. If it cannot be built as written in your
scope, STOP and report with the options. Do not choose a different design
quietly.

Worktree `/Users/rsaunderson/Source/mine/starh2-wt-3737`, branch
`carmack/t-3737`. Tripwire before any work:
`git -C /Users/rsaunderson/Source/mine/starh2-wt-3737 rev-parse HEAD` must print
the commit that adds this brief. Its first parents, going down, are four review
briefs (f7c3e71, 3265276, fc8b4e6, 173d6a4 or their successors), then 6ea2fcf
(step 2) and 3fca452 (step 1). If not, STOP.

## Premises (settled; do not argue them)

- The design's premises and Ryan's Q1-Q6. No external users: breaking changes
  are free, no shims. zio primitives only (`./zb build std-io-gate`). Read a
  dependency's source before you use it (`zig-pkg/<name>/src`); for Zig 0.17
  read `~/.claude/skills/zig/SKILL.md`.
- zio is cooperative. A handler that ignores cancel cannot be stopped. A
  connection must never wait for such a handler, and `serve` must never return
  while one still runs (Q5), logging what it waits on.
- **Ryan's lifetime rule: minimise borrowing; scoped bulk lifetimes are the
  norm.** A handler's world is its own per-request arena. Nothing a handler
  holds may point into connection-owned memory, except through the gate while
  the gate is open. Every pointer from one scope into another that you keep or
  add must be named in the commit message with the reason it cannot be avoided.
- Accepted costs already decided: the write-byte timeout (60 s, nginx
  `send_timeout`), the body minimum rate, the 408 on a slow body. Keep every
  step-1 and step-2 defence working on HTTP/1.1.

## The change

1. **An HTTP/1.1 handler reaches only its link.** At dispatch, the request
   (head, headers, body, trailers) is copied into a request arena from the
   request pool (t-3537), and the link is the first allocation in that arena,
   exactly as HTTP/2 does today. The connection's head and body buffers are
   never handed to a handler. Response calls enter the connection through the
   gate; after teardown they fail with the cause, like HTTP/2.
2. **One link model, not two.** Share the HTTP/2 link, gate, orphan and
   accounting code with HTTP/1.1. Two implementations of one contract drift.
   If a shared type cannot serve both without a flag-ridden union, STOP and
   report the shape you would need.
3. **Teardown does not wait for the handler.** An HTTP/1.1 connection closes
   its gate, requests cancel, waits only for calls inside the gate, orphans the
   link and returns. The orphan count and the task-handler token keep `serve`
   waiting and reporting, as for HTTP/2.
4. **The reaper is deleted**, with its `JoinHandle.cancel` wait, its limits
   fields, its accounting, and the step-2 HTTP/1.1 exit join (`joinHandler`,
   `reaper_has_job`, `cancelJoin`), which the link makes unnecessary. If
   anything still needs the reaper, STOP and report what.
5. **Behaviour an HTTP/1.1 client sees does not change**: keep-alive,
   pipelined heads, `Expect: 100-continue`, chunked and close-delimited
   framing, the 408 and 503 answers, TLS. Say in the report how you checked
   each one.

## Tests

- **Attack test, must FAIL on 6ea2fcf and PASS after:** the HTTP/1.1 form of
  the `outlive:` tests at the end of `tests/goaway_delivery.zig` (h1c and
  HTTP/1.1 over TLS; handlers that honour cancel, are shielded, and ignore
  cancel; hold 6 s). At shutdown or on the peer closing, the connection and its
  slot must be gone within 3 s while the handler still runs; the handler's late
  write must fail with the cause; `serve` must not return until the handler
  ends. A second, well-behaved client must be served throughout.
- Keep the step-2 shutdown and startup-failure tests for HTTP/1.1 (h1c and
  TLS, cancelled `serve` and startup failure). Change only what the deletion of
  the join forces, and say what.
- A test that a handler cannot reach connection memory after teardown: run it
  under the request pool's poisoning (or add poisoning) so a stale borrow would
  read poison.
- Mutation-test every new defence (break the code, not the test) and read why
  each mutant fails.

## Bench

There is no HTTP/1.1 bench arm. This step changes the HTTP/1.1 hot path (a
copy into the arena per request), so add a minimal h1c keep-alive GET arm to
the bench (`./zb build bench`), then run at least 6 alternating pairs of
6ea2fcf against your commit on the Mac (`-Doptimize=fast`, the same flags as
the h2 arms). Read failures first. If the copy costs more than you can separate
from noise, say so with the numbers. The per-write timer cost is t-3773; report
what the new arm shows about it, but do not fix it here.

## Gates and evidence

- Mac: `./zb build ci` with a fresh `--cache-dir` on a `git archive` export.
  Known failures: the two `macos-libcxx-probe` steps (t-3540), and occasional
  `deadlines T2/T3`, `compression I4`, `t3562` half-close. The `t3567` idle
  table is under investigation right now as a possible regression of step 1;
  report every failure of it with its output.
- nachos (Linux): you may use `ssh nachos` ONLY in a new directory
  `~/src/s-3759-h1impl` (copy `zig-pkg` and `testdata/*.pem` from
  `~/src/s-3759-spike`; put the source in with `git archive <sha> | ssh nachos
  'tar -x -C ~/src/s-3759-h1impl'`; `export
  PATH="$HOME/.local/share/mise/shims:$PATH"` first). Touch no other directory,
  run no bench there, change no system setting, kill what you start by exact
  PID.
- Build every test binary with its own `--cache-dir` and check there is
  exactly one binary before running it. Run the new and changed test binaries
  directly at least 5 times on the Mac and on nachos. Wrap every build and run
  in a time limit (`perl -e 'alarm shift; exec @ARGV' <s> ...` on the Mac,
  `timeout` on nachos). Never block a zio executor thread with a blocking
  `Thread.join` or a blocking socket call in a test client: run clients off the
  executor and poll.
- Cross-compile every test binary for `x86_64-linux-gnu` from the Mac before
  you push anything to nachos.
- One commit, on top of 6ea2fcf and below the review briefs (rebase the briefs
  onto it). Full decision-record message in the repo's style: problem, changes,
  benefits, alternatives rejected, what it depends on, statistics, and the
  named cross-scope pointers. Never push. Never touch other worktrees or
  branches.

## Report

Write `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/t3759-h1links-report.md`
(not committed): the commit; each test with its result on 6ea2fcf and on your
commit; ci on both machines; the mutation table; the bench table; how each
client-visible behaviour was checked; every corner cut; every place the design
did not fit; open questions. Label anything not measured as inference.
