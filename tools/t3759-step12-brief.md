# t-3759 steps 1 and 2: missing deadlines, accept robustness, shutdown contract

You implement the first two steps of the hardening design
`tools/hardening-design.md` (v3, commit 686d695). Read the whole design
first, then `AGENTS.md`, then `tools/t3759-inventory.md` rows 1, 4, 8, 9 and
section 5. The design is settled; this brief only scopes your part. If the
design is wrong or cannot be built as written somewhere in your scope, STOP and
report with the options. Do not choose a different design quietly.

Worktree `/Users/rsaunderson/Source/mine/starh2-wt-3737`, branch
`carmack/t-3737`. Tripwire before any work:
`git -C /Users/rsaunderson/Source/mine/starh2-wt-3737 rev-parse HEAD` must
print the commit that adds this brief (its parent is 686d695). If not, STOP.

## Premises (settled; do not argue them)

The six premises at the top of the design, and Ryan's decisions Q2-Q6 at its
end. No external users: breaking changes are free, no shims. zio primitives
only; no `std.Io` concurrency abstraction (`./zb build std-io-gate`). Read a
dependency's source before you use it (`zig-pkg/<name>/src`). For Zig 0.17
APIs read `~/.claude/skills/zig/SKILL.md`; do not guess.

## Commit 1: missing deadlines and accept robustness (design step 1)

- **Request body clock starts at HEADERS without END_STREAM**, not at the first
  DATA (`session.zig:1674-1678` is the only arm today).
- **Request body minimum rate** after a grace period (Kestrel: 240 B/s after
  5 s; make both `Limits` fields), counting body bytes only (not padding, not
  zero-length DATA), paused only while the server's advertised receive credit
  prevents progress. HTTP/2 and HTTP/1.1. On expiry: H2 RST_STREAM (pick the
  code and say why), H1 close (408 if nothing was sent yet).
- **Response minimum rate**: measured only while bytes for the stream are
  queued anywhere between the scheduler and the socket and the client is not
  taking them; it follows the bytes to completion. Today
  `checkSlowConsumers` skips `pw.len == 0` (`connection.zig:3530`) while bytes
  may still sit in the write queue, and a 1-byte WINDOW_UPDATE counts as
  progress. Replace "any progress" with a rate. An SSE stream that sends one
  small event a minute must never trip it. HTTP/1.1 too, if it has an
  equivalent; say what you find.
- **Accept never exits on an error.** `server.zig:617-623` returns on any
  error but Canceled and ConnectionAborted. EMFILE, ENFILE, ENOBUFS, ENOMEM
  (check how zio maps each, `zig-pkg/zio-*/src/os/net.zig`): count, log at
  most once a minute, back off 10 ms doubling to 1 s, retry. Decide which
  errors are truly fatal for a listener and say why.
- **RLIMIT_NOFILE at boot**: read it, raise the soft limit toward the hard
  limit (clamp: macOS can report RLIM_INFINITY, and `setrlimit` above
  `kern.maxfilesperproc` fails), and refuse to start (`Server.init` error)
  below `max_connections` plus a stated margin.
- Fix the stale comment at `limits.zig:235-237` (the accept loop parks now).

Attack tests, real sockets, h2c and TLS where both apply, HTTP/1.1 where it
applies. Each must FAIL on 686d695 and PASS after, and must show a second,
well-behaved client still served during the attack:

1. Inventory row 1, the timer part: streams opened with HEADERS without
   END_STREAM and then silence are reset by the body deadline. (The shares
   that bound how many come in step 4; here, show they no longer live for
   ever.)
2. Row 8: a body dripped below the minimum rate is ended; an honest slow
   upload above it is not; an upload the server itself throttles is not.
3. Row 9: zero window plus 1-byte WINDOW_UPDATEs on a stream with pending
   bytes is ended by the response rate; a sparse SSE stream is not.
4. Row 4: an injected accept error (a test hook in the accept path is
   acceptable here; say why it is needed) does not end the accept loop, and
   the endpoint accepts again after it clears.
5. The boot check: a `Server.init` with a limit below `max_connections` fails.

## Commit 2: the shutdown contract (design step 2)

Design section "Shutdown (Q5)":

- `serve` never returns while a handler still runs. GOAWAY, drain deadline,
  then request cancel of every handler AND log right then what is still
  running (count per route if the route is available; otherwise count, and
  say so), how long the drain ran, and the drain deadline.
- Grace of 1 s (a `Limits` field), then if handlers remain log again and keep
  waiting, logging at an interval (a `Limits` field). The wait ignores cancel
  and uses zio's `debug_cancel` exemption (zio `task.zig`, `cancel_debug.exempt`;
  read it). It must not spin: `waitOrphanedHandlers` (`server.zig:411-415`)
  is the place; read zio `checkCancel` (`task.zig:503-516`).
- Every wait in `serve` is covered, including `connection_group.await`
  (`server.zig:392-399`), where HTTP/1.1 connections wait today.
- Every check in `Server.deinit` becomes a run-time check that logs, not
  `std.debug.assert`, so a release build cannot free the request pool under a
  live handler. A live orphan at `deinit` is a contract violation by the
  caller (serve has not returned); say what `deinit` does then, and it must
  not free memory a handler can reach.
- Account underflow: wherever a release can take a counter in
  `GlobalAccounting` below zero, refuse the subtraction (saturate at zero),
  count it and log it. List every such site.
- Test harness: because `serve` now waits for handlers, a leaked charge
  becomes a hang. Add a per-test `serve` deadline helper that fails loudly
  with the stuck count, and use it in every test file that starts a `Server`.
  If converting every file is too large, convert the ones that run task
  handlers and list the rest in the report.
- Tests: the cancel-time log fires with the right count; a cooperative handler
  ends within the grace and `serve` returns; a stuck handler keeps `serve`
  waiting and logging (run it in a child process so the suite does not hang;
  kill it after two log intervals and check its output).

## Gates and evidence

- `./zb build ci` on the Mac, fresh `--cache-dir`, after each commit. Known
  failures on master, not yours: the two `macos-libcxx-probe` steps (t-3540),
  and occasional `deadlines T2/T3`, `compression I4`, `t3567` idle table,
  `t3562` half-close (t-3752). Any other failure is yours until shown
  otherwise; run 686d695 the same way to compare.
- Mutation-test each new defence: break the production code (not the test) so
  the defence is gone, show which tests fail, and read why. A mutation no test
  catches is a finding; report it.
- Bench, Mac only, alternating 686d695 and your tip, at least 6 pairs:
  `./zb build bench -Doptimize=fast -- -n 100000 -c 50 -m 10 -t 4 --rounds 3`.
  Read `failed == 0` first. Do not use nachos.
- Build every test binary with its own `--cache-dir` and check there is
  exactly one binary before you run it. Wrap every build and binary in a time
  limit (`perl -e 'alarm shift; exec @ARGV' <s> ...`). A time limit on
  `zb build` kills the build, not the test it started: before any bench,
  `pgrep -fl 'zc-'` and kill leftovers by exact PID.
- Commit per step on `carmack/t-3737`, with a full decision-record message in
  the repo's style (problem, changes, benefits, alternatives rejected, what it
  depends on, statistics). Never push. Never touch other worktrees or
  branches. Do not run anything on nachos.

## Report

Write `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/t3759-step12-report.md`
(not committed): commits; each attack test with its result on 686d695 and on
your tip; ci results; the mutation table; the bench table; every corner cut;
every place the design did not fit; open questions. Label anything not
measured as inference.
