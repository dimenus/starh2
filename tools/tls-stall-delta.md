# TLS stall delta-debug (local)

The 30s `stall` class (started == done == total, a handful of failed
requests) landed in `57359b7`. This file is the harness protocol. Do not
fold `not-started` into that result.

`not-started` (`started < total`) was the shared `non_data` HEADERS budget
(10k burst, 1k/s). A legitimate one-shot connection GOAWAY'd
`ENHANCE_YOUR_CALM` after ~10k requests. HEADERS is exempt, like DATA;
rapid-reset still hits `rst` + `non_data`. Do not put HEADERS back on
`non_data`. t-761.

The harness is `tools/tls-stall-delta.sh`. Read its header comment for the
protocol and for the reason behind each guard. It does not change production
code.

## The two classes, and how the harness tells them apart

The harness classifies each round by h2load's request accounting, never by
duration.

- **`stall`** — `started == total`, `done == total`, `failed == errored > 0`,
  `timeout == 0`. Every request reached the server. A few never completed on
  time. Captured at 30.14s, 64.74s and 137.47s with an identical `requests:`
  line, so a duration threshold is the wrong test. The loss was 26 requests in
  five captures and 30 in one.
- **`not-started`** — `started < total`. The client never submitted the rest.
  This is the `-c 10` family and the large `-n` family. It is a different
  defect. It is fast and it loses thousands of requests.
- **`other`** — any remaining shortfall.
- **`slow`** is an orthogonal flag, so a round that both parks and loses
  unstarted requests reports both facts.

The classifier was validated against ten captured rounds and reproduced their
known labels.

## What the socket sample shows at a stall

Every stalled round sampled so far shows exactly one connection still
established, the same one at t=5s, t=15s and t=25s, with `Recv-Q` and `Send-Q`
at zero on both the server row and the client row. No kernel data waits in
either direction, so the unconsumed work is inside the Starh2 process.

`netstat` sees kernel socket buffers only. It cannot see the TLS receive
accumulator or any channel, which is why the in-process diagnostic pass is
still needed.

## What the thread sample shows at a stall

`tools/stall-thread-sample.sh` catches a stall and samples every thread of the
live server and of h2load. Two captures, both with the exact 26-failure
signature, gave the same result: every executor thread sat in `kevent` for
100% of a 3-second window, and no starh2 frame appeared on any stack.

That refutes lost socket readiness, lock contention on `session_mu`, and a
spin, all in one measurement. Nothing in the server is runnable, so the stall
is a lost wakeup or work stranded behind a parked consumer.

A suspended coroutine sits on no thread stack, so the sample names the runtime
state and never the await point. That still needs the in-process counters.

## Re-derive every count; do not cite the ones below

The counts here came from the harness BEFORE it was hardened. That version used
a fixed port 19443, recorded no binary identity, and had no freshness check.
Any of those can attribute a round to the wrong build. Treat the counts as
directional only, and re-derive them with the current harness in compare mode.

Directional observations, in that light:

- TLS, `--task-migration`, auto executors, `-n 20000 -c 50 -m 10 -t 4` — the
  smallest genuine failing case by request count.
- TLS, `--task-migration --executors 2`, `-n 100000 -c 50 -m 10 -t 4` — a
  higher rate, and the useful case for repeated runs.
- h2c with migration has not produced the stall.
- TLS with migration off has produced the stall at one executor, so migration
  is not necessary for it.
- `-c 10` produces `not-started` with migration on and off.
- `-n 1000000 -c 50 -m 10 -t 4` with migration off produced `not-started` in 3
  of 3 rounds. This is the cheapest deterministic reproducer of that class.
- The plaintext zio-only three-task example under `tools/zio-migration-repro/`
  stays green under both scheduler settings.

## Accelerator

`-N 2` preserves the migration-on failure at auto executor width and finishes
faster. Do not use it with one executor: it creates overload failures in the
migration-off control. The unaccelerated signature is authoritative.
