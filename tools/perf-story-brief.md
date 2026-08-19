# Brief: the perf story on both OSes (t-845)

You are building a characterization, not hunting a chip. The deliverable is
(1) a runnable matrix script, (2) the matrix itself, run fresh on both
machines, and (3) an analysis of three named anomalies. Every cell cites a
log written by this session; a cell filled from the git log or from memory
is a fail.

## 0. Tripwire — run first, STOP on any mismatch

Local: `git rev-parse --short HEAD` must match the SHA in your prompt.

Remote (`ssh ryan@nachos.trex-elevator.ts.net`, prefix every remote shell
with `export PATH=$HOME/.local/bin:$PATH`):

```sh
git -C ~/src/starh2-base rev-parse --short HEAD   # must be 55835a4
git -C ~/src/starh2-t844 rev-parse --short HEAD   # must be be0dd60
~/.zvm/bin/zig version                            # must be 0.16.0
h2load --version | head -1
poop true true >/dev/null && echo poop-ok         # installed, verified
ls ~/src/oss/http2-zig-hendrik/boringssl >/dev/null && echo hendrik-ok
```

## 1. Premises — settled

- Arms: `55835a4` (old tls.zig records) and `be0dd60` (TLS stream + quiet
  turn fill). Opponents: http2.zig (packed oneshot only; it has no
  streaming handler) and Go net/http (the Datastar axes).
- The bespoke harnesses stay authoritative for SSE cadence, mixed, stall,
  and packing. You may ADD tooling; you may not modify an existing harness
  script or a test.
- Numbers from one machine compare only to numbers from the same machine.
- Do not touch `~/src/starh2` on nachos, `~/src/starh2-t843`, the task
  store, or anything about t-850 (a known deadline-heap flake under
  investigation; if a LOCAL full-suite run wedges at ~0 CPU, kill that test
  process, note it in the report as a t-850 sighting, and continue).
- The recipe values of existing harnesses are fixed.
- `pkill -f` over ssh matches the remote shell's own argv and kills your
  session. Use a bracket pattern: `pkill -f "starh2-bench-serve[r]"`.

## 2. Phase 1 — tooling

1. `--self-drive-oneshots N` on `starh2-bench-server`: an in-process
   loopback client over a REAL TCP connection to its own listener (no
   in-process shortcut past the socket), drive N oneshot requests on one
   connection, clean exit 0. This makes the server a whole program that
   hyperfine and poop can measure. `./zb build ci` must still pass with the
   flag present (plain build).
2. `tools/perf-story.sh`: runs the whole matrix below on the current
   machine, emits one table plus raw logs under `/tmp/perf-story/`, and
   prints per row: binary sha256, uname -n (inside the logged stream), the
   uptime load line, and the exact command. It must FAIL loudly on zero
   rows (a scan that finds nothing must not print a clean table).
3. Whole-program cells, exact shapes:
   - hyperfine (both OSes): `hyperfine --warmup 1 'h2load <args> <url>'`
     against a long-running server per arm — the measured binary is h2load,
     identical across arms, so wall time is the throughput oracle.
   - hyperfine + poop (Linux): the `--self-drive-oneshots` binary per arm
     that carries the flag (be0dd60 only; 55835a4 is excluded from this
     view, not backported).
   - poop compares arms in one invocation where both commands exist.
4. Peak RSS at 200 SSE streams: server under `/usr/bin/time -l` (Darwin) /
   `/usr/bin/time -v` (Linux), driven by `tools/sse_bench/run.sh` for its
   normal window, SIGTERM, read the peak line. Both arms, both OSes.

## 3. Phase 2 — the matrix

Rows = arm x workload, per machine:

1. Official packed 100k TLS + h2c (`./zb build bench` recipe in AGENTS.md),
   plus http2.zig via `tools/bench-hendrik.sh` where it runs.
2. oneshot-only / mixed / stall (`tools/sse_bench/mixed.sh`, vs Go).
3. SSE 200-stream (`tools/sse_bench/run.sh`, vs Go).
4. Isolated `bench-pipeline` under hyperfine (both OSes) and poop (Linux).
5. Peak RSS at 200 streams (both arms).
6. Server CPU/req (the bench user/sys print).
7. The whole-program cells from phase 1.

Run order per machine: build everything first, then measure with nothing
else running. On nachos, check `pgrep -fl "starh2-bench-serve[r]|sse-serve[r]"`
prints nothing and the load line before every row. On Darwin the machine is
shared: still record the load line in every log; if the 1-min load exceeds
2.0, mark the row BLOCKED-LOADED and move on — a loaded Darwin cell is
worse than an empty one, and the owner will re-run those rows in a quiet
window with your script.

## 4. Phase 3 — the three anomalies (analysis, not fixes)

1. Go beats starh2 p50 on Darwin and loses everything on Linux, same code.
   Say which layer the Darwin gap lives in, with a measurement that
   separates zio's kqueue path from starh2's own work (the isolated
   pipeline rows and the whole-program rows bracket it).
2. Bimodal oneshot-only rounds on Linux (~95k vs ~125k, both arms,
   STARH2_EXECUTORS=2). Re-run the `-Dobserve=true` counter table on
   be0dd60 for a slow round and a fast round and say what differs.
3. The Darwin baseline is dirty: every earlier Darwin number came from a
   loaded laptop. Your (quiet or BLOCKED-marked) Darwin column replaces it.

Change no production code in phase 3. If an anomaly needs a code change to
explain, write the hypothesis and the experiment it needs; do not run it.

## 5. Report

1. Tripwire output, verbatim.
2. The matrix, one table per machine, every cell citing its log path
   (`/tmp/perf-story/...` on the machine that ran it) and binary sha256.
   BLOCKED-LOADED cells named as such.
3. The three anomaly write-ups, each citing the rows it rests on, each
   claim marked measured or inference.
4. Milestone commit SHAs (tooling commits only).
5. `git status --porcelain | grep -v "^??"` on both nachos worktrees after
   the runs; the control trees must be untouched.
6. What you could not run, and why. An honest gap beats a filled cell.

## 6. Moving code to nachos

Same mechanism as t-844, and confirm the SHA before trusting any run:

```sh
git push ryan@nachos.trex-elevator.ts.net:src/starh2 +HEAD:refs/heads/t845-story
ssh ryan@nachos.trex-elevator.ts.net 'git -C ~/src/starh2-t844 checkout --detach t845-story && git -C ~/src/starh2-t844 rev-parse --short HEAD'
```

`vendor/boringssl` and `testdata/` in the nachos worktrees are untracked
and already set up; do not delete them. The 55835a4 arm builds from
`~/src/starh2-base` unchanged.
