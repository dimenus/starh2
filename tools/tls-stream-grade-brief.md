# Brief: grade the TLS-as-stream cut on nachos

You are running a **measurement**, not a build. You change no code. The two
arms are already built into git worktrees on the remote box. Your job is to
run the harness, collect the outputs, and report them verbatim. The owner
recomputes every number from the files you leave on disk, so a claim without
a file path is discarded.

## 0. Tripwire — run this first and STOP on any mismatch

All remote commands run over ssh as `ryan@nachos.trex-elevator.ts.net`.
Always prefix the remote shell with `export PATH=$HOME/.local/bin:$PATH`.

```sh
ssh ryan@nachos.trex-elevator.ts.net 'export PATH=$HOME/.local/bin:$PATH
hostname
git -C ~/src/starh2-t843 rev-parse --short HEAD   # must be f7c41e2
git -C ~/src/starh2-base rev-parse --short HEAD   # must be 55835a4
~/.zvm/bin/zig version                            # must be 0.16.0
h2load --version | head -1
git -C ~/src/starh2-t843 status --porcelain | grep -v "^??" | wc -l   # must be 0
git -C ~/src/starh2-base status --porcelain | grep -v "^??" | wc -l   # must be 0'
```

If any value differs, stop and report the mismatch. Do not fetch, reset,
rebase, or "fix" a worktree. Untracked entries (`testdata/`, `vendor/`) are
expected; a modified tracked file is not.

## 1. Premises — settled, do not argue or work around

- **You change no file**, on the laptop or on nachos. Not in the worktrees,
  not in the harness, not in a test. If something fails, the failure is the
  finding. Paste it and stop that arm.
- **Do not touch `~/src/starh2`** (the main checkout). It holds another
  session's dirty tree.
- The recipe values are fixed. Do not change STREAMS, INTERVAL, SECONDS_RUN,
  WARMUP, ONESHOT_WORKERS, STARH2_EXECUTORS, or ROUNDS to make a number move.
- Numbers from this machine are only compared to numbers from this machine.
  The Darwin tables in the git log do not transfer.
- You do not decide the next phase. You report.

## 2. The runs, exactly as written

Before every arm run, on nachos:

```sh
pkill -f starh2-bench-server; pkill -f sse-server; sleep 1
pgrep -fl 'starh2-bench-server|sse-server'   # must print nothing
uptime                                        # 1-min load must be < 2.0
```

The harness uses fixed ports 19450/19451, so a leftover server answers for
the wrong arm. The `pgrep` output goes in your report for every run.

Arm order is A-B-A so drift is visible: **t843, base, t843 again.**

```sh
mkdir -p /tmp/t843-grade   # on nachos
# run 1 (and run 3, with mixed-t843-2):
ssh ryan@nachos.trex-elevator.ts.net 'export PATH=$HOME/.local/bin:$PATH
hostname; uptime
cd ~/src/starh2-t843 && STREAMS=32 INTERVAL=10 SECONDS_RUN=5 WARMUP=1 \
  ONESHOT_WORKERS=8 STARH2_EXECUTORS=2 ROUNDS=3 OUT=/tmp/t843-grade/mixed-t843-1 \
  tools/sse_bench/mixed.sh 2>&1 | tee /tmp/t843-grade/mixed-t843-1.log'
# run 2:
ssh ryan@nachos.trex-elevator.ts.net 'export PATH=$HOME/.local/bin:$PATH
hostname; uptime
cd ~/src/starh2-base && STREAMS=32 INTERVAL=10 SECONDS_RUN=5 WARMUP=1 \
  ONESHOT_WORKERS=8 STARH2_EXECUTORS=2 ROUNDS=3 OUT=/tmp/t843-grade/mixed-base-1 \
  tools/sse_bench/mixed.sh 2>&1 | tee /tmp/t843-grade/mixed-base-1.log'
```

The first line of every log must be `nachos`. A log whose first line is not
`nachos` is a run on the wrong machine and is discarded.

Then the packing oracle, t843 arm only:

```sh
ssh ryan@nachos.trex-elevator.ts.net 'export PATH=$HOME/.local/bin:$PATH
hostname
cd ~/src/starh2-t843 && tools/oneshot-phase-trace.sh 2>&1 | tee /tmp/t843-grade/phase-trace.log
echo PHASE_TRACE_EXIT=$?'
```

The exit code line is part of the report. Exit 9 means the packing gate
failed; that is a finding, not a thing to fix.

If a `mixed.sh` internal build fails (each invocation builds its own arm),
paste the error and stop that arm. Do not install a dependency, do not edit
`build.zig`, do not copy files between worktrees.

## 3. Report format

Every claim cites a `/tmp/t843-grade/...` path on nachos.

1. The tripwire output, verbatim.
2. For each of the three mixed runs: the `pgrep` check, the `uptime` line,
   and every per-round result line from the log, verbatim. No averages
   without the per-round lines beside them.
3. The table: arm x {oneshot-only rps/p50, mixed oneshot rps/p50, mixed SSE
   events, stall delivering/oneshot}. One row per run, not per arm.
4. `sha256sum` of `/tmp/t843-grade/mixed-*/starh2/bin/starh2-bench-server`
   for every run directory.
5. The phase-trace exit line and the `records/response` line.
6. `git status --porcelain | grep -v "^??"` for both worktrees, after the
   runs. Must still be empty.
7. Anything you could not run, and why. An honest gap beats a filled cell.

## 4. What the numbers decide (for context; the owner decides)

- t-842 hard lines, on the t843 arm: mixed SSE at the full offered count
  every round (32 streams x 10 ms x 5 s = 16000; report the exact count, do
  not round), and stall delivering 31/32.
- The BIO question: does t843 mixed-oneshot sit in the base arm's band, or
  clearly below it? The old blocking-BIO cut collapsed mixed oneshot; the
  non-blocking BIO is the arm you are grading.
- The Go gap on this machine is recorded for t-843 (duplex TLS ownership),
  which is a later decision, not yours.
