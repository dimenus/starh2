# Brief: instrument the TlsPump quiet turn, then recover oneshot-only

Two phases, in order. Phase 1 is an instrument. Phase 2 is a fix that the
instrument's numbers must justify. A fix without the phase-1 numbers is
rejected whatever it measures, because the suspects below are inferences.

## 0. Tripwire — run first, STOP on any mismatch

Local (your worktree): `git rev-parse --short HEAD` must match the SHA in
your prompt. Do not fetch, pull, or rebase.

Remote (`ssh ryan@nachos.trex-elevator.ts.net`, always prefix remote shells
with `export PATH=$HOME/.local/bin:$PATH`):

```sh
git -C ~/src/starh2-t843 rev-parse --short HEAD   # must be f7c41e2
git -C ~/src/starh2-t844 rev-parse --short HEAD   # must be f7c41e2 (yours to move)
~/.zvm/bin/zig version                            # must be 0.16.0
h2load --version | head -1
```

`~/src/starh2-t843` is the CONTROL arm. You never modify it. `~/src/starh2`
(the main checkout) is another session's tree; never touch it.

## 1. The graded facts you are fixing (nachos, 2026-08-19, /tmp/t843-grade)

Arms f7c41e2 (TLS-as-stream, non-blocking BIO) vs 55835a4 (old tls.zig
records), A-B-A, mixed recipe, all verified against the logs:

| axis | 55835a4 base | f7c41e2 |
|---|---|---|
| oneshot-only rps / p50 | 153.6-167.1k / 44-51 us | 88.6-111.2k / 70-86 us |
| mixed oneshot rps / p50 | 102.2-110.3k / 71-74 us | 115.6-119.5k / 63-65 us |
| mixed SSE events | 16000+ every round | 16000+ every round |
| stall | 31/32, ~110k | 31/32, ~114-117k |
| server CPU (mixed.sh line) | 9-10 s | 11-12 s |

Two shapes in that table drive this brief:

- f7c41e2 WINS mixed oneshot and LOSES oneshot-only by 30-45%.
- On f7c41e2 only, mixed oneshot BEATS oneshot-only in every round. That
  inversion says the pump pays something exactly when traffic does not keep
  the write queue non-empty.

Suspects, in `src/edge/tls.zig` `Pump.run` — inferences until phase 1 says
otherwise:

- a two-arm `std.Io.Select` (write queue vs socket peek) built and torn down
  per quiet turn;
- `sleep(.zero)` yields in `readOne` (read_free empty; live task handlers);
- the `pending_read` retry loop.

## 2. Premises — settled

- The stack is malleable, no external consumers. Breaking an internal API is
  fine; name it in the commit.
- `TlsPump` stays the sole SSL_read/SSL_write owner. No mutex on the SSL
  object. No second task on it.
- Keep the non-blocking BIO. Keep packing in `emit_batch`. Keep `session_mu`
  covering Session + FairScheduler only. Do not change zio.
- No wall-clock sleep, timer, or poll in production paths. `sleep(.zero)`
  yields are the existing idiom and stay legal.
- Do not put HEADERS on `non_data`. Do not regenerate any golden. Do not
  weaken or delete a test assertion.
- The bench recipe values are fixed. A number bought by changing STREAMS,
  INTERVAL, ROUNDS, workers, or executors is discarded.
- Do not touch the task store; the driving session holds the claim (t-844).

## 3. Phase 1 — the instrument

Add quiet-turn counters to `Pump`, following the repo's existing pattern:
gate on `@import("build_options").observe` exactly like the
`test_observed_*` counters in `connection.zig` (`-Dobserve=true` puts them
into ReleaseFast; Debug has them on; plain ReleaseFast compiles them out —
see `attachStarh2Options` in `build.zig` and commit `cdaeaab`).

Count at least, per pump: turns, Select constructions, Select write-wins,
Select peek-wins, `tryGet` write hits (write served with no Select),
`readOne` calls, `WantRead` returns, `read_free`-empty yields,
live-handler yields, `pending_read` retries, writeChunks calls, chunks per
write batch. Expose them the same way existing counters reach `--trace`
output (find how the bench server prints trace counters and extend that).

Then measure on nachos, ReleaseFast + `-Dobserve=true`, in
`~/src/starh2-t844` after you push (see section 6): the mixed recipe's
oneshot-only phase and mixed phase (`TRACE=1` if that is the switch the
harness uses; read `tools/sse_bench/mixed.sh`). Report counters per phase,
normalized per request. Name the dominant quiet-turn cost with its number.

**If the counters do not name any suspect** (Selects/turn is low in the
oneshot-only shape, yields are rare), STOP after phase 1 and report. Do not
fix blind. That result would redirect t-843/t-844 and is worth more than a
guessed patch.

Mutation-prove the instrument once: force one counted path in a unit test
(or a deliberate local mutation you revert) and show the counter moves.
An instrument that has never fired is untested.

## 4. Phase 2 — the fix

Guided by phase 1's numbers. Shapes that fit the grain, if the numbers point
at them:

- Serve the write queue with bounded `tryGet` attempts before constructing a
  Select; only Select when both sides were empty.
- Reuse one Select across turns if `std.Io` allows re-arming; do not build a
  parallel wait mechanism beside `std.Io` to avoid it.
- Reorder the turn so a buffered ciphertext read is attempted before the
  Select, if the counters show reads arriving while the pump parks.

Refused, whatever the numbers say:

- A busy loop. The server-CPU line of `mixed.sh` is graded (section 5).
- Starving reads to serve writes (the stall and SSE lines are graded).
- A wall-clock backoff.
- Moving SSL calls onto a second task or the actor.

Commit per milestone with the repo's commit format. A killed run resumes
from the last commit.

## 5. Grading — all axes at once, A-B-A on nachos

Build both arms' benches from their own worktrees. Fix arm =
`~/src/starh2-t844` at your pushed HEAD. Control = `~/src/starh2-t843`
(f7c41e2). Order: fix, control, fix. Before each run:

```sh
pkill -f starh2-bench-server; pkill -f sse-server; sleep 1
pgrep -fl 'starh2-bench-server|sse-server'   # must print nothing
uptime                                        # 1-min load < 2.0
```

```sh
ssh ryan@nachos.trex-elevator.ts.net 'export PATH=$HOME/.local/bin:$PATH
{ uname -n; uptime
  cd ~/src/starh2-t844 && STREAMS=32 INTERVAL=10 SECONDS_RUN=5 WARMUP=1 \
    ONESHOT_WORKERS=8 STARH2_EXECUTORS=2 ROUNDS=3 OUT=/tmp/t844-grade/mixed-fix-1 \
    tools/sse_bench/mixed.sh; } 2>&1 | tee /tmp/t844-grade/mixed-fix-1.log'
```

(control run: same shape, `~/src/starh2-t843`, OUT/log named mixed-ctrl-1;
third run: mixed-fix-2. `mkdir -p /tmp/t844-grade` first. The first line of
every log must be `nachos`.)

Hard lines, every one required; a build that trades one for another fails:

1. Mixed SSE events >= 16000 in every fix-arm round. Report exact counts.
2. Stall delivering 31/32 in every fix-arm run.
3. Mixed oneshot: every fix round within 5% of the same-session control
   band, or above it.
4. Oneshot-only: every fix round beats every same-session control round.
   Success target: fix median >= 130k rps (recovers more than half the
   measured gap toward 55835a4's 154-167k). If the fix lands lower, report
   the number honestly and say what the instrument says is left.
5. Server CPU (the `mixed.sh` printed line) within +20% of the control arm.
6. Packing oracle in the fix worktree, redirect not pipe (a pipe reports
   tee's exit): run `tools/oneshot-phase-trace.sh`, log to
   `/tmp/t844-grade/phase-trace.log`, echo `PHASE_TRACE_EXIT=$?` directly
   after. Must be 0.
7. `./zb build ci` passes in your LOCAL worktree (plain, no -Dobserve).
8. Plain ReleaseFast carries no instrument cost:
   `strings <plain-releasefast-bench-server> | grep -c STARH2_PUMPTRACE`
   must print 0, and the `-Dobserve=true` build must print > 0. Put the
   literal `STARH2_PUMPTRACE` marker in the counter-print path so this
   check has something to find.

## 6. Moving code to nachos (exact commands)

Your worktree is on the laptop. nachos benches run from
`~/src/starh2-t844`, which is a DETACHED worktree. After each milestone
commit:

```sh
git push ryan@nachos.trex-elevator.ts.net:src/starh2 +HEAD:refs/heads/t844-fix
ssh ryan@nachos.trex-elevator.ts.net 'git -C ~/src/starh2-t844 checkout --detach t844-fix && git -C ~/src/starh2-t844 rev-parse --short HEAD'
```

The printed SHA must equal your local HEAD. A bench run whose remote SHA you
did not confirm this way is discarded. `vendor/boringssl` and `testdata/`
in that worktree are untracked and already set up; do not delete them.

## 7. Report

1. Tripwire output, verbatim.
2. Phase-1 counter table (oneshot-only vs mixed, per request), and one
   sentence naming the dominant cost.
3. The mutation proof of the instrument.
4. The diff summary of the fix and the milestone commit SHAs.
5. Per-round result lines of all three grading runs, verbatim, with
   `/tmp/t844-grade/...` paths, pgrep checks, uptime lines.
6. The eight hard-line results, one per line, each with its evidence path.
7. `git status --porcelain | grep -v "^??"` for both nachos worktrees after
   the runs (control must be untouched).
8. What you could not do, and why. An honest gap beats a filled cell.
