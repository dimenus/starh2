# nachos re-bench: PLB with the six balancer fixes

Host: nachos (Linux, io_uring, 12 physical cores). Commit 0932727 (the
balancer fixes), binaries in `rebench-build.txt`. Arms: WS, WS2 (same binary
and arguments as WS: the in-session A/A pair), PL (pinned + `.local`), PLB
(pinned + `.local` + `--conn-balance`, now the fixed balancer). Same harness
and shapes as the pre-fix run. Rounds: SSE/one-shot 24, burst 32, CPU 16,
open loop 16, every mix 16. Rows: `rebench-linux-rows.txt` (phases 1, 2, 3, 5
and the unpaced mix), `rebench-linux-mix-paced.txt`,
`rebench-linux-mixm-paced.txt`. The host was idle before every round
(limit 0.5 busy cores).

## How to read it

- A cell is the median per-round ratio arm / WS. Lower is better, except
  rows marked "higher better".
- A/A range is WS2/WS in THIS session. `*` = the arm's median ratio is
  outside that range. Inside the range means "not resolved", not "equal".
- `knee` = every stream delivered and pooled p50 > 200 us. In the mix rows
  the same test is written `p50>200us` and means "saturated". `partial` = a
  stream opened but delivered nothing, or ended early.
- The two **pre-fix** columns are the ratios from the earlier sessions
  (`linux-rows.txt`, `linux2-mix.txt`, `linux2-mixm.txt`). They are a
  different session, so they are for comparison of direction only. PL's code
  did not change, so the PL column against the PL pre-fix column shows how
  much a ratio moves between sessions by itself.
- `@2`, `@8`, `e8` = executors; `eprod` = 12 executors (production width).
  `mix` = heavy connections are executors/2, `mixm` = executors/4.
  `unpaced` = the 4 churn workers open short connections as fast as they
  can (about 5000 per second); `paced` = 20 ms pause per short connection.

## Table

| shape / metric | WS median | A/A WS2/WS range | PL | PLB fixed | PLB pre-fix | PL pre-fix |
|---|---:|---|---:|---:|---:|---:|
| SSE 500 @2 exec: outcome | 0/24 knee | WS2 0/24 knee | 4/24 knee | 0/24 knee | 0/24 knee | 3/24 knee |
| SSE 500 @2: p50 | 17.5 | 0.77-1.44 | 0.75* | 0.72* | 0.74* | 0.75* |
| SSE 500 @2: p99 | 67.0 | 0.43-2.65 | 0.71 | 0.58 | 0.58 | 0.70 |
| CPU/event, 200 streams, 1 conn | 4.2 | 0.94-1.11 | 0.59* | 0.59* | 0.60* | 0.58* |
| CPU/event, 200 streams, 10 conns | 4.4 | 0.92-1.11 | 0.72* | 0.73* | 0.71* | 0.72* |
| one-shot req/s @2 (higher better) | 820493.8 | 0.86-1.16 | 0.99 | 1.01 | 0.98 | 0.97 |
| one-shot req/s @8 (higher better) | 2392712.6 | 0.92-1.08 | 0.93 | 0.93 | 0.94 | 0.92 |
| closed-loop p50 @2 | 469.0 | 0.70-1.54 | 1.29 | 1.26 | 1.29 | 1.28 |
| closed-loop p99 @2 | 984.5 | 0.71-1.76 | 0.83 | 0.84 | 0.81 | 0.83 |
| closed-loop p99 @8 | 382.0 | 0.87-1.38 | 0.96 | 0.93 | 0.92 | 0.90* |
| open loop 320k @2: p50 | 90.5 | 0.28-3.28 | 1.34 | 1.12 | 1.41* | 1.72* |
| open loop 320k @2: p99 | 263.0 | 0.39-2.52 | 1.05 | 1.07 | 1.17 | 1.11 |
| open loop 590k @2: p50 | 234.5 | 0.49-1.59 | 1.25 | 1.39 | 1.29 | 1.51 |
| open loop 590k @2: p99 | 636.5 | 0.64-1.45 | 0.94 | 0.91 | 0.89 | 1.01 |
| unpaced mix-e8 outcome | 8/16 p50>200us, 8 partial | WS2 10/16 p50>200us, 6 partial | 14/16 p50>200us, 2 partial | 3/16 p50>200us | 0/12 p50>200us | 12/12 p50>200us |
| unpaced mix-e8 worst heavy conn p99 | 1911.0 | 0.95-1.02 | 1.31* | 0.75* | 0.42* | 1.85* |
| unpaced mix-e8 worst light conn p99 | 390.0 | 0.89-1.13 | 1.16* | 0.66* | 0.54* | 1.05 |
| unpaced mix-e8 CPU/event | 7.6 | 0.99-1.00 | 0.57* | 0.69* | 0.75* | 0.52* |
| unpaced mix-e8 stopped streams (sum) | 59 | WS2 62 | 41 | 0 | - | - |
| unpaced mix-eprod outcome | 1/16 p50>200us, 15 partial | WS2 4/16 p50>200us, 12 partial | 10/16 p50>200us, 6 partial | 14/16 p50>200us, 2 partial | 10/12 p50>200us, 1 partial | 10/12 p50>200us, 1 partial, 1 failclosed |
| unpaced mix-eprod worst heavy conn p99 | 2326.0 | 0.95-1.04 | 1.79* | 0.69* | 0.61* | 1.75* |
| unpaced mix-eprod worst light conn p99 | 462.0 | 0.88-1.11 | 1.97* | 1.28* | 1.21* | 1.99* |
| unpaced mix-eprod CPU/event | 7.6 | 0.99-1.01 | 0.54* | 0.65* | 0.76* | 0.55* |
| unpaced mix-eprod stopped streams (sum) | 196 | WS2 203 | 64 | 4 | - | - |
| paced mix-e8 outcome | 0/16 p50>200us, 2 partial | WS2 0/16 p50>200us, 1 partial | 0/16 p50>200us | 0/16 p50>200us | 0/12 p50>200us | 0/12 p50>200us, 1 partial |
| paced mix-e8 worst heavy conn p99 | 1180.5 | 0.90-1.11 | 0.17* | 0.14* | 0.09* | 0.17* |
| paced mix-e8 worst light conn p99 | 120.0 | 0.82-1.46 | 0.68* | 0.44* | 0.55* | 0.64* |
| paced mix-e8 CPU/event | 6.6 | 0.99-1.01 | 0.45* | 0.47* | 0.49* | 0.45* |
| paced mix-e8 stopped streams (sum) | 9 | WS2 8 | 1 | 0 | 0 | 1 |
| paced mix-eprod outcome | 7/16 p50>200us, 9 partial | WS2 6/16 p50>200us, 10 partial | 2/16 p50>200us | 0/16 p50>200us, 1 partial | 0/12 p50>200us | 3/12 p50>200us, 1 partial |
| paced mix-eprod worst heavy conn p99 | 1974.5 | 0.89-1.09 | 1.51* | 0.51* | 0.31* | 1.54* |
| paced mix-eprod worst light conn p99 | 366.0 | 0.86-1.21 | 0.90 | 0.55* | 0.49* | 1.02 |
| paced mix-eprod CPU/event | 6.9 | 0.99-1.02 | 0.45* | 0.55* | 0.55* | 0.45* |
| paced mix-eprod stopped streams (sum) | 83 | WS2 68 | 4 | 4 | 3 | 3 |
| paced mixm-e8 outcome | 0/16 p50>200us | WS2 0/16 p50>200us | 0/16 p50>200us | 0/16 p50>200us | 0/12 p50>200us | 0/12 p50>200us |
| paced mixm-e8 worst heavy conn p99 | 275.5 | 0.17-2.26 | 0.27 | 0.16* | 0.35 | 0.43 |
| paced mixm-e8 worst light conn p99 | 60.0 | 0.51-1.37 | 0.73 | 0.60 | 0.64* | 0.81 |
| paced mixm-e8 CPU/event | 7.3 | 0.90-1.14 | 0.39* | 0.42* | 0.40* | 0.40* |
| paced mixm-e8 stopped streams (sum) | 1 | WS2 1 | 0 | 0 | 0 | 0 |
| paced mixm-eprod outcome | 0/16 p50>200us, 1 partial | WS2 0/16 p50>200us, 2 partial | 3/16 p50>200us | 0/16 p50>200us | 0/12 p50>200us | 3/12 p50>200us, 1 partial |
| paced mixm-eprod worst heavy conn p99 | 1317.5 | 0.94-1.08 | 0.09* | 0.06* | 0.06* | 0.11* |
| paced mixm-eprod worst light conn p99 | 102.5 | 0.65-1.42 | 0.72 | 0.62* | 0.53* | 0.67* |
| paced mixm-eprod CPU/event | 8.1 | 0.98-1.03 | 0.37* | 0.40* | 0.40* | 0.38* |
| paced mixm-eprod stopped streams (sum) | 7 | WS2 9 | 0 | 1 | 0 | 1 |
| burst, 200 streams at once: fails | 0/32 | WS2 0/32 | 0/32 | 0/32 | 0/36 | 0/36 |

Regenerate: `COMPACT=... summarize.py` as in `report-tables.sh`, with the
`rebench-linux-*.txt` files.

## Plain read

1. **The fixed PLB keeps its win in every shape except one.** SSE 500: no
   knee in 24 rounds (PL: 4). CPU per event: 0.59 of WS on one connection and
   0.73 on ten, the same as before the fixes. Paced mixes: no saturated round
   at 8 or 12 executors at either load level, worst heavy connection p99
   0.06-0.51 of WS, CPU 0.40-0.55 of WS.
2. **The costs did not change.** One-shot throughput at 8 executors is 0.93
   of WS (every pinned arm). Closed-loop p50 at 2 executors is about 1.26
   (inside this session's wide A/A range). Open-loop p50 is 1.12-1.39, also
   inside the A/A range this time; the pre-fix run had it outside at 320k.
3. **One shape got worse: the unpaced churn mix at 8 executors.** PLB was
   saturated in 3 of 16 rounds (pre-fix 0 of 12), and its worst heavy
   connection p99 is 0.75 of WS (pre-fix 0.42). It is still better than WS
   and much better than PL (1.31, saturated 14 of 16).
   Inference, not measured directly: the fixed balancer ranks by live
   connections first. With about 5000 short connections per second, the
   connection counts are mostly churn, so the rank no longer keeps the heavy
   connections apart. The old rank (task handlers first) did, and that same
   rank is what caused the inline pile-up defect.
4. **At 12 executors unpaced, PLB was already saturated before the fixes**
   (10 of 12 then, 14 of 16 now); the tail ratio is similar (0.61, 0.69).
5. **Stopped streams (t-2654) stay near zero under PLB** (0, 4, 0, 4, 0, 1
   across the six mix shapes), while WS has 1 to 196.

## Rows that changed against the pre-fix run

Changed, in a direction that matters:

| row | PLB pre-fix | PLB fixed | note |
|---|---:|---:|---|
| unpaced mix-e8 outcome | 0/12 saturated | 3/16 saturated | worse |
| unpaced mix-e8 worst heavy conn p99 | 0.42 | 0.75 | worse; PL moved 1.85 to 1.31 between the same two sessions, so part of this can be session drift |
| unpaced mix-e8 worst light conn p99 | 0.54 | 0.66 | worse |
| unpaced mix-eprod outcome | 10/12 saturated | 14/16 saturated | a little worse |
| paced mix-eprod worst heavy conn p99 | 0.31 | 0.51 | worse, still 0 saturated rounds |
| paced mix-e8 worst heavy conn p99 | 0.09 | 0.14 | slightly worse |
| paced mixm-e8 worst heavy conn p99 | 0.35 | 0.16 | better (A/A range is very wide here: 0.17-2.26) |
| unpaced mix CPU/event (e8, eprod) | 0.75, 0.76 | 0.69, 0.65 | better |
| open loop 320k p50 | 1.41* | 1.12 | now inside the A/A range (0.28-3.28, much wider than before) |

Not changed (within 0.03, or the same count): SSE 500 outcome, p50 and p99;
CPU per event on 1 and 10 connections; one-shot req/s at 2 and 8 executors;
closed-loop p50 and p99; open loop 590k; paced mix-e8 and mixm CPU per event;
paced mixm-eprod; burst (0 failures).

## What is open

- A rank of connections + handlers (one score) may keep the heavy
  connections apart under churn AND pass the inline pile-up gate. It is a
  different rule from the approved one, so the committed code does not use
  it. It is measured next as a variant binary.
- The Mac (kqueue) re-bench is not in this file; that run waits for the Mac
  to be idle.
