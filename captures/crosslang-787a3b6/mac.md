# starh2 787a3b6 cross-language benchmark on the Mac (2026-10-03)

Run 15:22-16:58 CT on Ryan's MacBook Pro (macOS 25.6, arm64, 18 cores, no
SMT, kqueue, on AC). starh2 = origin/master 787a3b6 in two builds: work
stealing (WS, the master default) and pinned with the connection balancer
(rank `sum`, `--spawn-placement local --conn-balance`). Opponents: Go
net/http (1.27.1), Kestrel (.NET SDK 10.0.401), hyper (cargo 1.98.1), each
pinned to starh2's width (`OPPONENT_WIDTH=match`). Same phases and settings
as the nachos run in `nachos.md`, 4 rounds
each with the arm order rotated. All toolchains were present; nothing was
installed.

## Discipline and validity

- Idle gate as in the Mac re-bench: 1-minute load under 1.5 and no outside
  process over 50% before a phase; during the measurement, outside CPU
  under 150% total and 50% for any one process, or the phase reruns.
- 3 phases were rerun once, all for real outside load: a Microsoft app
  (112% and 180%) twice, and macOS background services (mobileassetd,
  duetexpertd) once. All 14 phases then passed. The gate also waited 95
  times (30 s each) before phases.
- The gate counts outside load from the moment the harness prints
  `== width:`, after every arm is built, launched and ready. Before that
  change the first two tries of the first phase were discarded for
  syspolicyd, corespotlightd and XprotectService: macOS scanning the
  binaries the phase had just built, which happens before any measured
  round. The output folders carry `.metadata_never_index`.
- Every phase checked starh2's ready line and had measured rows. No failed
  streams and no oneshot errors in any arm.
- The build-config control uses `-Dtarget=aarch64-macos` (generic CPU for
  this platform) instead of nachos's `-Dtarget=x86_64-linux-gnu`.
- `auto` = 18 executors here (12 on nachos), so the `@24` phase
  oversubscribes the Mac's cores.

### Medians, Mac (4 rounds each)

| shape | starh2 WS | starh2 pinned | Go | Kestrel | hyper |
|---|---:|---:|---:|---:|---:|
| mixed @2: SSE p99 us | 116 | 105 | 151 | 146 | 120 |
| mixed @2: oneshot req/s | 126k | 129k | 78k | 103k | 123k |
| oneshot-only @2: req/s | 130k | 131k | 79k | 102k | 122k |
| mixed @auto: SSE p99 us | 158 | 106 | 328 | 220 | 172 |
| mixed @auto: oneshot req/s | 125k | 129k | 66k | 69k | 117k |
| oneshot-only @auto: req/s | 131k | 130k | 67k | 69k | 116k |
| SSE 200 streams @2: p50 us | 75 | 84 | 65 | 282 | 164 |
| SSE 200 streams @2: p99 us | 198 | 186 | 176 | 1048 | 346 |
| conns50 @2: req/s | 192k | 193k | 122k | 166k | 191k |
| conns50 @2: p99 us | 28k | 28k | 8112 | 30k | 28k |
| conns50 @auto: req/s | 149k | 161k | 134k | 146k | 169k |
| conns50 @auto: p99 us | 37k | 34k | 24k | 36k | 32k |
| conns50 @24: req/s | 149k | 159k | 129k | 145k | 169k |
| conns50 @24: p99 us | 37k | 34k | 23k | 35k | 32k |
| conns50 @auto, generic CPU: req/s | 150k | 161k | 133k | 147k | 168k |

### Ratio starh2 / opponent, Mac (same run; req/s: above 1 = starh2 faster; latency: below 1 = starh2 faster)

| shape | WS/Go | pinned/Go | WS/Kestrel | pinned/Kestrel | WS/hyper | pinned/hyper | pinned/WS |
|---|---:|---:|---:|---:|---:|---:|---:|
| mixed @2: SSE p99 us | 0.77 | 0.69 | 0.80 | 0.66 | 0.97 | 0.86 | 0.90 |
| mixed @2: oneshot req/s | 1.61 | 1.65 | 1.23 | 1.27 | 1.02 | 1.04 | 1.03 |
| oneshot-only @2: req/s | 1.64 | 1.65 | 1.28 | 1.30 | 1.06 | 1.07 | 1.01 |
| mixed @auto: SSE p99 us | 0.48 | 0.31 | 0.72 | 0.44 | 0.92 | 0.59 | 0.67 |
| mixed @auto: oneshot req/s | 1.89 | 1.95 | 1.81 | 1.89 | 1.06 | 1.11 | 1.03 |
| oneshot-only @auto: req/s | 1.95 | 1.92 | 1.90 | 1.89 | 1.13 | 1.11 | 0.99 |
| SSE 200 streams @2: p50 us | 1.15 | 1.16 | 0.27 | 0.23 | 0.46 | 0.48 | 1.11 |
| SSE 200 streams @2: p99 us | 1.13 | 0.81 | 0.19 | 0.17 | 0.57 | 0.52 | 0.93 |
| conns50 @2: req/s | 1.58 | 1.59 | 1.16 | 1.13 | 1.01 | 1.02 | 1.00 |
| conns50 @2: p99 us | 3.40 | 3.39 | 0.92 | 1.00 | 0.98 | 0.99 | 1.00 |
| conns50 @auto: req/s | 1.11 | 1.19 | 1.02 | 1.10 | 0.88 | 0.95 | 1.08 |
| conns50 @auto: p99 us | 1.51 | 1.42 | 1.03 | 0.96 | 1.14 | 1.06 | 0.93 |
| conns50 @24: req/s | 1.15 | 1.24 | 1.02 | 1.09 | 0.88 | 0.94 | 1.07 |
| conns50 @24: p99 us | 1.60 | 1.48 | 1.05 | 0.98 | 1.14 | 1.06 | 0.93 |
| conns50 @auto, generic CPU: req/s | 1.13 | 1.20 | 1.02 | 1.10 | 0.89 | 0.96 | 1.07 |

### nachos against Mac: same-run ratios (nachos, Mac)

| shape | WS/Go | WS/Kestrel | WS/hyper | pinned/WS |
|---|---|---|---|---|
| mixed @2: SSE p99 us | 0.76, 0.77 | 0.83, 0.80 | 0.97, 0.97 | 1.12, 0.90 |
| mixed @2: oneshot req/s | 1.97, 1.61 | 1.05, 1.23 | 1.00, 1.02 | 1.01, 1.03 |
| oneshot-only @2: req/s | 1.98, 1.64 | 1.12, 1.28 | 1.07, 1.06 | 1.00, 1.01 |
| mixed @auto: SSE p99 us | 0.39, 0.48 | 0.55, 0.72 | 0.63, 0.92 | 0.98, 0.67 |
| mixed @auto: oneshot req/s | 2.30, 1.89 | 1.49, 1.81 | 1.09, 1.06 | 1.01, 1.03 |
| oneshot-only @auto: req/s | 2.36, 1.95 | 1.59, 1.90 | 1.09, 1.13 | 1.00, 0.99 |
| SSE 200 streams @2: p50 us | 0.21, 1.15 | 0.09, 0.27 | 0.21, 0.46 | 1.06, 1.11 |
| SSE 200 streams @2: p99 us | 0.18, 1.13 | 0.11, 0.19 | 0.20, 0.57 | 1.06, 0.93 |
| conns50 @2: req/s | 2.46, 1.58 | 1.09, 1.16 | 0.98, 1.01 | 1.00, 1.00 |
| conns50 @2: p99 us | 1.68, 3.40 | 0.73, 0.92 | 0.75, 0.98 | 0.96, 1.00 |
| conns50 @auto: req/s | 1.22, 1.11 | 1.13, 1.02 | 1.01, 0.88 | 1.01, 1.08 |
| conns50 @auto: p99 us | 1.47, 1.51 | 1.07, 1.03 | 1.02, 1.14 | 0.98, 0.93 |
| conns50 @24: req/s | 1.34, 1.15 | 1.09, 1.02 | 1.02, 0.88 | 1.00, 1.07 |
| conns50 @24: p99 us | 1.61, 1.60 | 1.12, 1.05 | 1.01, 1.14 | 1.00, 0.93 |
| conns50 @auto, generic CPU: req/s | 1.21, 1.13 | 1.12, 1.02 | 1.01, 0.89 | 1.01, 1.07 |

## Plain read

1. On the Mac starh2 still leads Go and Kestrel on req/s in every shape
   (about 1.1-1.95x Go, 1.0-1.9x Kestrel), and has lower mixed SSE p99
   (0.31-0.80x).
2. Against hyper starh2 is level at width 2 (1.01-1.07x on req/s) but
   behind on conns50 at auto and 24: WS 0.88x, pinned 0.94-0.95x (hyper
   169k against 149k and 161k; the round ranges do not overlap).
3. SSE 200 streams: starh2 beats Kestrel (0.17-0.27x) and hyper
   (0.46-0.57x), but is level with Go (p50 75 against 65 us, p99 198
   against 176 us), with wide, overlapping round ranges. nachos showed a
   5x lead over Go in the same shape; the Mac does not reproduce it.
4. Pinned + balancer helps more on the Mac than on nachos: mixed SSE p99 at
   auto 106 against 158 us (0.67x, ranges do not overlap), and conns50 req/s
   at auto and 24 about 1.07-1.08x WS. Elsewhere it is level.
5. conns50 p99 against Go is the worst row on both hosts (Mac 3.4x at
   width 2), again with Go serving far fewer requests (122k against 192k).

## nachos against Mac (ratios, not absolute numbers)

- Same direction on both hosts: starh2 ahead of Go and Kestrel on req/s;
  level with hyper at width 2; mixed SSE p99 better than every opponent at
  width 2 (0.76-0.97 on both); conns50 p99 worse than Go.
- Smaller lead over Go on the Mac: oneshot req/s 1.6-1.95x against
  2.0-2.5x on nachos.
- Larger lead over Kestrel on the Mac at auto: 1.8-1.9x against 1.5-1.6x.
- Weaker against hyper on the Mac at wide conns50: 0.88x WS against
  1.01-1.02x on nachos.
- SSE 200 streams: nachos 0.18-0.21x Go, Mac about level with Go (1.13-1.15x
  for WS, with wide spread).
- Pinned against WS: level on nachos (within about 6%, except mixed SSE p99
  @2 at 1.12x); on the Mac pinned is better at auto (mixed SSE p99 0.67x,
  conns50 req/s 1.07-1.08x).

## Files

The raw logs, per-arm medians, the runner and the table scripts are kept off-repo in
`starh2-crosslang-787a3b6-mac.tar.gz` (Ryan's Dropbox, `starh2-captures/`). The harness hooks the run used
(`STARH2_ZIO_SCHEDULING` in run.sh, `STARH2_EXTRA_ARGS` in both scripts) are
committed in `tools/sse_bench/` alongside this summary.
