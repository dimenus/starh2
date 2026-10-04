# starh2 787a3b6 cross-language benchmark on nachos (2026-10-03)

Run 11:48-12:51 CT on nachos (Linux 7.2.5, io_uring, 12 physical cores /
24 threads). starh2 = origin/master 787a3b6, in two builds: work stealing
(WS, the master default) and pinned with the connection balancer (rank
`sum`, flags `--spawn-placement local --conn-balance`). Opponents: Go
net/http, Kestrel (dotnet 10), hyper. Opponent width = starh2's width
(`OPPONENT_WIDTH=match`). Same phases and settings as the 978213b run
(captures/nachos-head-runner.sh): 4 rounds each, arm order rotated.

## Discipline and validity

- Every phase passed on the first try: no phase was dirty (no outside
  process above 50%, outside total under 150%). The gate waited 42 times
  (30 s each) for the load average left by the previous phase to drop
  under 1.5; the top outside process then was qemu at 3%. Steam idled.
- Every phase checked starh2's ready line (scheduling and balancer as
  expected) and had measured rows. No failed streams and no oneshot
  errors in any arm.
- Harness hooks (committed with this summary): run.sh takes `STARH2_ZIO_SCHEDULING` like
  mixed.sh does, and both pass `STARH2_EXTRA_ARGS` to starh2.
- Setup notes: the nachos system .NET has no SDK or ASP.NET Core, so the
  runner used the mise SDK (10.0.401) and `DOTNET_ROOT` pointing at it.

### Medians (4 rounds each)

| shape | starh2 WS | starh2 pinned | Go | Kestrel | hyper |
|---|---:|---:|---:|---:|---:|
| mixed @2: SSE p99 us | 106 | 118 | 140 | 128 | 110 |
| mixed @2: oneshot req/s | 185k | 186k | 94k | 175k | 184k |
| oneshot-only @2: req/s | 190k | 190k | 96k | 170k | 178k |
| mixed @auto: SSE p99 us | 102 | 100 | 260 | 186 | 162 |
| mixed @auto: oneshot req/s | 184k | 185k | 80k | 123k | 168k |
| oneshot-only @auto: req/s | 190k | 189k | 80k | 120k | 173k |
| SSE 200 streams @2: p50 us | 9 | 10 | 44 | 98 | 43 |
| SSE 200 streams @2: p99 us | 25 | 26 | 141 | 232 | 126 |
| conns50 @2: req/s | 330k | 330k | 134k | 303k | 338k |
| conns50 @2: p99 us | 12k | 12k | 7303 | 17k | 16k |
| conns50 @auto: req/s | 320k | 322k | 263k | 284k | 316k |
| conns50 @auto: p99 us | 20k | 19k | 14k | 19k | 19k |
| conns50 @24: req/s | 321k | 320k | 239k | 293k | 316k |
| conns50 @24: p99 us | 20k | 20k | 12k | 18k | 20k |
| conns50 @auto, baseline CPU: req/s | 319k | 322k | 263k | 285k | 316k |

### Ratio starh2 / opponent (same run; req/s: above 1 = starh2 faster; latency: below 1 = starh2 faster)

| shape | WS/Go | pinned/Go | WS/Kestrel | pinned/Kestrel | WS/hyper | pinned/hyper | pinned/WS |
|---|---:|---:|---:|---:|---:|---:|---:|
| mixed @2: SSE p99 us | 0.76 | 0.85 | 0.83 | 1.07 | 0.97 | 1.04 | 1.12 |
| mixed @2: oneshot req/s | 1.97 | 1.97 | 1.05 | 1.06 | 1.00 | 1.01 | 1.01 |
| oneshot-only @2: req/s | 1.98 | 2.00 | 1.12 | 1.11 | 1.07 | 1.07 | 1.00 |
| mixed @auto: SSE p99 us | 0.39 | 0.40 | 0.55 | 0.57 | 0.63 | 0.75 | 0.98 |
| mixed @auto: oneshot req/s | 2.30 | 2.35 | 1.49 | 1.49 | 1.09 | 1.11 | 1.01 |
| oneshot-only @auto: req/s | 2.36 | 2.39 | 1.59 | 1.56 | 1.09 | 1.09 | 1.00 |
| SSE 200 streams @2: p50 us | 0.21 | 0.16 | 0.09 | 0.12 | 0.21 | 0.22 | 1.06 |
| SSE 200 streams @2: p99 us | 0.18 | 0.12 | 0.11 | 0.12 | 0.20 | 0.25 | 1.06 |
| conns50 @2: req/s | 2.46 | 2.51 | 1.09 | 1.09 | 0.98 | 0.99 | 1.00 |
| conns50 @2: p99 us | 1.68 | 1.59 | 0.73 | 0.71 | 0.75 | 0.67 | 0.96 |
| conns50 @auto: req/s | 1.22 | 1.22 | 1.13 | 1.12 | 1.01 | 1.02 | 1.01 |
| conns50 @auto: p99 us | 1.47 | 1.46 | 1.07 | 1.05 | 1.02 | 1.00 | 0.98 |
| conns50 @24: req/s | 1.34 | 1.34 | 1.09 | 1.11 | 1.02 | 1.01 | 1.00 |
| conns50 @24: p99 us | 1.61 | 1.63 | 1.12 | 1.09 | 1.01 | 1.02 | 1.00 |
| conns50 @auto, baseline CPU: req/s | 1.21 | 1.23 | 1.12 | 1.13 | 1.01 | 1.02 | 1.01 |

### Against 978213b (2026-08-24, same shapes; starh2 then was work stealing)

| shape | starh2 old | starh2 WS now | WS now/old | Go old, now | Kestrel old, now | hyper old, now |
|---|---:|---:|---:|---|---|---|
| mixed @2: SSE p99 us | 122 | 106 | 0.87 | 148, 140 | 138, 128 | 112, 110 |
| mixed @2: oneshot req/s | 193k | 185k | 0.96 | 112k, 94k | 174k, 175k | 190k, 184k |
| oneshot-only @2: req/s | 200k | 190k | 0.95 | 99k, 96k | 162k, 170k | 186k, 178k |
| mixed @auto: SSE p99 us | 123 | 102 | 0.83 | 271, 260 | 196, 186 | 130, 162 |
| mixed @auto: oneshot req/s | 189k | 184k | 0.97 | 92k, 80k | 131k, 123k | 174k, 168k |
| oneshot-only @auto: req/s | 200k | 190k | 0.95 | 83k, 80k | 128k, 120k | 180k, 173k |
| SSE 200 streams @2: p50 us | 11 | 9 | 0.82 | 39, 44 | 79, 98 | 44, 43 |
| SSE 200 streams @2: p99 us | 27 | 25 | 0.93 | 159, 141 | 204, 232 | 122, 126 |
| conns50 @2: req/s | 325k | 330k | 1.01 | 136k, 134k | 296k, 303k | 326k, 338k |
| conns50 @2: p99 us | 8824 | 12k | 1.39 | 7272, 7303 | 12k, 17k | 12k, 16k |
| conns50 @auto: req/s | 314k | 320k | 1.02 | 264k, 263k | 281k, 284k | 311k, 316k |
| conns50 @auto: p99 us | 15k | 20k | 1.35 | 9648, 14k | 14k, 19k | 15k, 19k |
| conns50 @24: req/s | 315k | 321k | 1.02 | 242k, 239k | 283k, 293k | 314k, 316k |
| conns50 @24: p99 us | 15k | 20k | 1.34 | 8993, 12k | 13k, 18k | 15k, 20k |
| conns50 @auto, baseline CPU: req/s | 316k | 319k | 1.01 | 267k, 263k | 280k, 285k | 315k, 316k |

## Plain read

1. starh2 leads Go and Kestrel in almost every shape, in both builds:
   about 1.2-2.5x Go and 1.05-1.6x Kestrel on req/s, and 4-11x lower SSE
   latency with 200 streams. Against hyper it is level on req/s (0.98-1.11)
   and better on SSE latency (p99 0.20-0.25x with 200 streams; in the mixed
   shape 0.63-0.97x for WS, and 0.75-1.04x for pinned).
2. Where starh2 loses: conns50 p99 against Go (1.5-1.7x; Go keeps p99 low
   by serving fewer requests, 134k against 330k at width 2), and conns50
   p99 against Kestrel and hyper at auto and 24 (1.0-1.1x, about level).
3. Pinned + balancer against WS is level here: req/s within 1%, latency
   mostly within 6%. The one larger gap is mixed SSE p99 at width 2
   (pinned 1.12x WS, 118 against 106 us, one shape, 4 rounds). These
   shapes use one or a few connections, so the balancer has little to do;
   the placement-followup report has the shapes where it matters.
4. Baseline CPU against native at auto: 319k against 320k req/s (WS),
   no difference, as in August.

## What moved since 978213b (2026-08-24)

- starh2 SSE latency improved: mixed SSE p99 106 against 122 us at width 2
  and 102 against 123 us at auto; SSE 200 p50 9 against 11 us.
- starh2 oneshot req/s at width 2 fell about 5% (190k against 200k; the
  round ranges do not overlap). hyper fell about 4% and Go 3% in the same
  shape, so part or all of it is the host, not starh2.
- conns50 p99 rose about 1.35x for starh2, and also for every opponent
  (Go 1.0-1.4x, Kestrel and hyper 1.3-1.4x), while req/s held. Because
  all arms moved together, this points at the host.
- The host changed between the runs, so cross-date differences are not
  attributable to starh2 alone: kernel 7.1.6-arch1 -> 7.2.5-omarchy, Go
  1.26.6 -> 1.27.1, .NET SDK 10.0.302 -> 10.0.401, cargo 1.97 -> 1.98,
  and a different BoringSSL checkout path. Same-run ratios (the second
  table) are the reliable comparison.

## Files

The raw logs, per-arm medians, the runner and the table scripts are kept off-repo in
`starh2-crosslang-787a3b6.tar.gz` (Ryan's Dropbox, `starh2-captures/`). The harness hooks the run used
(`STARH2_ZIO_SCHEDULING` in run.sh, `STARH2_EXTRA_ARGS` in both scripts) are
committed in `tools/sse_bench/` alongside this summary.
