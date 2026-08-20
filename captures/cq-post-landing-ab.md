# CQ driver post-landing A/B evidence (2026-08-20)

Raw paired measurements for the t-878 CompletionQueue driver after landing
(new = 55e0c33 lineage, control = 7235fa7). Kept as CAPTURED EVIDENCE with
generating conditions; re-derive with `tools/cq-nachos-ab.sh <new> <base>`
(nachos) and the wedge/h2load probes (Darwin) rather than citing these
numbers forward. Order was alternated per round in every paired block.

## nachos (io_uring, musl-static cross-built arms, load 0.03-0.24)

oneshot, one TLS conn, Go client:

    w2: cq 112799/112428/112217 rps p50 17us  |  ctl 84663/84700/84473 rps p50 21us
    w8: cq 177900/177132/176770 rps p50 41us  |  ctl 173327/172834/170832 rps p50 43us

h2load 400k / 50 conns / m=10 / t=4, 15 paired rounds, all PASS (no
WEDGE/SLOW/PARTIAL): cq 831k-885k req/s (median ~857k) vs ctl 799k-831k
(median ~813k); cq won 15/15 pairs.

SSE-200 at 1ms: parity - cq p50 19/19/19us, ctl p50 20/19/18us.

Wedge screens: 0/10 low rounds per arm (Go client w8), 0/30 on h2load.

Go net/http reference, same box, same discipline (go server from
tools/sse_bench/server.go, cross-built; load 0.24):

    w2:  go 64157/63384/63416 rps p50 28us p99 ~91us
    w8:  go 82509/83848/82007 rps p50 87us p99 ~285us
    sse: go p50 278/295/180us p99 424-655us (go's rounds are the noisy arm)

So on Linux the landed stack is ~1.76x Go at w2, ~2.13x at w8, and ~12x on
SSE p50 - a full reversal of the pre-fix characterization (SSE p50 was 2x
BEHIND Go in the HANDOFF-era record).

## Darwin (M3 Pro, resident load ~3-6: Defender/tailscaled/XProtect)

From the t-878 delegate report plus my independent grading runs:

    wedge-probe w2: 23.1k-26.9k rps (floors 15000) - all PASS
    wedge-probe w8: 43.1k-44.3k rps (floors 25000) - all PASS
    h2load-wedge-rate: CLEAN 0/30
    mixed (32 SSE + 8 oneshot, one conn): cq ~43.1k rps vs ctl 36-38k
      at equal SSE latency (+14%)
    SSE-200: equal or better vs control; RSS: cq 0.3-0.7 MB SMALLER

## Conditions that matter when re-running

- nachos toolchain drift (t-880) blocks building THERE; both arms must be
  cross-built from the Mac until it is fixed, which is what the runner
  script encodes. musl-static is not gnu-native; compare within a
  toolchain, never across.
- Darwin's quiet floor is ~3+ (Defender, tailscaled); pairing and order
  alternation are what make its rows meaningful, not absolute quiet.
- Zero client errors in every row above; a row with errors is not a
  datapoint, it is a bug report.
