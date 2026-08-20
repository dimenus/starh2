# Actor-select post-cut A/B evidence (t-882, 2026-08-20)

Raw paired measurements for the ec52be2 actor-select cut (new = ec52be2,
control = 55e0c33). Kept as CAPTURED EVIDENCE with generating conditions;
re-derive with `tools/cq-nachos-ab.sh ec52be2 55e0c33` rather than citing
these numbers forward. Order alternated per round in every paired block.

## nachos (io_uring, musl-static cross-built arms)

oneshot, one TLS conn, Go client, 3 paired rounds, zero client errors:

    w2: new 117916/117174 rps p50 16us  |  ctl 111508 rps p50 17us
    w8: new 186091/186860/186212 rps p50 39us  |  ctl 177120/177551/177698 p50 41us

SSE-200 at 10ms: new p50 13/17/14us  |  ctl 17/20/19us.

h2load 400k / 50 conns / m=10 / t=4: all 6 rounds PASS (no WEDGE/SLOW);
new 873k-908k req/s vs ctl 848k-876k; new won 3/3 pairs.

## Darwin (M3 Pro)

    wedge-probe W2: 23602/23814/24165 rps (floor 15000) - 3/3 PASS
    wedge-probe W8: 39902/40378/41703 rps (floor 25000) - 3/3 PASS
    h2load-wedge-rate: CLEAN 0/30
    peak-rss-200 (tls, 200 SSE streams, 10s, 200k events delivered/arm):
      new 18416 KiB, ctl 18928 KiB (new 512 KiB SMALLER)
      client-side SSE p50: new 66us vs ctl 92us

## Correctness soaks on the final pin (zio 3025548)

    Darwin: 30 suite runs - 0 accounting leaks, 0 clobbers.
    nachos: 100 suite runs - 0 accounting leaks, 0 clobbers, 5 timeout
      wedges (t-850 class).
    nachos BASELINE control (55e0c33): 100 suite runs - 0 accounting
      crashes, 1 timeout wedge. The wedge class predates the cut; the
      5-vs-1 wedge count is weak evidence of worsening, tracked on t-850.
    Pre-fix rates for the two zio select bugs this cut exposed: item-drop
      leak ~1/8 suite runs; fast-path clobber 8/30 (diagnostic CAS).

## Mutation lines (gate 6)

    a. refill ring removed -> T2 + compression tests fail (lost refill
       wake starves big bodies).
    b. deadline timer branch forced .none -> T2 deadline ordering fails.
    c. BOTH teardown ack-drain layers removed -> deinit accounting
       panics (integer overflow at the ledger). NOTE: removing only
       drainWriteAcksForced is absorbed by deinit's redundant drain
       (filed as a simplification candidate).

## Conditions that matter when re-running

- musl-static is not gnu-native; compare within a toolchain.
- Zero client errors in every row above; a row with errors is a bug
  report, not a datapoint.
- nachos builds fresh again since 8af4c78 (t-880) with an explicit
  -Dtarget; native (no -Dtarget) still hits the SFRAME linker limit.
