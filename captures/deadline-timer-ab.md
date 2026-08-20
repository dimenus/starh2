# Deadline-timer post-cut evidence (t-883, 2026-08-20)

Raw paired measurements for the a0a9319 heap-delete cut (new = c9da45e
lineage, control = 0e921e6). Re-derive with `tools/cq-nachos-ab.sh` and
the probes; do not cite these numbers forward. Order alternated per
round in every paired block.

## The retired premise (tools/deadline-timer-probe, 5/5 + extended run)

One executor, migration off, 500 ms hog that never yields: a 50 ms
timed wait fires at 51-52 ms for BOTH `zio.ResetEvent.timedWait` and
`std.Io.Event.waitTimeout`. The t-824 starvation that justified the
heap is gone at the runtime level (fork 53504b4).

## nachos (io_uring, musl-static arms, 3 paired rounds, zero errors)

    oneshot w2: new 118280-118414 rps p50 16us | ctl 117528-117871 (noise)
    oneshot w8: new 186332-188135 rps p50 39us | ctl 185429-186765 (noise)
    SSE-200 at 10ms: new p50 10/11/12us | ctl 14/15/15us (BETTER - the
      session-lock + heap churn is off the SSE cadence path)
    h2load specimen: 6/6 PASS; new 903-923k req/s vs ctl 892-909k

Suite soak, paired, same session: cut 0 leaks 0 wedges / 100 runs;
base (heap present) 0 leaks 10 wedges / 100 runs.

## Darwin (M3 Pro)

    wedge-probe W2: 24557-25090 rps (floor 15000) 3/3 PASS
    wedge-probe W8: 45877-45919 rps (floor 25000) 3/3 PASS
      (t-882's own W8 band was 39.9-41.7k; the donate-yield's death pays)
    h2load-wedge-rate: CLEAN 0/30
    mixed (32 SSE + 8 oneshot, STARH2_EXECUTORS=2): SSE 15999/16000
      events at oneshot 45-46k rps; Go reference 16000 at 38k with 2.3x
      the server CPU (11.68s vs 27.17s). Stall round: 31/32 delivering,
      oneshot holds 43k.
    peak-rss-200: new 19664 KiB vs ctl 19744 KiB (200/200 delivering
      both arms).
    ci: pass, pass, WEDGE in the clean A/B (plus 2 wedges under stale
      load earlier). The wedge is the t-850 class; the live lldb capture
      (captures/t883-live-wedge-parked-stacks.txt) shows 65 parked futex
      waiters all in std.Io.Queue getOne - zio Futex wake/cancel
      delivery, heap exonerated. The base ci arm of the A/B was void
      (worktree environment failures), so kqueue cut-vs-base rates stay
      unresolved; the io_uring paired soak above is the controlled
      comparison and favors the cut 0-vs-10.

## Mutation lines

    m1 (early wake removed): full suite HANGS at the watchdog (the
      narrow test-deadlines pass is explained - RST is covered by
      cancellation; the wake's consumers are writer-fail paths in
      lifecycle).
    m2 (timed wait forced .none): deadlines T1 and T2 fail.

## Conditions

- musl-static vs gnu-native: compare within a toolchain.
- Zero client errors in every quoted row.
- NEVER kill a wedge before `tools/wedge-lldb-capture.sh` ran on it.
