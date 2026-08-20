# Verifier-session evidence for the mem-BIO wedge (t-866), 2026-08-19 EOD

From the second-solver/verifier session (socket 76345), held here for the
round-four grading step. Use for grading, not as settled fact.

1. THE WEDGE SURVIVES MIGRATION-OFF. Probe A/B on Darwin, WORKERS=2:
   baseline (migration on) 4 runs, 2 wedges (rps 3249, 0); with
   SERVER_ARGS="--no-task-migration" 6 runs, 1 hard wedge (rps 0).
   Falsifies any fix theory living only in zio's migration/steal path.
   Mechanism must sit in a path common to both scheduler modes:
   futex/Waiter wake, loop.wake, or a starh2-level circular hold.
2. CYCLE CANDIDATE, unruled: the actor park gate at
   src/edge/connection.zig:2458 legally parks with inline_n > 0 when
   pending_complete_receipt_n == complete_receipt_capacity. If the pump is
   simultaneously unable to post the complete-batch WriteCompletion (BIO
   pair or pool exhaustion), that is a RESOURCE CYCLE, not a lost wake —
   no wake audit closes it. The 630 ms trickle argues against a hard
   cycle; rule it out with an occupancy snapshot, not argument.
3. The main-tree wedge-probe.sh edit (SERVER_ARGS passthrough, printed in
   the header) is the verifier's; keep it.

Grading checklist derived from this: the round-four fix must (a) make the
probe green 3/3 at 2 and 8 workers, (b) name an interleaving that exists
in BOTH migration modes, and (c) either explain the 630 ms trickle or
show its snapshot ruling out the receipt-capacity cycle.

4. SECOND-SPECIMEN RATE, Darwin, sha 806137a, executors=2: plain h2load
   -n 400000 -c 50 -m 10 -t 4 wedged 1/10 rounds under a 60 s watchdog
   (healthy rounds finish in ~0.9 s). Tool: tools/h2load-wedge-rate.sh
   (new; per-round bench_lock, kills and counts, evidence per wedge).
   Evidence bundle: /tmp/starh2-h2load-wedge-rate/wedge-5/. A 20-round
   extension is running; totals go here when it finishes.
5. WEDGE-TIME THREAD STATE (from that bundle's server-sample.txt): both
   executor threads parked in Executor.park -> Loop.poll -> kevent, the
   timer executor in poll(.max) -> kevent, process at 0% CPU. Zero
   runnable tasks anywhere. Consistent with a task stuck .waiting whose
   wake was lost, OR a coroutine-level resource cycle; thread stacks
   cannot see parked coroutine stacks, so this does not discriminate
   between those two. The discriminator remains a queue/flag occupancy
   snapshot at the wedge.

## Morning checklist (for whoever reads the post-fix matrix, 2026-08-20)

- Nachos matrix: /tmp/perf-story-post-fix (7235fa7 vs 55835a4). Darwin
  runner armed, writes /tmp/perf-story (progress in
  /tmp/perf-story-darwin-progress.txt).
- REQUIRED when quoting probe rows: name the THRESHOLD each arm ran with.
  The wedge-probe floors (15000 under 8 workers / 25000 at 8+) are
  Darwin-calibrated; rows are only comparable when every one names its
  gate. Verifier's closing request, binding on the write-up.
- Headline question: mem-BIO (7235fa7) vs record path (55835a4) at
  WORKERS=8 — independent verification saw 51.4k vs the ~46k control band
  on Darwin pre-landing.
- Then: 9d0621e ordinary review; t-875 deterministic regression test;
  t-843 plan-of-record rev 7 from the fresh numbers.
