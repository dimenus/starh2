# Brief: TLS pump on zio.CompletionQueue (t-878)

Rebuild the TLS pump as a `zio.CompletionQueue` driver, so multi-source
waiting is the runtime's primitive instead of our hand-built protocol.
Everything here builds on the post-fix baseline `7235fa7`+; the control
numbers are the post-fix perf-story columns, never the pre-fix bands
(those were stall-polluted; see MEMBIO_WEDGE_WORKLOG.md).

## Provenance

The zio maintainer suggested `zio.CompletionQueue` directly (2026-08-20)
as the zio-specific extension for this shape. Read
`zig-pkg/zio-*/src/completion_queue.zig` FIRST: one driver task waits on
one futex word; operations are raw `ev.Completion`s; `submit` and `close`
are thread-safe; the queue implements the future protocol, so the driver
can wait on it alongside a channel receive in one zio `select`.

Why this is worth a cut: the pump's current wake protocol (tryGet both
queues, `wake.reset`, re-tryGet, dirty-flag swap, `Event.wait`) is exactly
the parallel-machinery shape AGENTS.md warns about. It is measured correct
today, but its correctness is not establishable by reading — the t-866
hunt burned four wrong scheduler theories against it before the real bug
(BIO-in accounting, unrelated) was found by ledger. A CQ driver makes the
lost-wake class UNSAYABLE: there is no reset, no flag, no recheck list.

## Premises (state these when delegating; they are settled)

Ryan confirmed the boundary decision 2026-08-20: edge takes zio directly.

- starh2 is malleable, no external consumers; breaking internal APIs is
  free. Do not build compatibility shims.
- `src/edge` TAKES A DIRECT zio DEPENDENCY, openly. No interface hiding
  the CQ: an abstraction over it would be two implementations of one
  contract. State the new dependency in the landing commit. The std.Io
  purity of edge ends with this cut; `tests/backend_parity.zig` keeps the
  std.Io.Threaded arm only for the h2c paths that remain std.Io-pure.
- The BIO-in park invariant survives the rewrite: the driver must not
  wait while `Conn.pendingInbound()` is true (the t-866 fix). The
  deterministic record test in tls.zig pins the predicate; keep it green.
- One SSL owner (the driver). Packing stays in `emit_batch`. All
  production output still passes `FairScheduler`'s sink
  (`test_queue_wire_bypass == 0` stays a mutation canary).

## The design

- The pump becomes the CQ driver. The blocking `CipherRead` task is
  DELETED: the socket read is a raw completion the driver resubmits after
  each arrival. Task count per connection drops by one.
- Outbound stays a channel: the actor pushes WireChunks; the driver waits
  with `select(.{ .io = &cq, .write = write_ch.asyncReceive() })`. The
  actor-side dirty flags for the pump die with the Event they guarded.
- The actor's `waitForActivity` is an explicit NON-GOAL for this cut.
  One driver rewrite at a time, or the grade cannot attribute.
- Ownership per the CQ doc: operations submitted by other tasks live on
  the heap with driver-owned results; shutdown is `cancelAll(.keep)` and
  drain via `next`. Error paths release exactly once, as everywhere.

## Grading lines (all against 7235fa7 controls, same machine, n>=3)

1. `tools/wedge-probe.sh` 3/3 at WORKERS=2 AND 8, at the recalibrated
   floors (15000/25000). Quote the THRESHOLD on every row.
2. `tools/h2load-wedge-rate.sh` CLEAN 0/30 (wedges AND slow).
3. Packed oneshot within noise of control or better, and CPU/req not
   worse (control: ~493k req/s, 11651 ns/req Darwin loaded; nachos
   columns in the post-fix matrix).
4. Mixed and SSE-200 rows within noise of control or better. SSE p50 is
   the sensitive one; the nachos control is 16us.
5. The deterministic record test and the full suite green; `zb build ci`
   green including release targets (edge now needs zio types to compile
   on every target ci builds — verify musl/gnu cross before declaring).
6. Mutation line: revert the driver's pendingInbound gate; the record
   test must fail. Kill one resubmit path on purpose; the probe must go
   red. A rewrite that cannot fail its own gates is ungraded.
7. RSS within +1 MiB of control at the peak-rss-200 row (the fixed
   sampler). The CQ should SHED state (one fewer task, no cipher pool
   round-trip), not add it.

## Boundary conditions

- Do not touch the h2c dual-pump path in this cut.
- No timeout, no poll, no Select anywhere in the driver loop; the CQ
  future-protocol select is the one multi-source wait.
- If the CQ needs a zio patch to compose with `write_ch` (the channel
  future protocol), that patch goes to the FORK first with Ryan's
  sign-off, and the brief stops until it lands there. Do not vendor-edit
  zig-pkg silently; the t-866 record shows why every result must name
  which zio it ran against.
- If grading line 3 or 4 fails by more than noise, STOP and report with
  the phase-trace deltas. The fallback is keeping today's pump, which is
  correct and fast; this cut is bought with simplicity, not desperation.
