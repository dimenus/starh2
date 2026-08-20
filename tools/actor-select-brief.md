# Brief: the actor waits in one zio select (t-882)

Rebuild the actor's `waitForActivity` as one `zio.select`, so the last
hand-built multi-source wait in the edge dies. The recheck-list class
("a source left out of this list is a hang", `connection.zig` ~2606)
must become unsayable, the same way t-878 made the pump's lost-wake
class unsayable. Everything here grades against the CQ-landed baseline
`55e0c33`+; the control numbers are the paired columns in
`captures/cq-post-landing-ab.md`, re-derived with `tools/cq-nachos-ab.sh`.

## Provenance

t-882 is the follow-on that the CQ brief named as its explicit non-goal
("one driver rewrite at a time, or the grade cannot attribute"). The CQ
cut landed and won its A/B on every line, so the attribution slot is
free.

Read these bodies FIRST, before any design work (AGENTS.md rule; a
builder that used an API it did not read has not finished this brief):

- `zig-pkg/zio-*/src/select.zig` — the future protocol contract. The
  fast path checks branches in DECLARATION ORDER and returns without
  registering; a loser branch's `asyncCancelWait` returning false means
  the wake is in flight and the primitive must transfer it.
- `zig-pkg/zio-*/src/sync/channel.zig` — mutex-guarded ring, direct
  sender-to-receiver handoff, wake transfer on a lost claim, graceful
  close drains buffered items before `error.Closed`.
- `zig-pkg/zio-*/src/time.zig` (`Timeout`) — implements the future
  protocol; each `asyncWait` arms an `ev.Timer` in its per-wait
  context; `.none` registers nothing and never completes, so the timer
  branch can be present in every select.
- `zig-pkg/zio-*/src/sync/ResetEvent.zig` — persistent set, future
  protocol. The one-shot shape for shutdown.
- `zig-pkg/zio-*/src/sync/Notify.zig` — read it to REJECT it: a
  `signal()` with no registered waiter is a no-op and the wake is LOST.
  Notify re-creates the set-before-park race this cut deletes. Do not
  use it for any actor wake.
- `src/edge/tls.zig` (`Pump.run`, ~1090–1180) — the landed house
  pattern: progress loop, then one select; no reset, no dirty flag.

## Premises (settled; state them when delegating)

- Ryan ruled 2026-08-20 (t-878): `src/edge` takes zio directly, openly.
  This cut extends that to the ACTOR, which h2c and TLS share. The
  consequence must be stated in the landing commit: the h2c path stops
  being std.Io-pure, and the `std.Io.Threaded` arm of
  `tests/backend_parity.zig` can no longer run a connection. Delete or
  shrink that arm in the same commit; do NOT keep a second
  `waitForActivity` behind a backend switch — two implementations of
  one contract drift.
- starh2 is malleable, no external consumers. No compatibility shims.
- In production the std.Io vtable IS the zio runtime, so the actor task
  can call `zio.select` directly, exactly as the TLS pump already does.
- The handler-deadline HEAP is out of scope. t-883 owns replacing it,
  and t-850/t-873 hold a live deadlock candidate against it. This cut
  only changes HOW the actor waits on the heap's min deadline, not the
  heap. Coordinate before touching `deadlineHeapInsert` or its locks.

## The producer inventory (complete, from `connection.zig` @ 55e0c33+)

Every producer that can wake the actor today, with its evidence store:

| # | producer | today | becomes |
|---|---|---|---|
| 1 | ReadPump chunk / EOF post (`wire_pump.zig:108,145`) | `read_ch` std.Io.Queue + wake | `zio.Channel(WireChunk)`, select branch `.reads` |
| 2 | TLS pump chunk post + `tls_read_dirty` (`tls.zig:575`) | same queue + dirty flag + wake | same channel; `tls_read_dirty` DIES |
| 3 | WritePump/TLS-pump completion post (`wire_pump.zig:214`, `tls.zig:568`) | `write_ack_ch` + wake | `zio.Channel(WriteCompletion)`, branch `.write_acks` |
| 4 | Handler completion + reaper path (`connection.zig:4376, 857`) | `completion_ch` + wake | `zio.Channel(u32)`, branch `.completions` |
| 5 | Scheduler refill from a handler (`connection.zig:3307,3315`) | `sched_refilled` flag + wake | flag stays; producer rings the DOORBELL |
| 6 | Inline complete job queued (`connection.zig:4321`) | `inline_n` + wake | doorbell |
| 7 | Handler deadline armed (`connection.zig:5128`) | `deadline_armed` flag + heap + wake | doorbell (the next park recomputes `nextDeadlineNs`) |
| 8 | Writer failure | `writer_failed` flag + pump post | doorbell (or arrives via branch 3, verify the site) |
| 9 | Complete-receipt readiness | `complete_receipts_ready` atomic | doorbell |
| 10 | Server shutdown | std Event + atomic flag | `zio.ResetEvent`, branch `.shutdown`; set once, never reset |
| — | protocol/handler deadlines | timer added only when armed | `zio.time.Timeout` branch, `.deadline` from `nextDeadlineNs()`, `.none` otherwise |
| — | `test_release_complete_receipt_ack` hold | extra select arm | keep as an extra branch on a zio primitive with persistent state |

The DOORBELL is one `zio.Channel(void)` with capacity 1, producer side
`trySend` where `error.WouldBlock` means "already rung" — the same
coalescing `actor_wake.set` gives today, but the evidence is channel
state that persists until the actor takes it. There is no reset
anywhere, so the set-after-reset race has no words left.

- Rung-4 rule: flag writes and the ring live in ONE function
  (`ring(reason)`), and that function is the only writer of those
  flags. A future producer cannot set a flag without ringing.
- The actor's turn order stays: on any wake, drain acks, completions,
  receipts, THEN flags, then emit — the existing loop top already does
  this. The doorbell take happens before the flag drain.
- `write_ack_ch` and `completion_ch` keep their proven capacities
  (`control_entries + max_streams`; the post sites never block today
  via `putOneUncancelable`). A full channel on the producer side is an
  invariant violation: assert, never drop, never block a pump.

## The design

- `waitForActivity` becomes: progress checks, then
  `zio.select(.{ .reads, .write_acks, .completions, .doorbell, .timer, .shutdown })`.
  Declaration order is the tie-break on simultaneous readiness; choose
  it deliberately and say why in the commit (reads-first matches
  today's turn shape).
- The reset-then-recheck block at `connection.zig` ~2606–2631 is
  DELETED, including `actor_wake`, `tls_read_dirty`, and the
  ten-condition park guard. The park guard's conditions become either
  channel state (already selected) or flags the doorbell covers.
- `io_queue.tryGet` loops at the loop top become channel `tryReceive`
  loops; the batch-ingest shape (up to `inbound_chunk_batch`) is
  unchanged.
- Teardown: closing the channels is the wake. `close(.graceful)` on
  read/ack channels replaces the sentinel push for h2c and matches the
  TLS pump's existing contract; a select winner of `error.Closed`
  routes to the teardown path. The teardown drain order in `run`'s
  defer (stop pumps, shutdown socket, cancel, drain acks last) is a
  contract; keep it.
- Ownership: buffered `WireChunk`s carry pool indices and accounting.
  Whoever drains a closed channel releases exactly once — the same
  failDrain discipline the TLS pump landed.

## Grading lines (all against 55e0c33+ controls, same machine, n>=3)

Controls: `captures/cq-post-landing-ab.md`; re-derive, do not cite.
Note t-880 is fixed (8af4c78): nachos can build fresh again with an
explicit `-Dtarget`; the cross-build-only condition in that capture is
lifted, but never compare musl-static rows against gnu-native rows.

1. `tools/wedge-probe.sh` 3/3 at WORKERS=2 AND 8, floors 15000/25000.
   Quote the THRESHOLD on every row.
2. `tools/h2load-wedge-rate.sh` CLEAN 0/30.
3. Packed oneshot within noise of control or better on both boxes
   (nachos w2 control band ~112k rps p50 17us; w8 ~177k p50 41us).
4. Mixed and SSE-200 within noise or better; SSE p50 is the sensitive
   line (nachos control 19us).
5. Full suite and `zb build ci` green, including release cross-targets.
6. Mutation lines, all three, or the rewrite is ungraded:
   - Remove one producer's doorbell ring (e.g. the refill site); a
     deterministic test must hang-detect or fail, and the h2load screen
     must go red. If nothing fails, the coverage is a lie — build the
     missing deterministic test first.
   - Drop the timer branch with a deadline armed; the deadline tests
     must fail.
   - Close-path mutation: skip the ack-channel drain in teardown; the
     leak/ownership canaries must fire.
7. RSS within +1 MiB of control at peak-rss-200. Task count is
   unchanged (this cut deletes a wait protocol, not a task), so any
   regression is real.

## Boundary conditions

- Do not touch the TLS pump's driver loop, the deadline heap's
  structure, or `FairScheduler`. `test_queue_wire_bypass == 0` stays a
  mutation canary.
- No poll, no `sleep(.zero)` in the park path, no flag recheck between
  select and park — the select IS the park. The donate-yield after
  `fired > 0` on the hot turn stays; it is not part of the wait.
- If zio needs a patch to compose these branches, it goes to the FORK
  first with Ryan's sign-off, and the work stops until it lands there.
  Never vendor-edit `zig-pkg` silently; every result names its zio pin.
- If grading line 3 or 4 fails beyond noise, STOP and report with
  phase-trace deltas. The fallback is today's actor, which is measured
  correct and fast; this cut is bought with simplicity, not throughput.
