# Complete-oneshot receipt pipeline

Sol Extra High (`gpt-5.6-sol-xhigh`) picked **A — Pipeline** on HEAD `320a170`.
This brief is the implementation contract. Do not satisfy it by deleting the
receipt, finishing jobs before the write ack, or folding `finishHandlerJob`
into AckDrainer.

## Premises (settled — state these if you delegate further)

- Malleable stack, no external consumers. Breaking change is allowed; name it.
- Share-nothing. `session_mu` covers Session + FairScheduler + TLS. A new mutex
  is the signal that a task should have owned the state.
- Do not fight `std.Io` / zio. Do not start by changing zio.
- Production wire through FairScheduler sink. `test_queue_wire_bypass == 0`.
- Complete oneshots skip spawn: encode the inline batch, one `drainEmit`.
  Unticketed DATA joins only while that drain-turn's `hold_unticketed` is live.
  **A drain-turn receipt still exists.** This cut changes who waits it.
- Starve stories stay: other connections; handler work vs actor (SSE must not
  stop ingest); peer stops reading (WritePump owns `writeAll`).
- Only the actor drives the TLS cipher.
- `zig build test` does not reach TLS. `tls-smoke` is the TLS gate. `./zb build ci`
  is definition of done.

Rejected without new evidence (do not retry): skip the receipt; spawn completes
while still draining per job; fold AckDrainer into WritePump; skip completion
hop keyed on `defer_receipt`; encrypt-on-WritePump.

## The defect

`runPendingInline` encodes, `drainEmit`s, then **blocks the actor in
`waitTicket`**. Ingest of the next HEADERS waits on WritePump + AckDrainer.
Unloaded hop matches Go (~129 µs vs 128 µs). Eight in-flight oneshots on one
TLS connection: starh2 p50 ~704 µs / 8.9k rps vs Go 303 µs / 22.6k. SSE event
p50 was not the gap.

Go already does this split. `net/http` `serverConn.serve` never waits for a
handler or for TCP; handlers wait for their frames (`writeDataFromHandler`).
Tiny writes that fit the bufio buffer run inline on `serve`. That is why a
managed runtime wins this oneshot shape: the protocol loop is not the waiter.
http2.zig is not the Datastar opponent (one-shot `setBody`; measured
zero-window oneshots still multiplex). Detail: `tools/oneshot-gap.md`.

AckDrainer `tickets.complete`s; only `TicketTable.wait` reclaims the ticket
slot. Successful acks do **not** `actor_wake.set` (only shutdown / fail_all /
malformed chain). `waitTicket` is today's waiter.

## Design

Keep the receipt. Stop making the actor wait it.

1. **Pending complete-batch receipts are actor-owned**, capacity **2**.
   Not `inline_sids` (that is the ready queue). Each entry holds the batch's
   stream ids, the receipt ticket, and `ticket_slot`. Depth 2 is the first
   pipeline bound Sol named ("a bounded second drain-turn"). Do not use the
   8-deep `write_ch` as the bound: `sendAccountedWire` can already block on
   `write_ch.putOne` while holding `session_mu`; running ahead until that
   blocks is the defect, not the cap.
2. After `drainEmit` attaches the receipt, **return to the actor loop**. Do
   not call `waitTicket` on the actor. Do not `finishHandlerJob` yet. Slots
   stay `in_use`, `awaiting_receipt` true (t-537), `live_handlers` held.
3. **AckDrainer reports, it does not finish jobs.** Copy a boolean through
   `WireChunk` → `WriteCompletion` (integers/bools only; pumps still hold no
   `Connection` pointer). Set it when this chunk carries a complete-batch
   drain-turn receipt. On that ack: `tickets.complete` as today, then
   **flag-before-wake** (`complete_receipt_ready` or equivalent flag, then
   `actor_wake.set`). Do not wake the actor on every SSE flush ticket.
   Do not call `finishHandlerJob` from AckDrainer.
4. Actor, each loop (after `drainCompletions` is fine): if the flag is set,
   consume each **already-complete** pending receipt: `waitTicket` here is
   legal only when it cannot block (event already set) — that is how the
   ticket slot is reclaimed. Then `finishHandlerJob` + `drainCompletions` /
   `releaseSlot` as today. Shutdown / `fail_all` must finalize every pending
   batch the same way.
5. **`hold_unticketed` stays drain-local.** Derive it from this drain-turn's
   receipt ticket, as today. Clear when that scratch is handed to `queueWire`.
   Do not derive it from "any receipt is in flight". Joining later SSE DATA
   onto an earlier complete HEADERS was a lifecycle SSE shutdown hang.
6. If pending receipts are at 2, ingest and queue more complete jobs, but
   **do not `drainEmit` another complete batch** until a slot frees. SSE
   `drainEmit` from task `sendCb` is unchanged.
7. Receipt reservation failure must refuse or fail-closed that batch. It must
   not emit an unreceipted complete write (that is skip-the-receipt).

## Mechanical acceptance (all axes; any failure fails the build)

The laziest passing outputs, and the check that kills each:

| Laziest pass | Kill it with |
|---|---|
| Delete `waitTicket`, finish immediately (skip-receipt) | Test: while the first complete batch's write ack is **held**, a second complete GET on the same connection must observe `live_handlers >= 2` (or equivalent: first job still `in_use` and second dispatched). Skip-wait finishes the first job, so the count never stays at 2. Also `records/response` on packed h2load must stay **< 0.4** (`tools/oneshot-phase-trace.sh` exit 9). |
| Leave `waitTicket` on the actor (no-op) | Same hold-ack test: second request must dispatch **before** the first ack is released. Today's code cannot. Plus 8-worker p50 must move (below). |
| Naked `actor_wake.set` without a flag | Lost-wakeup: actor reset then park. The hold-ack test plus SSE lifecycle hang if the flag is missing. Follow the existing flag-before-wake rule (`sched_refilled`). |
| `finishHandlerJob` in AckDrainer | Premise 2 / pump contract. Pumps and AckDrainer exchange integers. Also the rejected completion-hop / SSE shutdown class. |
| Wake actor on every write ack | SSE event rate wakes the actor every record. Forbidden. Only complete-batch receipt chunks set the wake bit. |
| Bound of 1 | Not a pipeline. Pending-receipt capacity must be 2, asserted. |
| Depth 2 but latency unchanged | Interleaved keep-vs-pipeline 8-worker oneshot-only (below). |
| Packing sacrificed for p50 | `records/response < 0.4`. |
| Leak ticket slots / handler slots | After the hold is released: `reaper_reserved == 0`, `live_handlers == 0`, `test_observed_slots_in_use == 0`, every reserved complete-batch ticket consumed by `wait`/`releaseReserved`. |
| Change the 8-worker client to 1 worker so p50 matches Go | Client stays 8 workers, 1 connection. |

### Required new test (you write this; do not call it optional)

A same-connection test with a **test hook that holds WritePump/AckDrainer
completion of the complete-batch receipt** (mirror `test_hold_completion_drain`,
do not reuse that hook — it is the completion channel, not the write ack).

Sequence:

1. Start server, one h2c or TLS client, complete `/` (or `/hello` if that is
   what the test server serves — use `examples/bench_server.zig`'s `/` shape or
   an equivalent complete handler).
2. Arm the hold. Send oneshot A. Wait until A is dispatched (`live_handlers >= 1`
   or `test_observed_live_handlers`).
3. Send oneshot B on the **same connection**.
4. **Assert B dispatches before the hold is released** (`live_handlers >= 2`).
   Timeout fail, not hang. This is the ingest-overlapped-write invariant.
5. Release the hold. Both complete. Accounting and live/slot counters return to
   zero. DebugAllocator clean if this is a lifecycle-style test.

HEAD today must fail step 4. If your test passes on unmodified HEAD, the test
is wrong — stop and fix the test, not the production code.

Existing gates that must stay green: `./zb build test-lifecycle`,
`test-backend-parity`, `test-exact`, `./zb build test -j1`, `./zb build tls-smoke`.
Do not skip `tls-smoke`. Do not weaken h2spec exclusions.

### Measurement (not a substitute for the test)

Interleaved keep vs this cut, Darwin is enough if Nachos is unreachable:

```sh
# 8 workers, 1 TLS connection, no SSE, same Go client as mixed.sh oneshot-only
# keep = binary before the cut; pipeline = your binary
```

Record: p50, mean if easy, rps, **max outstanding complete receipts** (test
counter or `/trace` field you add), `records/response` from
`tools/oneshot-phase-trace.sh` on the pipeline binary.

**Reject the cut** (leave it unmerged, say so) if:

- max outstanding receipts never reaches 2 under that 8-worker load, or
- depth ≥ 2 but p50/rps sit inside keep-vs-keep repeat spread, or
- `records/response > 0.4`.

Do not tune `--executors` or worker count to manufacture a win. Mixed.sh
defaults stay `STARH2_EXECUTORS=2`, 8 oneshot workers.

After a real move: `tools/sse_bench/mixed.sh` still delivers the printed SSE
event target (16000 at default STREAMS/INTERVAL/SECONDS) and stall is 31/32
delivering. The script does not fail on a short SSE count — **you** inspect
the event count. A silent drop is a fail.

Update `tools/oneshot-gap.md` with the numbers. Do not invent them.

## Tripwire (do this first, then stop if it fails)

```sh
git rev-list --count HEAD..master
```

Expected: **0**. If it is not 0, STOP and report. Do not rebase and continue.

Base must be the commit that contains this brief.

## Commits

Commit per milestone (a killed run resumes from the last commit):

1. Pending-receipt type + AckDrainer wake bit + actor consume path. Red test
   from "Required new test" written first if possible.
2. `runPendingInline` no longer blocks. Test green. Comments in
   `runPendingInline` updated: the receipt is the self-clock; the actor is not
   the waiter.
3. Gates + measurement + `oneshot-gap.md`. If the measurement rejects the cut,
   commit the test+code anyway only if the test is correct on HEAD-fail; then
   STOP without claiming a latency win.

Do not regenerate goldens. Do not touch `captures/`, `DIAG_BUILD_SPEC.md`,
`HANDOFF.md`. Do not push.

## Zig

Zig 0.16. `./zb` not a guessed `zig`. Do not guess 0.16 APIs — `zigstd`.
`std.testing.FailingAllocator` is not thread-safe.
