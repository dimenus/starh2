Reading B. Tier 3 must give every stream that has eligible DATA one 16 KiB quantum before any of those streams gets a second quantum. Ascending stream-id order is how this build finds "the next stream," and nothing in the tests or in `connection.zig` requires that sequence.

## 1. Which reading the code requires

**[LOGIC]** The requirement is the purpose clause on the cursor, not the sort that implements it:

```28:30:src/edge/fair_scheduler.zig
//! Within tier 3, `drr_cursor` is what makes the round robin fair. It resumes
//! at the stream after the one last served, so a low-numbered stream cannot win
//! every pass. A fixed quantum caps how long any one stream holds the wire.
```

That sentence is round-robin. The failure it names is a low id winning every pass. The same module states the failure a byte-FIFO causes: one large download holds the connection and stalls the other streams on it (`fair_scheduler.zig` lines 9–13). A fixed quantum plus a rotation is the whole contract. There is no per-stream deficit field anywhere on `FairScheduler`; `drr_cursor` is only a resume point. "One quantum, then the next eligible stream" is what the code does.

The id sequence is the mechanism. Each successful pending emit does this, which is not in the brief:

```663:663:src/edge/fair_scheduler.zig
            self.drr_cursor = sid +% 2;
```

Combined with the ascending sort, `+% 2` is "the next candidate id" for the odd client ids this server actually accepts. `Session.onHeaders` rejects `stream_id % 2 == 0`, and `push_promise` fails the connection, so the live set is odd ids and `+% 2` lines up with "next in the sorted array." That alignment is an encoding of the rotation. It is not a second contract.

**[LOGIC]** No test pins id order. The only multi-stream emit is `tests/scheduler.zig` "fair scheduler: terminal when ordinary full; zero windows": it enqueues stream 1 then stream 3, gives stream 1 a zero window, and asserts that `emitOneDataPublic` returns true. `build` discards `sid` (lines 198–199). The assertion is "a blocked stream does not stop the other stream," which an arrival ring satisfies the same way a sorted scan does. The unit tests in `src/edge/fair_scheduler.zig` never emit two streams. `drr_cursor` is never read by a test.

**[LOGIC]** `connection.zig` does not depend on which pending stream is next. The call sites that touch pending streams are `forEachPending` (deinit accounting, writer-fail drop, slow-consumer sweep, shutdown sweep), `nextSlowDeadlineNs` (the park timer), `removePending` / `findPending` by id, and `drain` via `drainEmit` / `drainEmitReceipt`. The `forEachPending` callbacks copy ids into `sid_scratch` and act afterward; the order of the walk is irrelevant. The line that looks like an ordering dependency is only "DATA goes through the scheduler":

```4744:4745:src/edge/connection.zig
    /// DATA never goes straight to the wire here. It enters the scheduler, so
    /// that fairness between streams is decided in one place.
```

**[PREMISE]** HTTP/2 priority would be the thing that could make id order or weights load-bearing. P7 says it is out of scope, and this server does not implement it. The premise that would revive reading A is a test, or a caller, that fails when the emit sequence is 5,1,3 instead of 1,3,5. There is no such caller on this tree.

## 2. Design

Pending storage stays a fixed `pending_slots` array of length `max_streams`, `stream_id == 0` still means free, and slabs stay rented from the boot pool. Selection becomes an intrusive ready FIFO. Lookup becomes a fixed open-addressed table. Both are allocated in `init`. Nothing on the write path allocates, and `std.HashMap` stays out: `pendingWriteMapBytes` exists because a map that resizes on insert would break `resourceUpperBound` (`fair_scheduler.zig` line 38, the promise that a connection stays inside that bound).

### What `init` allocates

- `pending_slots`, as today. Each `DataPending` gains `active_next`, `active_prev`, `q_next`, `q_prev` (`u32`), and `place: enum { free, idle, ready, blocked }`. `@sizeOf(DataPending)` grows, and `pendingWriteMapBytes` follows it.
- `sid_index: []IndexEnt` of length `cap`, the smallest power of two that is at least `2 * max_streams`. `IndexEnt` is `{ stream_id: u31, slot: u32 }`. `stream_id == 0` means empty. Zero the array. This replaces `sid_scratch`.
- No third buffer. Ready, blocked, active, and the free list are heads plus the intrusive links. Sentinel is `0xffff_ffff`. `init` rejects `max_streams` above that sentinel.

`limits.zig`'s `sched_scratch` term is `max_streams * @sizeOf(u31)` and exists to count today's scratch. Point it at `cap * @sizeOf(IndexEnt)` instead. Leaving the index out of the bound makes `resourceUpperBound` a lie the moment `init` succeeds.

**[LOGIC]** Heads and a `u64 seen_credit_gen` live in the struct with no allocation. `deinit` still walks `pending_slots` and `returnSlab`s, then frees `sid_index`.

### Index

Home slot is `(@as(u32, stream_id) *% 0x9E3779B1) & (cap - 1)`. Linear probe. Lookup and insert stop at an empty slot. Insert is only used after a miss, so a duplicate id is a bug.

Deletion is backward shift, with no tombstones. Clear the hole, then walk forward. For each occupied entry, if the hole lies on the probe path from that entry's home to its current slot, move the entry into the hole and continue from the vacated slot. Stop at an empty slot. A tombstone would turn a later lookup into a walk of the table. This scheduler's own comment says the interesting connection is a long-lived SSE stream beside one-shot requests, which is exactly the insert/delete churn that grows a tombstone chain.

**[LOGIC]** With load at most one half and no tombstones, a hit is one or two probes in the common case. The worst case is a single cluster, bounded by `active_pending`, over 8-byte entries. Today's `findPending` always walks every `DataPending`, including the free ones.

### Lists

A live slot is on the active list for its whole life. It is on at most one of the ready FIFO and the blocked list.

- Ready is a FIFO. Head is the next emit. A newly eligible stream joins the tail.
- Blocked is the set of streams that have something to send (`len > 0` or `end_stream`) and whose last check saw `stream_win <= 0` while the connection window was positive.
- Idle is an occupied slot with nothing to send (the fresh slot before the first byte sticks, or a slot left allocated after `error.OutOfMemory`). It is on the active list only.

`active_pending` still counts occupied slots, so the framed-head exception below stays meaningful.

Connection-window exhaustion does not move the ready FIFO. `emitOneData` already returns before touching pending streams when `conn_win <= 0` (the check at `fair_scheduler.zig` line 589). `StreamWindow.availableSend` also returns 0 when `conn_send <= 0`, so once that gate has passed, `stream_win <= 0` means the stream window itself. Those two reads are the same `windows.conn_send` on the actor under `session_mu`.

### `enqueueDataBytes`

The ticket-without-a-frame check, the slab rent, the `OutOfMemory` cap, and the `memcpy` stay. `allocPending` is a hash lookup and, on a miss, a pop of the free list. The free-slot scan goes away. A failed first rent still `freePending`s the brand-new empty slot.

After the bytes stick, `arm`: if `place` is `idle` and (`len > 0` or `end_stream`), push the tail of ready. If it is already `ready` or `blocked`, leave it there. A second enqueue of a blocked stream must not insert a second ready node. That duplicate is the bug that serves one stream twice per round.

### `emitOneData`

Framed DATA stays in front of the ring, unchanged, including the `active_pending == 0` exception. Those bytes are already past `Session`:

```568:572:src/edge/fair_scheduler.zig
    /// 2. Already-framed DATA goes first. Those bytes are past `Session`, so
    ///    holding them would reorder frames that the protocol layer has already
    ///    committed to.
    /// 3. Otherwise round-robin the pending slots from `drr_cursor`, and skip
    ///    any stream whose own window is empty.
```

A framed emit does not rotate the ready FIFO. `drr_cursor` is deleted.

Then:

1. `conn_win <= 0` returns false. The ready FIFO stays as it is.
2. `ready` empty returns false. Blocked streams are not visited.
3. Otherwise pop the head and detach it (`place = idle`, still on the active list) before building, so a nested `removePending` during `build_data_frame` is unlinking a slot that is not on the FIFO. The `pw.stream_id == sid` re-check between build and shrink stays; it is what makes that nesting safe (`fair_scheduler.zig` lines 574–578 and 643).
4. `stream_win <= 0`: push the blocked list and pop the next head. This walk only happens for streams that were thought to be ready. Each such stream leaves the FIFO in this call, so the next `emitOneData` does not see it.
5. `len == 0` and `!end_stream`: `freePending` and continue. That is today's reap of a drained SSE slot, still gated on a positive stream window, so a zero-window empty slot remains occupied and `active_pending` does not drop early. The slab is already returned at the previous successful emit (`fair_scheduler.zig` lines 654–661); this pop only drops the slot.
6. Otherwise take the same quantum, `min(len, DATA_QUANTUM, avail)`, build, shrink, attach the flush ticket, and return the slab when `rest == 0` and `!end_stream`, all as today. On `error.FlowBlocked`, push blocked and continue. The scheduler already sizes the slice to `avail`, and `makeDataFrame` returns `FlowBlocked` when it would shorten that slice, so this is the disagreement path (tombstone, or the window moved). Parking it is safe because `checkSlowConsumers` is the backstop if credit never returns, and a real RST removes the slot through `wakeHandlerWaiters`.
7. On success, if the slot is still ours: `end` or a finished `end_stream` calls `freePending`. A partial remainder with `stream_win <= 0` goes to blocked. A partial remainder with window left joins the ready tail. A drained non-final slot (`rest == 0`, `!end_stream`) joins the ready tail so the next pop can reap it, as in step 5.

The walk cannot spin. This call only pushes to blocked or returns on a successful sink. It never pushes ready except after a success, and that return ends the call.

`dataEligible` is the third scan today (`fair_scheduler.zig` lines 491–494) and it runs from the ordinary-control yield. It becomes `conn_win > 0 and (framed_len > 0 or ready_len > 0)`. A ready head whose window just died makes one forced `emitOneData` that parks it; `shouldForceDataNow` then sees an empty ready FIFO and the ordinary loop continues. That preserves the comment that a window-blocked stream must not spin the control loop (`fair_scheduler.zig` lines 509–511).

`drain` calls `rearm` once, then the existing terminal loop, ordinary loop, and DATA loop. `emitOneDataPublic` rearms once before the single emit so unit tests that credit between calls keep working. Rearm does not run per frame inside the loop. Windows only shrink during `drain`: `makeDataFrame` debits, and the actor holds `session_mu`, so ingest cannot apply a `WINDOW_UPDATE` under the build.

### Getting a blocked stream back without a scan

**[LOGIC]** An unconditional walk of the blocked list on every `drain` is the design that loses. `sched.pause` on TLS is `pending_n > 0 or stashFull()` (`connection.zig` lines 3194–3198), so `drain` often returns after one DATA frame. `streamSendAvailable` on a map miss scans `tombstones`. Probing every blocked stream on that cadence is heavier than sorting a dense `u31` scratch, which is the case where this work would be slower than today.

Rearm is therefore per stream, with a broadcast only when one stream id is not enough.

On `Session`, two plain fields, no allocation: `pending_credit_sid: u31 = 0` and `pending_credit_broadcast: bool = false`. Set them only where send credit actually grows:

- `StreamWindow.applySendUpdate` succeeds for stream `sid`. If a broadcast is already set, leave it. If `pending_credit_sid` is 0 or already `sid`, store `sid`. A second distinct id sets `pending_credit_broadcast`.
- `applyInitialWindowDelta` when `delta > 0` (the `SETTINGS_INITIAL_WINDOW_SIZE` arm in `onSettings`). That retunes every open stream, so it is a broadcast.
- `applyConnWindowUpdate` sets neither flag. Connection credit does not move the ready FIFO, so there is nothing to repair.

`drainEmit` and `drainEmitReceipt` both, before `sched.drain`: if the broadcast flag is set, `rearmAllBlocked`; else if `pending_credit_sid != 0`, `rearmStream` for that id; then clear both fields. A path that drains without this step leaves a newly credited stream on the blocked list until `checkSlowConsumers`, which is the same class of stall as a RST tombstone that `emitOneData` skips forever (`connection.zig` lines 3662–3663).

`rearmStream` is one hash lookup. `blocked` and `stream_win > 0` moves that node to the ready tail. Already `ready` is a no-op. A credited id that is idle, with `len == 0` and no `end_stream`, is `freePending` (the zero-window zombie whose window just opened). `rearmAllBlocked` walks only the blocked list, moves those with `stream_win > 0` and bytes or `end_stream` to the ready tail, and reaps idle zombies whose window is now positive. It does not walk `pending_slots` and it does not walk the ready FIFO. A RST stays blocked because `streamSendAvailable` returns 0 for a non-`no_error` tombstone, and `onWindowUpdate` does not call `applySendUpdate` for a stream that is already gone.

### The other four functions

`takePending` and `removePending` are a hash lookup plus `freePending`. `freePending` unlinks active, unlinks ready or blocked if `place` says so, backward-shifts the index, `returnSlab`s, `clear`s, and pushes the free list. A detached slot (`place == idle` because emit already popped it) is not on either selection list; unlink must tolerate that. `takePending`'s returned copy keeps the bytes and the ticket fields and zeroes the link fields. Nothing in-tree calls `takePending` today; the function still has to leave the lists and the index consistent.

`forEachPending` walks the active list. The four callers (deinit release, writer-fail, slow-consumer, shutdown sweep) already copy ids out and mutate afterward, which is required once the walk is intrusive. Idle occupied slots stay on the active list, so a slot that rented a slab and then failed the length check is still dropped on writer-fail and still returned in `deinit`.

`nextSlowDeadlineNs` walks the active list with the same minimum. It is the park timer (`Connection.nextDeadlineNs`), not the emit path. A heap here would be a second scheduler. Free slots drop out of the walk because they are not on the active list; the deadline for a given occupied slot is unchanged.

Direct `pending_slots` walks in `connection.zig` (deinit leak snapshot, the park diagnostic, the overflow diagnostic) keep working, because the array and the `stream_id == 0` sentinel stay.

Pending-versus-wire accounting is untouched. The sink still releases pending bytes only after it accepts the frame (`connection.zig` lines 82–86 and the `pending_data_outbound_release` handoff in `drainEmit`). Every emit still goes through that sink.

## 3. Invariants, and the test

These are the ones the code has to keep, and the ones a mutation breaks:

- A slot's `place` matches the list it is on. Ready and blocked are disjoint. A stream id is in the index exactly when the slot is occupied.
- The ready FIFO has no duplicates. Push happens from `arm` only when `place` was `idle`, from the success path of `emitOneData`, and from rearm of a blocked node.
- `emitOneData` does not read the blocked list. `conn_win <= 0` does not pop ready.
- `freePending` is the only unlink, the only index delete, and the only `active_pending` decrement. `returnSlab` stays inside it and inside the existing drain-empty path.
- After a successful quantum, a stream that still has bytes and window joins the tail. Every stream that was ahead of it on the FIFO is popped before it is popped again.
- `rearmAllBlocked` runs for a positive initial-window delta, and `rearmStream` runs for a stream `WINDOW_UPDATE`, on both drain entry points, before any `emitOneData`.
- Index delete leaves no tombstone. Lookup of a key that sat past the hole still hits.

**Fairness test.** Enqueue `DATA_QUANTUM` bytes on stream 1, twice, then one byte on stream 3. Both windows are large. Three `emitOneDataPublic` calls. The `build` callback records sids. The sequence is `1, 3, 1`.

The mutation is: after a quantum, insert the served stream at the head instead of the tail. Stream 1 then wins every following quantum and the sequence is `1, 1, 3`. Restarting each emit at the lowest pending id, which is what deleting the rotation does, fails the same assertion. That is the bug the comment at lines 28–30 names.

**Rearm test, separate mutation.** Enqueue bytes on stream 1 with `stream_win` returning 0, call `emitOneDataPublic` once, and assert the slot is blocked and no frame was sunk. Flip the window to positive, call the same `rearmStream` that `drainEmit` calls after `applySendUpdate`, emit again, and assert the frame is sunk. The mutation is deleting that `rearmStream` call. The second emit returns false and the bytes sit until slow-consumer.

**Index test, separate mutation.** Insert two ids that share a home slot, delete the one in the home slot, look up the other. The mutation is clearing the hole and returning without the backward shift. Lookup stops at the empty slot, `enqueueDataBytes` allocates a second slot for a stream that already has bytes, and the first slab is orphaned.

## 4. Where this is worse than today

**[PREMISE]** Time-to-first-byte for a stream that today sits just above `drr_cursor`. On this build that stream is the next DATA frame. On the ring it joins the tail and waits out every eligible stream already on the FIFO, each of which may hold a full 16 KiB quantum. With `n` eligible streams that is up to `(n - 1) * 16 KiB` of other streams' bytes before a one-byte response that the cursor would have sent next. The measured run hides it: 1-byte bodies, so a round is `n` tiny frames and the CPU cost of the sort dominates. The premise that makes this bite is full quanta, several eligible streams, and a new stream whose id is the cursor's next hit. A high id that the cursor has already passed gets better latency from the same change. The h2c profile in the brief (blocked set empty, 1-byte bodies) is the shape the ring speeds up.

**[PREMISE]** `enqueueDataBytes` on a stream that occupies `pending_slots[0]` and is alone. `findPending` hits on the first comparison. A multiplicative probe is a couple of arithmetic instructions and a load. Emit still walks every slot today, including the free ones, so a one-stream connection still gets cheaper at `emitOneData`. This premise is the opposite of the measured `-m 100` run, where the scan is long.

**[LOGIC]** `rearmAllBlocked` is the remaining walk, and it is real. A positive `SETTINGS_INITIAL_WINDOW_SIZE`, or two distinct stream `WINDOW_UPDATE`s in one ingest, probes `streamSendAvailable` once per blocked stream. That is the right work for a broadcast, and it is rare next to per-frame sorting. It becomes the old per-frame cost again only if a later change rearms on every `drain` or treats a connection `WINDOW_UPDATE` as a broadcast. Connection credit must not do that: the ready FIFO was never dismantled. Doing it anyway, under the TLS pause above, with a large blocked set of RST tombstones, is the CPU regression. Each probe that misses the stream map scans the tombstone ring, which is more than sorting 256 `u31`s.

Framed DATA (`enqueueFramedData`) is outside this design on purpose. The profile's hot lines are the pending collect and the sort, so the measured bytes are the slab path. A workload that frames in `Session` before the scheduler still pays the framed FIFO and never the ring.
