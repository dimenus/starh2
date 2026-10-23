**[LOGIC] B is the supported contract:** the scheduler documents round-robin fairness, and the tests do not pin ascending IDs. The main design issue is eligibility: a ready ring needs explicit window-change notifications, or removing the sort will leave blocked-stream scans in place.


## 1. **[LOGIC] Choose B: round-robin fairness, not ascending IDs**

The stated contract is `src/edge/fair_scheduler.zig:22`:

> `//! 3. DATA — round robin over streams in a fixed 16 KiB quantum.`

Lines 28–30 explain the purpose: prevent a low-numbered stream from winning every pass. Ascending IDs are the current mechanism, not a separately stated requirement.

**No test in either requested file pins ascending DATA order.** In `tests/scheduler.zig:11`, the main sink records only:

> `emitted: std.ArrayList(struct { control: bool, n: usize })`

It cannot assert inter-stream ordering. The two-stream scheduler case at lines 182–206 blocks stream 1 and checks that something progresses; it does not compare two eligible streams. The module-local tests cover ownership, capacity, tickets, and control classification—not multi-stream round-robin fairness.

The relevant `connection.zig` consumers are order-independent between streams:

- Both drain sinks associate accounting and capacity wakes with the selected stream, not its numeric position: `pending_data_wake_sid = stream_id` at lines 4417 and 4621.
- Flush tickets follow their stream’s outstanding bytes; batch ticket chains follow actual emission order.
- Complete-handler receipts attach to the drain’s final output, not the highest stream ID.
- Cancellation, slow-consumer sweeps, and shutdown enumerate or look up pending streams; none uses `drr_cursor` or assumes sorted emission.

**[LOGIC] Define B precisely.** For a stable, continuously eligible set, each stream gets one turn per cycle. Newly eligible streams join the tail; they do not acquire retroactive priority over turns already queued. An arrival-order ring does not satisfy a literal global “first turn before anyone’s second” rule when membership changes midway through a cycle.

Also retain the existing **already-framed DATA exception**: `emitOneData` gives its FIFO precedence at lines 591–602. Therefore, neither today’s implementation nor this replacement promises unconditional round-robin fairness across *all* framed and pending DATA.

## 2. **[LOGIC] Use stable slots, an indexed lookup, and an eligibility-aware ready ring**

Let `M` be the configured maximum pending streams and `P` the current pending count.

### Storage and initialization

Allocate at boot:

- `M` stable pending slots, retaining the existing slab ownership model.
- Intrusive slot-index links for a free list, an all-pending list, and a doubly linked ready ring.
- A fixed open-addressed `stream_id → slot_index` table, capacity rounded up to a power of two at least `2*M`.
- Per-slot membership state and a generation counter.

Use backward-shift hash deletion: move index entries, never pending slots. This avoids accumulating deletion tombstones during one-byte-response churn. That issue is already documented for `Session.streams` at `src/core/session.zig:156–161`.

Remove the scheduler’s `sid_scratch` and `drr_cursor`. Keep **Connection’s separate `sid_scratch`**, which cancellation and teardown still need.

Lookup is expected O(1), not a worst-case constant-time guarantee. Free-slot allocation and ready-ring operations are O(1). All capacity arithmetic must be checked at initialization, and `bound_shapes.zig`/`Limits.resourceUpperBound` must count the actual new storage.

### Eligibility must be maintained, not rediscovered

**[LOGIC] A ring containing every pending stream is insufficient.** It merely replaces the sort with repeated blocked-stream traversal. Likewise, changing `emitOneData` alone leaves the scan in `dataEligible` at lines 491–494.

Maintain ready membership from **stream-local eligibility**, with connection credit as a separate global gate:

- Pending work exists: `len > 0 || end_stream`.
- The stream is permitted to send and has positive stream-local credit.
- Connection credit is checked separately before selecting DATA.

This requires splitting the current query contract. `Session.streamSendAvailable` already includes connection credit (`session.zig:1630–1638`). Using that combined result to remove streams from the ring would strand them—or require rescanning everything—when connection credit returns.

Expose a Session-authoritative query for stream-local credit/status. Preserve the tested distinction between a normally removed stream and an RST tombstone; do not infer eligibility from `Session.streams.contains`.

**[LOGIC] Window-change plumbing is part of this design, not optional follow-up.** Add targeted invalidation for successful stream WINDOW_UPDATE, other stream-credit mutations, and relevant stream removal/reset transitions. SETTINGS_INITIAL_WINDOW_SIZE invalidates all pending classifications, including when credit becomes negative.

One concrete allocation-free transport is a Session-owned, boot-sized journal of `M` changed IDs plus an `all_changed` bit. Overflow coalesces into `all_changed`, rather than dropping notifications or allocating. Connection consumes it before either drain entry point. Selected-stream debits are refreshed directly after building; any other mutation during a drain must be synchronized before its next selection.

- A targeted update reclassifies that ID through the index.
- An all-stream update visits the all-pending list once. Preserve the relative order of still-ready entries; append newly ready entries.
- Connection WINDOW_UPDATE changes only the global gate; it does **not** walk blocked streams.

This permits O(P) work for genuinely bulk changes or journal overflow, **not per DATA selection**. Marking everything dirty on every ingest would defeat the design.

### Operation changes

| Operation | Replacement |
|---|---|
| `enqueueDataBytes` | Index lookup; obtain a free slot if absent. Preserve ticket validation, slab limits, and explicit exhaustion errors. Commit index/list membership only with successful state changes, rolling back newly acquired resources on failure. Append newly ready work to the tail; appending bytes or a ticket to an already-ready stream must not reposition or duplicate it. |
| `emitOneData` | Preserve the connection gate and already-framed FIFO branch. Otherwise select the ready head directly. Build at most `min(len, DATA_QUANTUM, stream_credit, connection_credit)`, preserving zero-length END_STREAM behavior. After successful build, update pending bytes/tickets and move remaining eligible work to the tail; unlink exhausted-credit work. Free fully drained entries and return their slabs. |
| `takePending` | Indexed lookup followed by the common removal operation. Return a **metadata-only removal snapshot**, not a slab slice or intrusive links. The current implementation returns its slab to the pool before returning the snapshot (`fair_scheduler.zig:725–732`), so returned bytes are not owned by the caller. |
| `removePending` | Erase the index entry, unlink ready/all-pending membership, return the slab exactly once, decrement the count, and return the slot to the free list. |
| `forEachPending` | Walk the all-pending list, including blocked entries: O(P). Keep structural removal outside callbacks, matching Connection’s existing collect-then-remove pattern. |
| `nextSlowDeadlineNs` | Walk that same list and retain the current timeout-zero, empty-body, and zero-timestamp exclusions. Still O(P), deliberately: this is deadline maintenance, not selection. Blocked streams must remain visible. |

`dataEligible` becomes an O(1) test of connection credit and ready/framed availability, preserving the existing control-yield behavior.

**[LOGIC] Do not cache a selected pointer’s identity across the builder without validation.** The existing safeguard is:

> `if (pw.stream_id == sid) {` — `fair_scheduler.zig:640`

The surrounding contract permits cancellation during construction. Capture `(slot_index, stream_id, generation)` and revalidate before shrinking, attaching tickets, or rotating. Never reuse saved links after the callback.

A `FlowBlocked` result must trigger reason-specific reclassification without consuming a turn. Connection exhaustion preserves ring order; stream exhaustion removes that entry until notification. A builder reporting blockage despite current positive credit is a contract error to surface—not a reason to spin or repeatedly scan.

## 3. **[LOGIC] Invariants and the missing fairness gate**

The essential invariants are:

- Every occupied slot has exactly one index entry and all-pending membership; every free slot belongs only to the free list.
- A pending stream appears **at most once** in the ready ring. Before selection, membership accurately reflects stream-local eligibility.
- Append and repeated positive-window notifications do not move an already-ready stream. Successful service rotates it; reactivation joins the tail.
- Global connection blockage and transport pauses preserve the next-stream position.
- Removal invalidates generation-sensitive references and releases each slab once.
- Session remains the credit authority. Building precedes pending mutation; the sink still owns acceptance and fail-closed behavior.

In particular, preserve the existing acceptance boundary:

> `c.applyOutboundRelease(c.pending_data_outbound_release, .pending);`  
> — `connection.zig:4396`

Do not move that release into slot removal or ring rotation. Framed-wire occupancy still releases on write completion, and every frame still traverses the scheduler sink.

**Add `continuously ready streams cannot be overtaken twice`.** Queue several quanta each for IDs in deliberately nonsorted order, such as `[11, 1, 9, 3]`, with ample windows. Record actual emitted stream IDs. Pause and resume across multiple drain calls, and append more bytes to an already-ready stream between calls.

Assert that every consecutive four pending-DATA selections contain all four IDs exactly once while all remain eligible. Do **not** assert ascending order.

This catches the mutations **“leave the served stream at the head,” “restart selection on each drain,”** and **“enqueue a duplicate ready-ring entry on append.”**

**[LOGIC] Add transition coverage separately.** Block most streams, verify one eligible stream progresses without per-emission queries of blocked IDs, then grant one blocked stream credit without enqueueing more bytes. It must become runnable exactly once. Repeat for connection-window exhaustion/restoration, SETTINGS decreases/increases, reset, slot reuse, and empty END_STREAM. This catches **“omit reactivation notification”** and **“mistake connection blockage for stream blockage.”**

## 4. What this design understates

**[LOGIC] This is larger than replacing `sort`.** Accurate eligibility adds a protocol-to-scheduler notification contract and more lifecycle invariants. It also adds fixed per-connection metadata. The all-pending deadline/slow-consumer walks remain; calling the entire scheduler “scan-free” would be inaccurate.

**[PREMISE] Very small configured stream capacity can favor today’s implementation.** If `M` is one or two and enqueue repeatedly hits the first slot, the current lookup and tiny sort are cheap. Hashing, membership bookkeeping, and generation checks can cost more. No throughput improvement is established for that workload.

**[PREMISE] Update-heavy, output-light traffic can pay more maintenance.** If a peer repeatedly changes windows while few DATA frames are emitted, readiness maintenance—and especially bulk reclassification after journal overflow—may cost more than querying lazily during a drain. Coalescing helps, but does not eliminate the tradeoff.

**[PREMISE] Poor hash distribution can defeat expected O(1).** If stream IDs cluster under the chosen hash, probing and backward-shift deletion can become O(P). Backward-shift deletion fixes tombstone accumulation, not collisions. Measure long-lived sparse-ID churn as well as the supplied short-body workload.

**[PREMISE] A newly ready stream can wait longer than under ID order.** If its ID would have fallen immediately after today’s cursor, tail insertion instead places it behind existing ready work. That is an intentional latency-order change under B, not a fairness failure.## 1. **[LOGIC] Choose B: round-robin fairness, not ascending IDs**

The stated contract is explicit in `src/edge/fair_scheduler.zig:22`:

> `//! 3. DATA — round robin over streams in a fixed 16 KiB quantum.`

Lines 28–30 explain the cursor’s purpose:

> `//! at the stream after the one last served, so a low-numbered stream cannot win`  
> `//! every pass.`

That specifies avoiding repeated service of one stream, not preserving numerical order.

**[LOGIC] No reviewed test pins ascending-ID DATA order.** In `tests/scheduler.zig`, the sink records only `{ control: bool, n: usize }`, not stream IDs. The two-stream scheduler case at lines 186–206 blocks stream 1 and merely asserts that emission progresses. The other relevant tests cover control gaps, window blocking, missing Session entries, reset tombstones, and pausing. The module-local tests cover ownership, capacity, tickets, and slab reclamation. None demonstrates fairness between two continuously eligible streams.

**[LOGIC] Connection depends on per-stream ordering and actual emission order—not numerical stream order.** Both sinks release pending accounting and wake the stream identified by the builder:

> `c.pending_data_wake_sid = stream_id;`  
> — `src/edge/connection.zig:4417`

Ticket chains follow emitted frames; the complete-handler receipt attaches to the drain’s final output. HEADERS-before-DATA ordering is maintained separately. Cancellation, capacity waits, slow-consumer sweeps, and shutdown address streams by ID or aggregate pending state. None reads `drr_cursor` or predicts the next numerical ID.

**[LOGIC] State B precisely.** For a stable set of continuously eligible pending streams, each gets one turn per cycle. New or unblocked streams join the tail; they do not retroactively acquire a turn in an already-started cycle. Also, today’s already-framed FIFO has precedence over pending DATA (`fair_scheduler.zig:591–602`). Retaining that behavior means the fairness guarantee applies to pending-DATA selection, not unconditionally to every DATA frame in tier 3.

## 2. **[LOGIC] Proposed design: indexed slots plus an eligible-only ring**

Let `M` be the configured stream capacity and `P` the current pending count.

### Storage and initialization

**[LOGIC] Keep stable pending slots; index them rather than moving them.** Allocate at initialization:

- `M` pending slots, including a generation counter, free-list link, active-list links, ready-ring links, and membership state.
- A fixed open-addressed `stream_id → slot_index` table, with power-of-two capacity at least `2*M`.
- Free-list, active-list, and ready-ring heads/counts stored inline.

Use full-ID comparisons and backward-shift deletion, so stream churn cannot accumulate hash tombstones. Hash operations are **expected** O(1), not worst-case O(1). Only table entries move during deletion; pending slots and their slabs remain stable.

Remove scheduler `sid_scratch` and `drr_cursor`. Retain Connection’s separate scratch array for teardown collections. Keep existing slab leasing and release behavior; the new structures allocate no payload storage.

Account for the table and expanded slot shape in `bound_shapes.pendingWriteMapBytes` and `Limits.resourceUpperBound`; remove the obsolete scheduler scratch term.

### Eligibility must become explicit

**[LOGIC] A ring containing every pending stream merely replaces sorting with blocked-stream scanning.** Maintain:

- An **active list** containing every pending stream.
- A **ready ring** containing only streams with a frame to send and positive **stream-local** credit.
- One separate connection-window gate.

This requires a query/API change. Today `Session.streamSendAvailable` includes connection credit: `session.zig:1632` returns `s.window.availableSend(self.windows.conn_send)`. Using that value to classify individual streams would strand them when the connection window closes.

Expose stream-local DATA credit separately, preserving current Session semantics: live streams use their signed stream window; a missing stream with no reset tombstone remains sendable; a reset tombstone is ineligible. Connection credit remains independently authoritative. Preserve the scheduler’s existing window gating for empty END_STREAM frames rather than changing that policy incidentally.

**[LOGIC] Readiness needs notifications from Session, not polling.** Wire these transitions:

| Event | Scheduler action |
|---|---|
| First pending DATA or empty END_STREAM | Classify and append to ready tail if eligible. |
| Append to an already-ready stream | Leave its position unchanged. |
| Successful frame construction | Reclassify that stream after its debit; rotate remaining eligible work to the tail. |
| Stream WINDOW_UPDATE | Look up and reclassify only that stream. |
| Connection window exhausted/restored | Close/open the global gate; do not empty or rebuild the ring. |
| INITIAL_WINDOW_SIZE change | Reclassify active pending streams once, including negative-window transitions. |
| Normal Session removal | Recheck any remaining pending entry using the missing-stream semantics. |
| Reset/cancellation | Remove pending state through the existing cleanup path. |

A concrete, allocation-free notification mechanism is a Session-owned journal of `M` changed IDs plus an `all_changed` flag. Accepted stream updates append IDs; INITIAL_WINDOW_SIZE sets the flag. Journal overflow coalesces to `all_changed`, preserving correctness without allocating or dropping information. Connection consumes it before either drain entry point. Bulk refresh preserves the relative order of streams that remain ready; newly eligible streams join the tail.

Direct scheduler framing refreshes its selected stream immediately, rather than journaling its own debit. Any credit-changing operation during a drain must likewise be reconciled before another selection.

This costs O(number of recorded changes), or one O(P) pass for a genuine bulk invalidation. It is **not** a blocked-stream walk on each DATA emission.

### Method changes

**[LOGIC] All lookup and selection surfaces must change together.**

| Method | Replacement |
|---|---|
| `enqueueDataBytes` | One indexed lookup; on a miss, pop a free slot instead of scanning. Preserve slab bounds, ticket validation, and failure cleanup. Publish new membership only after successful admission. Existing ready entries never move merely because more bytes arrive. |
| `emitOneData` | Preserve connection gating and existing framed-FIFO precedence. Otherwise select the ready head directly. Build at most `min(len, 16 KiB, stream_credit, connection_credit)` bytes, update pending state, and rotate remaining eligible work to the tail. Remove blocked or drained entries from the ready ring. |
| `takePending` | Indexed lookup; copy logical cleanup metadata, then unlink and release through the common removal helper. Return a metadata-only result, not a slab-bearing `DataPending`. |
| `removePending` | Indexed lookup, unlink from ready/active structures, delete the index entry, release the slab once, decrement pending count, and return the slot to the free list. |
| `forEachPending` | Walk the active list in O(P), including blocked streams. Keep the existing collect-then-remove calling pattern; callbacks must not structurally mutate the traversal. |
| `nextSlowDeadlineNs` | Walk the active list in O(P), retaining the current `len != 0`, nonzero timestamp, and timeout semantics. Blocked streams must remain included. |

**[LOGIC] `dataEligible` must also change.** Its current loop at `fair_scheduler.zig:491–494` would retain a pending-slot scan even after replacing `emitOneData`. Replace it with the connection gate, ready-ring nonemptiness, and the existing framed-head eligibility rule.

**[LOGIC] Keep deadline enumeration explicitly out of the constant-time claim.** `forEachPending` inherently enumerates work, and the actor’s slow-consumer sweep already does so. An active-list deadline scan is the simplest implementation; adding a deadline heap would add maintenance to every enqueue and quantum without evidence that this is the next bottleneck.

**[LOGIC] Tighten `takePending`’s ownership contract.** Currently it returns a copied slab pointer after `freePending` has returned that slab to the shared pool (`fair_scheduler.zig:725–732`). There are no callers in the searched source/tests. A metadata-only result makes the actual cleanup contract explicit and avoids exposing reusable storage.

## 3. **[LOGIC] Invariants and mutation-catching tests**

**[LOGIC] Membership and lifetime invariants:**

- Every occupied slot has exactly one index entry and one active-list membership.
- Every eligible pending stream appears exactly once in the ready ring; blocked streams appear zero times.
- Connection-window exhaustion changes neither membership nor next-turn position.
- Successful service moves remaining eligible work behind existing waiters. Enqueues and redundant credit notifications do not reorder existing ready entries.
- Empty non-final entries without a ticket are not schedulable work; reclaim them rather than depending on a later selection scan.
- Every removal uses one helper that updates all structures and returns ownership exactly once.

**[LOGIC] Preserve callback invalidation safety.** The existing scheduler explicitly checks:

> `if (pw.stream_id == sid) {`  
> — `src/edge/fair_scheduler.zig:641`

Its preceding comment explains that building can invalidate pending state. Capture `(slot_index, stream_id, generation)` before calling the builder and validate afterward. Do not retain list links across the callback or rotate a reused slot.

A `FlowBlocked` result consumes no fairness turn. Reconcile the selected entry with authoritative credit: remove it from ready if stream-blocked, or stop at the global connection gate. If the builder reports blocking while all authoritative eligibility conditions still hold, surface that contract violation rather than repeatedly selecting the same head or silently parking it forever.

**[LOGIC] Preserve the existing commit boundary.** Frame construction debits Session; pending accounting releases only when the sink accepts output; wire occupancy releases on completion. Keep both Connection sinks’ fail-closed handling and ticket attachment at `flush_remain` exhaustion. Ring membership changes must not move any accounting release earlier.

### Fairness test

**[LOGIC] Add `continuously eligible streams cannot receive a second turn before their peers`.**

Enqueue scrambled IDs such as `9, 1, 7`, each with at least four quanta and ample credit. Record stream IDs and payload sizes from emitted frames—not just DATA/control kinds. Across repeated single-frame emissions, assert:

- Every consecutive three-frame window contains all three IDs.
- At every prefix, service counts differ by at most one.
- No payload exceeds the quantum.
- Pause/resume between emissions preserves those properties.

Do not assert a particular numerical order. The mutation **“keep the served stream at the head instead of moving it to the tail”** must fail immediately. Run a variant with additional writes to already-ready streams to catch duplicate insertion or enqueue-induced reordering.

**[LOGIC] Add transition and work-count variants.** Fill nearly all slots with window-blocked streams and leave one ready. Instrument eligibility-query counts: repeated selection must not query every blocked stream. Then grant one blocked stream credit and require bounded service without another application enqueue. Exhaust and restore connection credit separately, and exercise SETTINGS reductions/increases while preserving existing waiters’ order.

These catch **“put blocked entries back on the ready ring,” “omit WINDOW_UPDATE activation,”** and **“rebuild/reset the ring whenever connection credit returns.”** Retain the existing missing-Session and RST-tombstone tests; a ring tied only to live Session entries would fail the former.

## 4. What this design understates

**[LOGIC] The cost moves; it does not disappear.** More metadata is reserved per connection, and enqueue/removal now maintain an index and multiple memberships. Bulk window changes and slow-consumer maintenance remain O(P). Byte copying, slab compaction, and Session’s own lookup costs remain outside the O(1) ready-head selection claim.

**[PREMISE] Small configured fan-out can be worse.** If `M` is one or a few streams, or repeated appends hit the first existing slot, today’s lookup is extremely cheap and sorting is trivial. Hashing and extra metadata writes can cost more. That workload must be measured separately from the supplied high-concurrency profile.

**[PREMISE] Collision-heavy IDs can make the hash index worse.** If IDs cluster under the chosen hash, lookup and backward-shift deletion approach O(P). Avoid advertising guaranteed constant-time lookup; include collision/churn cases in evaluation.

**[PREMISE] Frequent window reclassification can dominate.** If a peer repeatedly changes INITIAL_WINDOW_SIZE or sends enough updates to overflow the journal between drains, the new design repeatedly maintains readiness even when little DATA is emitted. Preserve survivor order, coalesce updates, and measure this shape rather than extrapolating solely from one-byte responses.

