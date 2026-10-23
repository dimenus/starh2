## 1. [LOGIC] Pick B: round-robin fairness

The source defines fairness by turns, not by numeric stream ID:

```22:30:src/edge/fair_scheduler.zig
//! 3. DATA — round robin over streams in a fixed 16 KiB quantum.
//!
//! The strict tier order would let controls starve DATA forever, so tier 2
//! yields after `CONTROL_BEFORE_DATA` frames whenever DATA is eligible.
//!
//! Within tier 3, `drr_cursor` is what makes the round robin fair. It resumes
//! at the stream after the one last served, so a low-numbered stream cannot win
//! every pass. A fixed quantum caps how long any one stream holds the wire.
```

That says what matters: advancing after the served stream so it cannot win twice before peers get a turn. Sorting is an implementation mechanism, not the contract.

No test pins ascending stream-ID order. The only direct two-stream scheduler test makes stream 1 ineligible, stream 3 eligible, and merely checks that something progressed:

```188:206:tests/scheduler.zig
try s2.enqueueDataBytes(1, &@as([100]u8, @splat(1)), false, 0, 0);
try s2.enqueueDataBytes(3, &@as([100]u8, @splat(3)), true, 0, 0);
// ...
const progressed = try s2.emitOneDataPublic(/* ... */);
try std.testing.expect(progressed);
```

`connection.zig` also assigns no meaning to which pending stream wins. It delegates that decision to the scheduler:

```4743:4745:src/edge/connection.zig
/// DATA never goes straight to the wire here. It enters the scheduler, so
/// that fairness between streams is decided in one place.
```

The selected ID is subsequently used only to query/debit that stream and wake its producer; it is never compared numerically.

There is one existing qualification: already-framed DATA remains FIFO ahead of pending slabs because it has passed protocol commitment:

```567:572:src/edge/fair_scheduler.zig
/// 2. Already-framed DATA goes first. Those bytes are past `Session`, so
///    holding them would reorder frames that the protocol layer has already
///    committed to.
/// 3. Otherwise round-robin the pending slots from `drr_cursor`, and skip
///    any stream whose own window is empty.
```

Thus B applies to pending DATA after that existing FIFO exception.

## 2. [LOGIC] Fixed index plus intrusive ready ring

### Boot allocation

For configured capacity `N`, `init` should allocate:

- `pending_slots[N]`, as today.
- `free_slots[N]`, initially containing every slot index.
- `active_slots[N]`, a dense list for iteration.
- A fixed open-addressed `stream_id -> slot_index` table with capacity at least `2N`, using backward-shift deletion so tombstones cannot accumulate.
- Intrusive scheduling links in each `DataPending`: `next`, `prev`, and state `{ready, blocked, selected}`.

The ready and blocked rings can share links because a slot belongs to at most one. Delete `sid_scratch` and `drr_cursor`. All allocations must be included in `Limits.resourceUpperBound`; no hash growth API may be reachable after boot.

### Window handling

A stream-specific blocked slot must not remain in the ready ring:

- If the connection window is zero, leave ready ordering untouched and return immediately. A connection WINDOW_UPDATE wakes the whole existing ring.
- If the connection window is positive but the stream window is zero, move that slot to the blocked ring.
- A successful stream WINDOW_UPDATE promotes exactly that stream from blocked to the ready tail.
- A SETTINGS initial-window change reclassifies the blocked/ready rings once because it can affect every stream.

This requires an exact, nonallocating send-credit notification from `Session.onWindowUpdate` to `Connection`, naturally as another optional `StreamHooks` callback. Without that notification, an “id map plus ready ring” is incomplete: either blocked streams never wake, or every actor turn must rescan them.

`enqueuePending`, already under `session_mu`, should pass the current classification into `enqueueDataBytes`. This avoids first placing a known-zero-window stream in the ready ring.

### Method changes

- `findPending`, `contains`, `pendingByteLen`: hash lookup, expected O(1).
- `allocPending`: hash lookup; on miss pop `free_slots`, insert the hash entry, and append the index to `active_slots`. No slot scan.
- `enqueueDataBytes`: preserve slab, ticket, timestamp, and error behavior. When the entry first has a legal frame (`len > 0 || end_stream`), append it to ready or blocked according to current credit. A zero-byte, nonfinal, unticketed enqueue can be a no-op rather than creating an unusable entry.
- `dataEligible`: connection credit plus `framed_len != 0 || ready_head != none`; no pending scan.
- `emitOneData`:
  1. Preserve connection-window gating and framed-DATA priority.
  2. Pop the ready head and mark it selected.
  3. Emit at most `DATA_QUANTUM`.
  4. Revalidate its ID after `build_data_frame`, preserving the existing nested-cancellation protection.
  5. If bytes remain and stream credit remains, append it to the ready tail.
  6. If only stream credit is exhausted, move it to blocked.
  7. If drained, remove it immediately.
  
  The normal selection path is O(1) and never visits blocked streams.
- `takePending`: hash lookup, copy the snapshot, unlink from either ring, hash-delete, dense-active swap-delete, return slab, and push the index onto the free stack.
- `removePending`: same removal path without returning the snapshot.
- `forEachPending`: iterate `active_slots[0..active_pending]`; callbacks may mutate the entry but not remove it during iteration. Existing callers already collect IDs before removal.
- `nextSlowDeadlineNs`: scan only `active_slots`, not all configured slots. I would not add a heap: this query occurs when choosing the actor’s next deadline, while heap maintenance would add O(log N) work to the measured enqueue and emit paths.

Pending-versus-wire accounting and the sink remain unchanged. Selection must not release accounting itself: `buildData` records the pending release, the scheduler sink applies it after accepting the frame, and WritePump completion releases wire occupancy.

## 3. [LOGIC] Invariants and fairness test

Required invariants:

1. Hash entries, active indices, and live `pending_slots` are a bijection.
2. `active_pending + free_count == N`.
3. Every legal pending frame is in exactly one state: ready, blocked, or temporarily selected.
4. A slot appears at most once in either ring.
5. Only the ready head may be selected; a served stream with remaining eligible data goes to the tail.
6. Connection-window exhaustion does not disturb ring order.
7. Stream-window-blocked entries are revisited only by their credit notification, a global SETTINGS reclassification, or removal.
8. Every slab return and accounting release still occurs exactly once.

I would add a test with three streams enqueued in non-ID order, each holding at least three quanta. Record the selected IDs for nine emissions and assert that every consecutive group of three contains each stream exactly once. Do not assert their numeric ordering.

That test catches the mutation “requeue the served slot at the head” or equivalently “fail to advance the ready head”: the hot stream then appears twice before all peers receive one turn.

I would also add a query-count canary with many blocked streams and one ready stream, asserting one emission does not call `stream_win` for the blocked set.

## 4. What this understates

[PREMISE: most connections have only zero or one pending stream.] The new design can be slower and materially larger per connection in that workload. Today, the sole entry is commonly in the first slot and sorting one ID is trivial; the replacement adds hashing, ring maintenance, active/free indexes, and several fixed kilobytes of boot memory. The measured high-concurrency workload may win substantially while ordinary single-stream requests regress slightly.

[LOGIC] A fixed open-addressed hash also has an O(N) collision worst case, unlike the claimed expected O(1). A per-connection salt and load factor at or below 0.5 make that unlikely, but if deterministic worst-case bounds are required, the index should instead be a fixed-capacity Patricia trie.
