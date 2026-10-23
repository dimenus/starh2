# Fable (Anthropic, read-only subagent) — answer to tools/fair-scheduler-brief.md at 039c978

Read in full: `src/edge/fair_scheduler.zig`, `tests/scheduler.zig`; every `sched.` call site in `src/edge/connection.zig`, plus `Session.streamSendAvailable` / `onWindowUpdate` / SETTINGS `initial_window_size` in `src/core/session.zig` and `resourceUpperBound` in `src/core/limits.zig`.

## 1. Pick: B (round-robin fairness; id order is incidental)

- Module comment `fair_scheduler.zig:28-30` states the property as fairness ("a low-numbered stream cannot win every pass"); id order is the mechanism.
- No test asserts DATA order between two streams. The only multi-stream tier-3 test is `tests/scheduler.zig:187-206` (`s2`), which asserts only `progressed`. `drr_cursor` and `sid_scratch` are referenced only inside fair_scheduler.zig.
- `connection.zig:4744-4745`: "DATA never goes straight to the wire here. It enters the scheduler, so that fairness between streams is decided in one place."
- HTTP/2 imposes no cross-stream DATA ordering; priority out of scope (P7).
- No test pins id order. `fair_scheduler.zig:663` `self.drr_cursor = sid +% 2;` also assumes odd-only ids.
- Behaviour change: today a new (highest-id) stream is served in the current pass if id >= drr_cursor; with an arrival-order ring it waits one full pass.

## 2. Design

Structures, all allocated at init from max_streams:
- `pending_slots` unchanged.
- `index: []u16` open-addressed hash, cap nextPow2(2*max_streams), value = slot idx, EMPTY 0xFFFF; linear probing, backward-shift delete (no tombstones). Hash `(id >> 1) * fib`, masked.
- `ready_q: []u16` ring of slot indices, cap max_streams.
- `blocked: []u16` dense unordered (swap-remove), `live: []u16` dense (swap-remove, per-slot live_pos), `free: []u16` stack.
- Per DataPending: `state: {free, ready, blocked, serving}`, `live_pos`, `blocked_pos`.
- Remove `sid_scratch`, `drr_cursor`; add `credit_gen_seen: u64`.

Why the scheduler owns its own id->slot map [LOGIC]: `Connection.slotIndex` (connection.zig:3599-3603) is itself a linear scan and pending outlives the handler slot (cancelHandler comment 3660-3663); `Session.streams` is dropped before the scheduler finishes (tests/scheduler.zig:353-357).

Blocked set re-arm: credit only increases at session.zig:1003-1004 (stream WINDOW_UPDATE), :999 (conn WINDOW_UPDATE), :933-940 (SETTINGS initial_window_size), all inside Session.ingest on the actor before drainEmit (connection.zig:3303-3309). Frame building only decreases credit (session.zig:545). Proposal: `Session.send_credit_gen: u64` bumped at those three sites; `drain`/`emitOneDataPublic` take it; on change, move every blocked slot to ready tail. O(blocked) once per credit event per turn. Rejected: per-stream "window changed" intent (cases 2 and 3 are all-streams anyway). [PREMISE: all credit increases pass through Session.ingest on the actor.] `conn_win <= 0` stays the first check.

Per function:
- enqueueDataBytes: index lookup / pop free / insert index / push live; on emittable data and not queued, push ready tail; if blocked stay blocked; `state` guards double push; QueueFull impossible, assert.
- emitOneData: framed path unchanged; sync credit gen; loop at most ready_len: pop head, state=serving, avail<=0 -> blocked; empty and !end_stream -> free; build; FlowBlocked -> blocked. Keep `pw.stream_id == sid` re-check after build (removePending can run during build) — pop-before-serve means removePending never deletes mid-ring [LOGIC]. Afterwards: end -> free; len>0 -> push tail; rest==0 && !end_stream -> free now instead of lazily (today the zero-length slot counts in pendingCount(), which connection.zig:4221 uses as an admission cap) [LOGIC]. Delete drr_cursor.
- takePending/removePending: index lookup; freePending also deletes from index, live, blocked; ring entries made stale are skipped on pop via `state`.
- forEachPending: iterate live; no callback removes during iteration (removals at 2235-2242, 2431-2438, 3728 run after); add a debug `iterating` flag asserted in freePending [LOGIC].
- nextSlowDeadlineNs: iterate live.
- dataEligible (also a scan, next in line): sync credit gen; `conn_win > 0 and (framed_len > 0 or ready_len > 0)`; a false "eligible" costs one wasted probe and drain already handles it [LOGIC].
- pendingByteLen/contains/findPending: index lookup; findPending stays public (2236, 2431, 3678, 6343, 6459).
- limits.zig resourceUpperBound: add the new arrays (~3 KiB/conn at 256 streams); tests/scheduler.zig:236 checks sizes [LOGIC].

## 3. Invariants and test

I1 a live slot is in exactly one of ready/blocked/serving; I2 index maps exactly live ids, live_len == active_pending; I3 pop-head, one quantum, push-tail; I4 blocked re-probed only after credit_gen changes; I5 post-build stream_id re-check stays.

Test "tier 3 is round robin": streams 1,3,5 enqueue 3x16 KiB, stream 7 16 KiB with window 0; assert 9 frames, no 7, between two appearances of a stream the other two appear exactly once, first three are a permutation of {1,3,5}. Phase 2: open window 7, bump credit_gen, enqueue one more on 1; assert 7 before 1's second emission and pendingCount()==0. Catches: push to head (m1), double push (m2), no re-arm (m3), freePending not clearing state/blocked (m4).
Second test: colliding ids with max_streams=4, delete the middle, assert others still found (catches clear-instead-of-backward-shift).

## 4. Where it is worse than today

- First-byte latency of a new stream on a busy connection: up to a full pass (~255 x 16 KiB) before its first byte, where today it often jumps in. Fairness trade; the fix would be priority (P7). [PREMISE: many streams simultaneously ready with full quanta.]
- Nondeterministic wire order (depends on enqueue timing). [LOGIC]
- Credit-change storms re-walk the blocked set (bounded by max_streams per turn). [PREMISE: many window-blocked streams plus a chatty peer.]
- ~3 KiB more per connection. [LOGIC]
- The index hash is a new place to be wrong silently; the collision test is not optional. [LOGIC]

In passing: drr_cursor's odd-only assumption [LOGIC]; framed path `active_pending == 0` must check the counter that is actually kept [LOGIC].
