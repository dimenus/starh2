# Design review: FairScheduler DATA selection without scans or a per-emit sort

You are one of three reviewers on a design panel. The others are from other
vendors and you will not see their answers. Your working directory is the
starh2 repository (Zig 0.17, HTTP/2 server on the zio runtime). Read the code;
the design under review is not built yet and exists only in this brief.

## Premises — treat these as settled

A finding that depends on the negation of one of these is out of scope.

- P1. The stack is not in real use and has no external consumers. Breaking
  `FairScheduler`'s API and its call sites in `src/edge/connection.zig` is
  allowed. Do not weigh backward compatibility.
- P2. `FairScheduler` is owned by one task (the connection actor) and is
  accessed under `session_mu`. Do not propose making it concurrent or
  lock-free.
- P3. No allocation after boot on the write path. Pending DATA lives in
  per-stream slabs carved from one boot allocation (see the module comment at
  the top of `src/edge/fair_scheduler.zig`). Any index or list you add must be
  fixed-size and allocated at `init`.
- P4. All production wire output passes through `FairScheduler`'s sink. The
  `test_queue_wire_bypass == 0` mutation canary must keep holding.
- P5. Outbound accounting distinguishes pending body bytes from framed wire
  bytes. Pending bytes release when the scheduler accepts the frame; wire
  occupancy releases on WritePump completion. Keep that split.
- P6. The three tiers (terminal controls, ordinary controls with the
  `CONTROL_BEFORE_DATA` yield, then DATA in a 16 KiB quantum) stay. This
  review is about tier 3 only.
- P7. HTTP/2 stream priority (RFC 7540 §5.3 / RFC 9218) is not implemented
  and is not in scope.

## What was measured

nachos, AMD Ryzen 9 9900X. Server pinned to 6 cores / 12 threads, h2load on
the other CCD. HttpArena baseline-h2c shape: `h2load -p h2c -c 1024 -m 100`,
`GET /baseline2?a=1&b=1`, 1-byte bodies, 12 zio executors. `perf record -g
--call-graph dwarf`, every thread busy. After commit e0f8b3a (Session.streams
fix), the top of the flat profile is:

    8.21%  edge.fair_scheduler.FairScheduler.emitOneData
    6.94%  edge.fair_scheduler.FairScheduler.enqueueDataBytes

`perf annotate` puts them on:

- `fair_scheduler.zig:261-262` — `findPending`, a linear scan of
  `pending_slots` (one per allowed stream, `Limits.max_streams_per_connection`
  = 256 by default) on every `enqueueDataBytes`, via `allocPending`.
- `fair_scheduler.zig:606-609` and `:612` — `emitOneData` copies every live
  pending stream id into `sid_scratch` and runs `std.mem.sort` on it, for
  every DATA frame it emits, to find where `drr_cursor` falls.

Throughput is 3.6-3.75M req/s in this setup. That number is context, not a
target.

## The open question you must decide: what does tier-3 fairness require?

Today tier 3 serves streams in ascending stream-id order, resuming at
`drr_cursor` (the stream after the one last served). Two readings:

- A. The ascending-id order is the contract. A replacement must reproduce it
  exactly (for example an ordered structure keyed by id).
- B. The contract is round-robin fairness: every stream with eligible DATA is
  served one quantum before any stream is served a second one. Id order is
  incidental, and an arrival-order ready ring satisfies it.

Read `tests/scheduler.zig`, the scheduler tests in
`src/edge/fair_scheduler.zig`, and every place in `src/edge/connection.zig`
that depends on which stream goes next, then decide which reading the code and
its tests actually require. Quote the line or test that decides it. If a test
pins id order without needing it, say so.

## What to produce

1. Your pick between A and B, with the evidence.
2. A concrete design for pending lookup and DATA selection that removes both
   the scan and the sort: data structures, what `init` allocates, and how
   each of `enqueueDataBytes`, `emitOneData`, `takePending`,
   `removePending`, `forEachPending` and `nextSlowDeadlineNs` changes. Say how
   a stream that is pending but not eligible (window exhausted, `avail <= 0`)
   is handled so the selection does not degrade into a scan of blocked
   streams.
3. The invariants the design relies on, and the test you would add that
   fails if fairness breaks. Name the mutation it catches.
4. What the design understates: the case where it is worse than today.

Rules for findings:

- Label each one **[LOGIC]** (holds regardless of any deployment fact) or
  **[PREMISE]** (depends on a fact; name it and what must be true for it to
  bite). Unlabelled findings are discarded.
- Quote at least one line from `src/edge/fair_scheduler.zig` or
  `src/edge/connection.zig` that is not quoted in this brief, so it is clear
  you read the code.
- Do not edit any file.
