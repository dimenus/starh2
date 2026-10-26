# Brief: slot pool stage 2, request memory from a server-wide block pool

You are the implementer. A reviewer from another vendor reads your diff, and
the orchestrator reruns every test and measures RSS itself. Do not tell the
orchestrator that tests pass; it recomputes them. Findings reach you only
through the orchestrator. Do not prompt, message or read any other agent's
pane.

## Why (settled; do not reopen)

Read the commit message of e7cf701 (stage 1) and `tools/arena-pool-panel.md`
(the panel brief, with the owner's decisions). In short: under
`smp_allocator`, RSS grows because blocks are freed on a different thread
than the one that allocated them. Stage 1 removed the per-connection boot
traffic. The load phase still grows about 10 MB per cycle because each
request's arena and body come from the general allocator (GPA): the arena is
made on the actor, grown on the handler's executor, and freed in
`finishHandlerJob` on whichever task finished.

## Decided design (do not reopen; if an item cannot work, STOP and show why)

1. **`RequestPool`, server-wide, lock-free** (new file,
   `src/edge/request_pool.zig`): blocks in power-of-two size classes from
   64 KiB up to the per-request cap (64, 128, 256, 512 KiB, 1, 2, 4 MiB at
   the defaults). Each class is built the way `src/edge/slab_pool.zig` is:
   one contiguous storage range and an atomic bitmap of free blocks, claimed
   lowest free bit first (read that file's header for why lowest-bit and not
   FIFO). No mutex and no allocation on the take and return path. Reuse
   `SlabPool` itself, or factor its bitmap out and share it, rather than
   write a second bitmap; say which and why. No per-executor caches.
   - **Storage is starh2's own mapping**, not the app's GPA: each class
     reserves `request_bytes_per_server / class_size` blocks of address
     space from `std.heap.page_allocator` (page-aligned, a multiple of the
     page size), so a class can use the whole cap on its own. About seven
     times the cap is reserved as address space; untouched pages cost no
     RSS. Only this mapping may be decommitted (item 2), which is why it is
     not GPA memory.
2. **The server cap counts RESIDENT bytes, so the bound is true.** Blocks
   are never given back to the GPA, so a cap on the bytes requests HOLD at
   one moment would let the per-class high-water marks add up past the cap
   over a mixed history. Track, per block, whether it is resident (an atomic
   resident bitmap beside the free bitmap). The server counter
   (`request_bytes_per_server`) is the bytes of resident blocks, free or in
   use, reserved with a CAS before an untouched block is first used, so two
   takers cannot both pass a check.
   - When a take needs an untouched block and the counter has no room,
     **reclaim**: claim a free resident block of another class (largest
     class first), decommit it, clear its resident bit, release it, give its
     bytes back to the counter, and retry. Only when no free resident block
     exists anywhere is the take refused (`server_cap`).
   - Decommit the way Ghostty does (read
     `/private/tmp/claude-501/-Users-rsaunderson-Source-mine-starh2/89e67bb8-f827-4e13-891d-4d40d0483cd7/scratchpad/refs/ghostty/src/terminal/mem.zig`,
     `decommit` and its Darwin pair): Linux `MADV_DONTNEED`; Darwin
     `MADV_FREE_REUSABLE`, with `MADV_FREE_REUSE` before the block is used
     again. A decommitted block's bytes are undefined to the next user.
     In a steady state nothing is reclaimed; reclaims are counted.
3. **The other caps count HELD class bytes, never the bump cursor.** A
   1.5 MiB buffer in a 2 MiB block charges 2 MiB.
   - per request: new `Limits.request_arena_bytes` (default 4 MiB; a power
     of two, at least 64 KiB, at least `request_body_bytes`). One field on
     the request's arena, touched by one task at a time.
   - per connection: the existing `request_bytes_per_connection`, now the
     class bytes a connection's requests hold (arenas and bodies), so one
     connection cannot take the whole pool. Raise its default so one
     connection can run a normal load (say 64 MiB) and say why in the
     commit.
   There is ONE accounting system for request memory: the old body
   reservation (`tryReserveRequest` / `releaseRequest`) becomes the pool
   charge, not a second counter beside it.
4. **Hard fail, no GPA fallback, anywhere on this path.** Over any cap, or
   when no block can be had, the arena's allocation returns
   `error.OutOfMemory`, and the lease records which reason fired
   (`request_cap`, `connection_cap`, `server_cap`). The address space is
   reserved in `Server.init`, so a failed reservation is a boot error, not a
   request error. Mapping
   before the response is committed: `request_cap` is 500; the others are
   503. After commit: `RST_STREAM(INTERNAL_ERROR)`. Today a refused body
   append and a refused dispatch allocation propagate to `run` and tear the
   connection down; catch them on the stream instead. `ae_parts.append(...)
   catch {}` silently drops an `accept-encoding` value; make it a stream
   failure too. Remove `req_alloc = leased orelse self.gpa` in
   `session.zig`: no header list is decoded onto the GPA.
5. **A small bump arena over pool blocks**, not `std.heap.ArenaAllocator`
   (its node growth and `retain_with_limit` call the child in ways that break
   the caps; both panelists showed the code). Allocator semantics:
   - `alloc`: bump the current block; else take the smallest class that
     fits the request (plus alignment).
   - `resize` / `remap`: grow or shrink in place when the memory is the
     current block's tail and the class has room. `ArrayList` growth relies
     on it.
   - `free`: rewind a tail allocation. If that leaves a block that is not
     the request's first block empty (cursor at 0), return the block to its
     class at once. A 1 MiB `ArrayList` render then holds its first block
     plus the 1 MiB and 2 MiB blocks at the last step, not every class it
     passed through.
   - Request end: the actor returns every block the request still holds.
6. **Ownership and threads.** The lease (`RequestLease`) is the arena's home,
   stable in `request_leases`, and stays occupied until `releaseSlot`. The
   `HandlerJob` holds a pointer, not a copy. `finishHandlerJob` no longer
   frees anything. `releaseSlot` returns the blocks after `handle.join()`.
   Leases that never dispatch (rejected, reset, connection end) return their
   blocks where the lease is released today.
7. **Bodies in the same arena.** `onData` appends to the stream's lease
   arena (the 413 length check against `request_body_bytes` stays first);
   `maybeDispatch` passes `list.items` (no `toOwnedSlice`, no GPA free);
   `releaseDispatchRequest`, `finishHandlerJob` and `Session.releaseRuntime`
   stop freeing bodies with the GPA.
8. **HPACK never fails the connection because the pool is busy.** The
   decoded output is bounded (`decoded_header_bytes`, 32 KiB default, plus
   the field array): prove the bound with a test and make the first block
   cover it, so once a request holds its first block, decode cannot run out.
   If the pool refuses the first block, decode into a per-connection scratch
   in the `ConnSlot` (planned in `slotBootBytes`, so the exact-fit checks
   cover it), keep the dynamic table in step, then answer the stream with
   503. A real decode error stays a connection error, as today.
9. **Counters** on `RequestPool`, atomics read by a `snapshot()`: per
   class `made` (an untouched block first used), `reused` and `reclaimed`;
   refusals by reason; resident bytes and their high-water; held bytes and
   their high-water. Show them on the bench server's `/mem` beside
   `live_bytes`, so `tools/rss-stability.sh` saves them.
10. **The bound.** `resourceUpperBound` counts the pool once, as
   `request_bytes_per_server` (resident bytes cannot pass it), plus the
   bitmaps. The reserved address space is not resident memory and is not
   counted; say so where the term is computed.
   `on_demand_conn` loses `request_bytes_per_connection` (it is now a share
   of the server pool, not extra memory). `h1_body` stays.
11. **Out of scope:** HTTP/1.1 (`src/edge/h1.zig` keeps its own arena), the
    frame and write-chunk GPA fallbacks, response-header scratch, the
    preface frames (all stage 3, t-3531).

## Tests (required)

- The pool: lowest-free-bit reuse per class; `made` stays flat across a
  second round of the same work; every cap refuses at exactly its limit
  (class bytes), and the reason is recorded; the resident counter holds
  under concurrent takers on several executors and never passes the cap;
  `deinit` asserts every block is home.
- Reclaim: a history that fills the cap with 64 KiB blocks, frees them,
  then needs 2 MiB blocks succeeds by reclaiming, the resident counter never
  passes the cap, and `reclaimed` counts it. A reclaimed block, used again,
  works (Darwin: after `FREE_REUSE`). A test that reads RSS before and after
  a reclaim on this OS shows the pages really leave (this is the dormant
  path; make it run).
- The bump arena: alloc, tail resize and remap, tail free, a block emptied
  by a free goes back at once, a non-tail free changes nothing, alignment.
- A 1 MiB `ArrayList` render (the bench's `renderFragment` shape) stays
  under the 4 MiB cap, and the test asserts the peak class bytes it held.
- End to end (h2c and TLS): zero GPA calls from request memory after
  warm-up (a counting GPA, as `tests/slot_reuse.zig` does for boot; say how
  you isolate request memory from the other per-request GPA calls stage 3
  still has).
- Failure mapping, end to end: a tiny per-request cap gives 500 before
  commit and `RST_STREAM(INTERNAL_ERROR)` after; a tiny server cap gives 503;
  a refused body append answers the stream and the connection serves the
  next request; a refused first block still decodes the header block (a
  later request on the same connection that uses the dynamic table decodes
  correctly) and answers 503.
- Blocks come home after: a normal end, a reset mid-body, a cancel, a
  connection error, a task handler on another executor.

## Mutation tests (required; break the CODE, not the tests)

1. Return blocks in `finishHandlerJob` (on the handler) instead of after
   the join.
2. Charge the bump cursor instead of the class size.
3. Skip the early return of an emptied block.
4. Remove the resident-bytes CAS (check, then add).
5. Let `onData`'s append error propagate (the old behaviour).
6. Decode into the GPA when the pool refuses the first block.
7. Count held bytes instead of resident bytes against the server cap.
8. Skip the decommit in reclaim (clear the bit and the count only).

For each, run the full `./zb build test` and record which test fails.

## Gates

- `./zb build test`, `./zb build ci` (on this Mac `ci` always fails
  `macos-libcxx-probe`, two runs; every other step must pass), `./zb build
  tls-smoke`.
- Zig caches a passing test run; change a comment in a test file to force a
  real rerun.
- The orchestrator measures RSS and throughput; you do not need to. Make
  sure `--gpa smp`, `--count-gpa` and `/mem` still work in the bench server,
  and add a bench flag to set the three caps.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-arena`. Never touch
  `/Users/rsaunderson/Source/mine/starh2` or any other worktree.
- Base tripwire, before any work: `git log --format=%s -4` shows, newest
  first, "Rework the stage-2 brief", "Add the brief for slot pool stage 2",
  "Add the panel brief for slot pool stage 2", and "Boot every h2c and TLS
  connection from a reusable slot". If not, STOP and report
  `STATUS: blocked`.
- `testdata/cert.pem` and `key.pem` are in the worktree.
- Kill only processes you started, by pid. Never `pkill -f`.
- This is a significant refactor. If a decided item cannot work, STOP and
  report it with the code that shows why, before building around it.
- Commit per milestone. Each message describes its own tree and is the
  decision record (problem, changes, benefits, alternatives rejected, what
  it depends on, statistics; markdown; plain ESL English). End it with
  `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file
  under "To file".
- Report in `tools/arena-pool-report.md` (not committed): commits, the
  tests and their output, each mutation and the test that caught it, the
  full gate summaries, what you could not verify. End with
  `STATUS: done s2p1`.

## Round 2 (after review of 33f8cb7)

The orchestrator ran `./zb build test` and `tls-smoke` on Linux (nachos):
both pass. The Linux decommit test ran: a reclaimed 2 MiB block went from
2,048 KiB to 0 KiB resident (`mincore`), and mutation 8 (no decommit) fails
"reclaim: the pages really leave" there. The RSS A/B is still running; its
result may add a round. A cross-vendor review asked for two fixes:

1. **Alignment above one page.** `vAlloc` returns null for an alignment
   above `page_size_min` without trying a larger block, and records no
   refusal reason, so the stream gets 503 (`server_cap`) though no cap was
   hit. Size a new block for the worst-case alignment padding and align
   from its real base; if no class can hold it, record `request_cap`. Test
   with an alignment of 64 KiB (and one larger than the largest class).
   Mutation: restore the early `return null`.
2. **`findLease` is a scan** over up to `2 * max_streams_per_connection + 1`
   touched leases, now on every DATA frame and header lookup, and the
   touched slice never shrinks. Make the lookup O(1): an actor-owned
   stream-id -> lease index (a fixed table sized at boot and in
   `slotBootBytes`, or the lease index kept where the stream already lives).
   Say which and why. Test: after a burst that touches every lease, a lookup
   does not scan (count probes in a test build). Mutation: fall back to the
   scan.

Decided, no change: HTTP/1.1 losing its server-wide body reservation is
accepted (its buffers are preallocated and counted in `h1_body`; t-3537
moves HTTP/1.1 onto the pool). Say it in the commit message of the change
that removed it, or in a new commit if that one is not easy to reword.

Same rules. Add a "Round 2" section to the report. End with
`STATUS: done s2r2`.

## Round 3 (after the RSS A/Bs on nachos)

What the orchestrator measured on nachos (`tools/rss-stability.sh`, smp,
10 cycles, n=3 per arm, server cap raised to 4 GiB so nothing is refused;
e7cf701 is stage 1, 79e7a7b is this branch):

- THP on (nachos is `always`): stage 2 idle RSS about 2.4 GB against about
  0.4 GB, slope 40-45 against 10-14 MB/cycle, and RSS grew 12-14 MB in
  every IDLE phase while the pool's counters did not move.
- With `MADV_NOHUGEPAGE` on the pool mapping only: idle RSS about 1 GB
  lower, slope and idle growth unchanged.
- With THP disabled for the whole server (`prctl(PR_SET_THP_DISABLE)`):
  idle-phase growth gone (8-23 KB), slope 10-26 against 11-13 MB/cycle
  (no better than stage 1), load-phase growth 14-16 against 8-11 MB/cycle.
  The pool's resident counter still creeps (+39 to +118 MB from cycle 2
  to 10): per-class high-water marks add up, and nothing is decommitted
  below the cap.
- Why stage 2 holds so much more than stage 1: `SmpAllocator` sends every
  allocation above 32 KiB to mmap/munmap, so stage 1 gave large render
  buffers back to the OS at once. The pool keeps them.

The owner decided: **a retain limit.**

1. **Retain limit.** New `Limits.request_pool_retain_bytes` (default
   64 MiB; at most `request_bytes_per_server`). When a block is returned
   and the pool's FREE resident bytes (resident minus held) would exceed
   the retain limit, decommit that block at once (the reclaim path's
   decommit: Linux `MADV_DONTNEED`, Darwin `MADV_FREE_REUSABLE`), clear its
   resident bit and give its bytes back to the resident counter. Below the
   limit, a returned block stays resident as now. The cap and the reclaim
   path stay as they are. Count decommits on return per class (`retired`),
   beside `made`, `reused` and `reclaimed`. Add a bench flag
   `--request-pool-retain-bytes`.
   - Say in the commit what a request costs above the limit (one madvise
     and fresh page faults), and that this is what `munmap` cost stage 1
     for every large buffer.
   - Tests: a burst above the limit leaves free resident bytes at the limit
     after the burst ends; blocks below it stay resident (no decommit, no
     `retired`); the resident counter and bitmap agree after concurrent
     returns and takes on several threads; the Linux and Darwin page probe
     shows a retired block's pages leave.
   - Mutation: skip the decommit on return (clear the bit and the count
     only); and compare against held instead of free resident bytes.
2. **`MADV_NOHUGEPAGE` on the pool mapping** (Linux), with a comment that
   says why (measured above). Darwin has no THP; nothing to do there.
3. **Instruments** (so the next A/B can split RSS exactly):
   - Bench server flag `--no-thp`: on Linux, `prctl(PR_SET_THP_DISABLE)`
     at startup, and print `"thp_disabled":1` in the ready line; an error
     on other OSes is fine. `tools/rss-stability.sh`: `RSS_NO_THP=1` adds
     the flag, and `RSS_SERVER_ARGS` passes extra server flags (word
     split), so a run can set the pool caps without a wrapper script. Put
     both in its header.
   - `/mem`: `request_pool.touched_bytes`, the pool's really resident
     bytes from `mincore` over the class mappings (Linux; on Darwin say
     "not available" rather than guess), beside the resident counter.

Same rules as before. Add a "Round 3" section to the report. End with
`STATUS: done s2r3`.

## Round 4 (landing): rebase onto master 4dfd2da

The orchestrator's nachos run of round 3 (THP off for every arm, 10 cycles,
n=3, smp): with the default retain of 64 MiB, the RSS slope fell from
12.8-16.8 (stage 1) to 6.9-8.1 MB/cycle, the load-phase growth from 9-12 to
3.1-3.5, the pool's resident bytes stayed at 96 MiB and its touched bytes at
about 50 MB from cycle 2 to 10, nothing was refused, and `./zb build test`
passed on Linux. Keep the 64 MiB default. A second cross-vendor review is
reading 4948936 now; its findings, if any, come as a later round.

Master has moved to 4dfd2da (t-3514: HEADERS before DATA, by construction
in `FairScheduler`; read its commit message). Rebase this branch onto it:

1. `git rebase 4dfd2da`, keeping every commit; resolve conflicts by hand
   (`connection.zig`, `fair_scheduler.zig`, `limits.zig` and the slot plan
   are the likely ones). Keep both sides' behaviour; t-3514's `started`
   keys stay in `FairScheduler.planBoot`.
2. Gates on the rebased tree: `./zb build test` (fresh cache), `./zb build
   ci`, `./zb build tls-smoke`.
3. Do not change behaviour beyond what the rebase needs.

Add a "Round 4" section to the report. End with `STATUS: done s2r4`.

## Round 5 (after the second review, of 4948936)

The orchestrator's gates on d432489 (Mac, clean cache) pass. A second
cross-vendor review found four defects; fix each with a test that FAILS
first, then a mutation that brings it back:

1. **A tightly aligned allocation overruns its block (High).** In `vAlloc`,
   `pad` is 0 for alignments up to a page, but the aligned start can still
   move past `header_bytes`: 65,472 bytes aligned to 4 KiB picks a 64 KiB
   block, starts at 4 KiB and ends at 69,568: a Debug assert, and
   `unreachable` in ReleaseFast. Above a page it overcharges by 63 bytes
   (960 KiB aligned to 64 KiB fits 1 MiB exactly but is refused). Size the
   block from the worst-case start offset (`alignForward(header_bytes,
   alignment)` when the alignment is at most a page, else `alignment`) plus
   the length, with checked arithmetic. Test allocations that end exactly
   at each class boundary, for several alignments.
2. **The retain decision races (High).** `give` reads `held_bytes` and
   `resident_bytes` at different moments while other returners move them;
   with N concurrent returners up to about N blocks stay resident above the
   limit (the round-3 report said one or two; the review shows it is not
   bounded by that). Make keep-or-retire ONE linearized decision: an atomic
   free-resident-bytes counter, CAS'd on return (add if it stays within the
   limit, else retire) and decremented when a resident free block is taken
   or reclaimed. Test with a barrier: N returners decide in reverse order;
   free resident bytes end at or under the limit. Update the round-3 claim
   in the code comments.
3. **A Linux `mincore` error reads as "not available on this OS" (Medium).**
   `touchedBytes` returns an error union: null only off Linux, an error on
   a Linux failure. `/mem` prints the error distinctly, and
   `tools/rss-stability.sh` treats a missing `touched_bytes` on Linux as a
   harness error when the pool exists.
4. **`touchedBytes` scans the whole reservation (Low).** At a 4 GiB cap
   that is 28 GiB per `/mem` call. Track each class's highest block ever
   made resident and scan only that prefix.

Same rules. Gates as before. Add a "Round 5" section to the report. End
with `STATUS: done s2r5`.

## Round 6 (after the third review, of 5c0927f)

The third review found the pool's logic correct (every free-resident
transition covered once, no shared ownership between reclaim and take,
aligned sizing exact). Two defects remain, both in the instruments:

1. **`touched_bytes` can miss a resident huge-page tail.** The scan stops at
   `made_end * size`; if `MADV_NOHUGEPAGE` was ignored (its error is
   ignored at init), a huge page that runs past the last made block is
   counted only up to that boundary. Round the scan end up to 2 MiB (the
   x86_64 and aarch64 Linux huge-page granule; name it), capped at the
   class mapping. Test: on Linux, a class whose last made block ends inside
   a 2 MiB region scans to the end of that region (arithmetic test that runs
   on every OS is fine).
2. **The RSS script checks `touched_bytes` only with `RSS_COUNT_GPA=1`.**
   `live_kb` returns early when GPA counting is off. On Linux, fetch `/mem`
   whenever the server answers it and validate `touched_bytes` when
   `request_pool` is present, whatever `RSS_COUNT_GPA` says; keep only the
   `live_bytes` extraction conditional. Require `curl` when that fetch is
   on. Check the shell logic on sample replies, as before.

Same rules. Add a "Round 6" section to the report. End with
`STATUS: done s2r6`.

## Round 7 (after the fourth review, of 1ad0d35)

The fourth review found three more defects in the instruments (the pool is
untouched):

1. **The huge-page rounding is relative to the mapping start.** Huge-page
   boundaries are absolute addresses, and `storage.ptr` is only page
   aligned. Align `@intFromPtr(storage.ptr) + made` up to the huge-page size,
   subtract the base, cap at `storage.len`. Extend the arithmetic test with
   a deliberately offset base.
2. **2 MiB is not the huge-page size everywhere** (aarch64 with 16 KiB or
   64 KiB base pages). On Linux read
   `/sys/kernel/mm/transparent_hugepage/hpage_pmd_size` once at pool init.
   If it cannot be read or parsed, scan each class's whole mapping prefix up
   to the cap (fail toward a number that is too large, never too small), and
   say so where the value is chosen.
3. **A failed `/mem` fetch passes silently on Linux without counting.** In
   `live_kb`, a failed or empty `curl` on Linux makes the cell `?` (the
   phase is invalid); use `--fail` so an HTTP error counts as a failure.
   Check the shell logic on sample cases, as before.

Same rules. Add a "Round 7" section to the report. End with
`STATUS: done s2r7`.
