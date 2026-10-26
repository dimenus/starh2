# Panel brief: request memory from a server-wide block pool (slot pool, stage 2)

You are a design reviewer from a different vendor than the author. Your cwd
is a git worktree of starh2 on branch `carmack/arena-pool`, at master e7cf701
plus this file. Read the code; do not edit anything; do not run the test
suite.

## Where this stands (settled; do not reopen)

- starh2 is a Zig 0.17 HTTP/2 server on the zio coroutine runtime. The goal is
  STABLE RSS under a repeating load with any general allocator (GPA) the
  embedding app picks. Chunky up-front or retained memory is welcome when it
  buys a stated bound.
- Under `std.heap.smp_allocator` the waste comes from blocks freed on a
  different thread than the one that allocated them, and it moves to another
  source whenever one source is moved. The decided fix is to remove the
  per-connection and per-request GPA traffic.
- Stage 1 (landed, e7cf701; read its commit message): every h2c and TLS
  connection boots from a reusable `ConnSlot` (`src/edge/conn_slot.zig`),
  sized by `Limits.slotBootBytes` (`src/core/boot_plan.zig`). Measured: churn
  growth per connection fell 4-6 times. The LOAD phase still grows about 10
  MB/cycle and is unchanged: request memory is still on the GPA.
- Owner's decisions for stage 2:
  1. Request arena memory comes from a **server-wide pool of blocks**, not a
     fixed arena per handler slot. (A per-slot arena of N bytes, first-touch
     and retained, was rejected: `max_streams_per_connection` is 256 and the
     bench renders up to 1 MiB per request, so peak retention would be
     gigabytes.) Blocks are made on demand, kept on a free list, never given
     back to the GPA before shutdown.
  2. **Hard fail above the caps.** A per-request cap and a per-server cap,
     both new `Limits`. Over a cap, the arena's allocation returns
     `error.OutOfMemory`; no GPA fallback. Before the response is committed
     the stream gets a 500 or 503; after, `RST_STREAM(INTERNAL_ERROR)`.
     Generous defaults (for example 4 MiB per request, 256 MiB per server).
  3. Memory is handed back on the actor after the handler is joined
     (`releaseSlot`), never on the handler's executor.

## Where request memory lives today (read these)

- `src/edge/connection.zig`: `borrowHeaderListAlloc` creates the lease arena
  with `.init(self.config.gpa)`; the arena moves into the `HandlerJob` at
  dispatch; `finishHandlerJob` ("runs on whichever task finished the job")
  calls `job.arena.deinit()` and frees the body from the GPA; `releaseSlot`
  runs on the actor after `handle.join()`.
- `src/core/session.zig`: `pending_bodies` is `u31 -> ArrayList(u8)` grown
  from `self.gpa` in `onData`, and `maybeDispatch` moves it out with
  `toOwnedSlice`. `request_bytes_per_connection` / `_per_server` already
  account body bytes.
- `examples/bench_server.zig` `renderFragment`: an `ArrayList(u8)` grown with
  `appendSlice(req.arena, row)` up to `kb` KiB (1 KiB to 1 MiB). Its growth
  needs ONE contiguous buffer of up to about 1 MiB plus the old copy.
- Zig 0.17 `std.heap.ArenaAllocator` (`zig env` -> lib_dir,
  `std/heap/ArenaAllocator.zig`): read how it sizes the buffers it asks its
  child for, and what `reset(.retain_with_limit)` does.

## Questions (answer each; be decisive; a survey with no pick is discarded)

1. **The contiguity problem.** Fixed-size blocks (say 64 KiB) cannot serve one
   1 MiB allocation, and an `ArrayList` that doubles needs exactly that. Pick
   one and say why: (a) size-classed blocks (powers of two from 64 KiB up to
   the per-request cap), the arena's child asking for the class at or above
   each request; (b) one contiguous region per request of the per-request cap,
   reused LIFO (what does that do to RSS, since a region keeps every page any
   earlier request touched?); (c) something else you can show in code.
   For your pick: what retained memory does it give at a peak of K concurrent
   requests, and what makes it stable rather than growing?
2. **The arena on top.** Keep `std.heap.ArenaAllocator` with a pool-backed
   child allocator, or write a small bump arena over pool blocks? Account for
   how `ArenaAllocator` grows its buffer list and for `resize`/`remap` in
   place, which `ArrayList` growth relies on.
3. **Cross-executor access.** Handlers run on other executors and allocate
   from the arena there. Take and return are on different threads. With zio
   (no `std.Io` primitives), what is the free list: one `zio.Mutex` stack, a
   lock per size class, or per-executor caches? Is contention on it a real
   cost at about 1 M requests/s? Quote the code that settles it.
4. **Request bodies.** Same pool, a separate body pool, or a per-stream buffer
   in the slot? `request_body_bytes` (256 KiB default) is the per-request
   body cap and `request_bytes_per_*` already account them. Where exactly do
   `onData`, `maybeDispatch`, `releaseDispatchRequest` and `finishHandlerJob`
   change?
5. **Caps and the bound.** Name the new `Limits` fields and defaults. How do
   they enter `resourceUpperBound`, and which existing terms do they replace
   (the `request_bytes_*` terms in `on_demand_conn` / `on_demand_server`)?
6. **Failure mapping.** Where does `error.OutOfMemory` from the arena surface
   for a complete handler, a task handler, a streaming (SSE) handler, and
   HPACK decode into the header list? Which code turns it into 500/503 or
   `RST_STREAM`, and is any path today a panic or a silent drop?
7. **The proving experiment.** What A/B on `tools/rss-stability.sh` shows the
   load phase flat, and what must it show to call stage 2 done? Which
   counters prove the pool did the work (blocks made, blocks reused, refusals)?
8. Anything in this plan that would make RSS grow or the bound lie.

## Output

Every finding must carry exactly one label, or it is discarded:

- [LOGIC]: holds regardless of any deployment fact.
- [PREMISE]: depends on a deployment fact; name the fact.

Quote at least one line of code for each claim about the code.
