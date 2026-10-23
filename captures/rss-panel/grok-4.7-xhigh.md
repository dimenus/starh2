The stack should follow **G2**. The bytes that actually became resident are not the bytes `resourceUpperBound` reserves, and the workload is idle connections plus an occasional large render.

## 1. Goal

**[LOGIC]** G2. Pending DATA already works this way. `src/edge/slab_pool.zig` dropped the old per-connection reservation because it cost `max_streams_per_connection × outbound_bytes_per_stream` on every connection, 16 MiB at the defaults, whether the connection wrote one byte or not. The pool is one boot mapping, rented with the lowest free bit, counted once. A request arena that retains its high-water mark on every slot is that abandoned design, applied to the 39% path.

**[PREMISE]** This depends on P1 and on the workload in the brief: thousands of mostly idle connections, one long-lived SSE stream, occasional large fragments. G1 lowers RSS only by making the reserved bound smaller. The fragment budget has to sit on every slot of every connection, including the ones that have not rendered anything. At a budget large enough for a Datastar signal body (`request_body_bytes` is 256 KiB) and a render, 32 slots × 256 KiB × 1,000 connections is 8 GiB reserved before anyone asks for a page. Cutting the budget until that number looks acceptable is how a legitimate fragment starts failing. G1 also never gives the memory back, so one large page on a connection pins that slot's budget for the life of the connection. On this workload the reserved set converges toward the worst case as connections are touched, which is the RSS shape the slab pool was added to get away from.

G2 keeps a hard ceiling. It counts the ceiling once, in the same place the slab pool is counted, and it does not touch those pages until a live stream needs them.

The saturated h2load number is the wrong score. While 32 streams on a connection are inside a 12 KiB render, those pages are live under either goal. G2 wins after the handler returns, and on the connections that are idle.

## 2. Per-request memory

**[LOGIC]** `Request.arena` is `HandlerJob.arena`, a `std.heap.ArenaAllocator` over the connection's GPA. `ArenaAllocator.init` does not allocate. The first `alloc` does. `reset(.retain_capacity)` keeps every chunk. `free` returns memory only for the most recent allocation, so a handler that builds a buffer, then builds another, keeps both until reset. `finishHandlerJob` resets with `retain_capacity` and `releaseSlot` does not touch the arena, so the slot holds its largest chunk until the connection dies. `borrowHeaderListAlloc` hands that same arena to HPACK before dispatch, and `src/datastar.zig` parses signals into it with `parseFromSliceLeaky` for the life of the handler. An SSE handler does not return between events, so fragments appended to that arena accumulate for the life of the stream. Nothing in that path consults `tryReserveRequestBytes`. That counter, and the 8 MiB / 256 MiB `request_bytes_*` terms, cover inbound DATA only.

Option (b) is G1. A fixed buffer per slot, sized for the fragment and reserved at boot, is the 16 MiB mistake again.

Option (a) is not sufficient on its own. `retain_with_limit` keeps the last chunk and asks the child allocator to shrink it. `SmpAllocator.resize` (the usual ReleaseFast allocator) returns false across size classes, and `ArenaAllocator.reset` then tries a fresh alloc and a free of the old block. Below `SmpAllocator`'s 64 KiB slab it does not unmap. Above that it does. If both the resize and the fresh alloc fail, the large node stays and `reset` returns false. The std comment says the memory was released. The failure branch does not release it. So a retain limit does not by itself put the 12 KiB JSON pages back, and it is weakest when the machine is already short.

Option (c), per connection, still pins a warm arena to an idle connection. Per executor is closer, but a free list that rotates through every buffer is the failure `SlabPool` already documents: one small alloc per buffer faults a page of every buffer.

**[LOGIC]** The design is a server-wide rent, same shape as the outbound slabs.

- Add `Limits.handler_arena_bytes`, default **256 KiB** (the existing signal-body cap). One pool, `max_streams_per_server` buffers, lowest-free-bit, never zeroed, counted once in `resourceUpperBound`. Rent on `borrowHeaderListAlloc` / slot claim, return on `releaseSlot`. Do not keep a grown `ArenaAllocator` on the job.
- `Request.arena` is a bump allocator over that one buffer. Bytes stay valid until the handler returns. `free` of a non-tail allocation still does not recycle, which is the Zig arena rule, so the cap is on the sum of allocations in one handler, not on the live set. The allocation past the buffer returns `error.OutOfMemory`, and the existing fallback in `runHandlerJobBody` fails the response. A connection with no live stream holds none of these buffers.
- A large render does not go in that buffer. `resp.send` / `Body.writeAll` already copy into the outbound slab in `enqueuePending`, in pieces of `outbound_bytes_per_stream`, and `freePending` returns the slab when the stream drains. The handler writes the fragment into a second rented buffer of `outbound_bytes_per_stream` (64 KiB), or straight into the DATA slab through a writer, then `writeAll` copies it and the render buffer goes back before the next event. SSE keeps parsed signals in `Request.arena` for the life of the stream and reuses one render buffer per event.

**[PREMISE]** Depends on P1. Handlers that append every SSE fragment to `req.arena`, or that parse a 256 KiB signal document and also build the HTML in the same arena, fail once the 256 KiB buffer is full. Today they grow until the machine is full. That failure is the contract.

## 3. The `Connection.init` loop

**[LOGIC]** The loop is not redundant for `arena`. It is redundant for every other `HandlerJob` field, and it is why this line is 86% of `Connection.init`'s faults.

`ArenaAllocator.init` stores two pointers and allocates nothing. The assignment at `connection.zig:1658` writes a 648-byte `HandlerJob`, 256 times: 165,888 bytes per connection, all faulted, including the 224 slots a `-m 32` client never claims. `allocSlot` always walks from index 0 and takes the first free slot, so the live jobs are a prefix. At the 8 MB → 644 MB sample, 18% is about 114 MB, which is that array for roughly 700 connections. That matches a ramp, not arena buffers.

`claimSlot` never mentions `handler_jobs`. It only marks the slot:

```3527:3530:src/edge/connection.zig
        slot.in_use = true;
        self.slots_claimed += 1;
        if (comptime test_observe) _ = test_observed_slots_in_use.fetchAdd(1, .acq_rel);
        slot.stream_id = stream_id;
```

`releaseSlot` does not reset or read the job either. The arena reset is `finishHandlerJob`, which runs only after `dispatch` has filled the job.

| Field | Set before any read? |
|---|---|
| `arena` | No, unless the loop (or a replacement) has run. `borrowHeaderListAlloc` calls `arena.reset` and `arena.allocator` while the slot is still free. `deinit` calls `arena.deinit` on every slot. |
| `conn`, `stream_id`, `req`, `resp`, `matched`, `slot`, `hctx` | Yes. `dispatch` writes `job.*` before `runHandlerJob` / `finishHandlerJob`. Nothing else reads them. The loop's `conn = undefined` and `stream_id = 0` are not consumed. |
| `owned_request` | Yes. Stored after the header copies, before the handler. The early `errdefer` is `releaseSlot`, which does not read it. |
| `task_counted` | Yes. The `dispatch` literal leaves it false, then sets it true before `spawnInto`. `finishHandlerJob` is the reader, and on the spawn-failure path it runs after that store. |

So the loop can go if, and only if, two things are added:

1. A side flag, zeroed at init the way `header_lease_sid` already is. The job memory itself cannot carry the flag, because it is uninitialized once the loop is gone.
2. An `ensureArena` on the lease path and at the top of `dispatch`, before either `reset`. `deinit` must skip slots whose flag is clear.

```1845:1846:src/edge/connection.zig
        for (self.handler_jobs) |*job| job.arena.deinit();
        self.config.gpa.free(self.handler_jobs);
```

`deinit` on an uninitialized `ArenaAllocator` walks garbage. That call is why the flag is mandatory. With it, an idle connection faults none of the 166 KiB, and a `-m 32` connection faults the prefix it actually claims, about 21 KiB.

## 4. Outbound path

**[LOGIC]** Four copies, three buffers, and one post-init allocation. The h2c measurement never enters TLS. Inclusive samples overlap, so the 20.4% + 14.5% + 12.8% are not 47% of RSS.

For a ~12 KiB oneshot the window is 65,535 and the quantum is 16 KiB, so one DATA frame drains the slab. `emitOneData` does not `memmove` the slab on that path.

1. **Slab.** `enqueueDataBytes` copies the handler bytes into a 64 KiB slab from the server-wide pool (`outbound_slabs_per_server` 4,096 × `outbound_bytes_per_stream` 64 KiB = 256 MiB virtual, one mapping, counted once). Lowest free bit, returned in `freePending` when the slot drains, including between SSE events. First touch is the pages written, not the whole 64 KiB. This memcpy sits on the handler, not under `drain` / `queueWire` / `flushBatch`. It is not one of the three named samples.

2. **GPA frame, under `emitOneData`.** `makeDataFrame` calls `allocFrame`. `rentFrame` refuses anything above the slab:

```4907:4909:src/edge/connection.zig
    fn rentFrame(self: *Connection, n: usize) ?[]u8 {
        if (n > self.frame_pool.slab_bytes) return null;
        return self.frame_pool.tryRent(self.config.io);
```

`frame_slab_bytes` is 512. A DATA frame is 9 + payload, so every body above 503 bytes is `gpa.alloc`, then `encodeDataFrameInto` copies the slab into it. That buffer is freed in `releaseWireBytes` after the sink copies it onward. During the ramp those are fresh pages. This is the allocation the `FairScheduler` module comment says the write path does not do. The comment still describes per-connection boot slabs; the code rents a server pool. HEADERS that fit in 512 bytes use `frame_pool`: 256 × 512 = 128 KiB per connection, virtual, touched only on the slabs that are actually written.

3. **`plaintext_scratch`, under `copyFrame` / `flushBatch`.** `Connection.init` allocates `emit_batch.max_plaintext`, which is `TLS_PLAINTEXT_SCRATCH_SIZE`, 16 KiB, per connection, counted inside `per_conn`. `flushBatch` does not allocate. It `take()`s a prefix of that scratch and calls `queueWire` with `owns_bytes` false. The 12.8% is that handoff plus the nested copy in `sendAccountedWire`. Sixteen KiB cannot be a large share of RSS. It is a large share of samples because the callee faults the write chunk.

4. **`write_chunk_storage`, under `sendAccountedWire`.** The destination is the per-connection write pool when the frame fits and a free index exists:

```4992:4993:src/edge/connection.zig
        std.debug.assert(src.len <= lease.bytes.len);
        @memcpy(lease.bytes[0..src.len], src);
```

`WIRE_CHUNK_SIZE` is 16,704. Eight chunks is 133,632 bytes, allocated at init, counted as `terms.write_payload`, faulted only for `src.len` bytes of the chunks actually leased. `queueWire` itself does not allocate. If the pool is empty or the frame is larger than a chunk, `leaseWriteChunk` does `gpa.alloc(n)` and the pump frees it on completion.

`ControlPool` is a counter (`control_bytes_per_connection` 64 KiB), not a buffer. Control bytes live in the frame slabs or the GPA.

**[LOGIC]** Under G2, delete the GPA frame for DATA that already fits in the 16 KiB scratch. Write the 9-byte header and the slab bytes straight into `plaintext_scratch`, then let the existing `sendAccountedWire` copy land in the write chunk. Do not raise `frame_slab_bytes` to 16 KiB. At 256 slabs that is 4 MiB of virtual address per connection, which is G1. Do not pre-touch the write chunks or the frame pool. Both are already demand-paged and the right size for what they hold.

## 5. Defaults

**[LOGIC]** `max_streams_per_connection = 256` is the RSS multiplier for the init loop, the scheduler slot arrays that are `@memset` at init, five hash maps `ensureTotalCapacity`'d to that cap, and `frame_slabs_per_connection = 256`. It is not what the workload opens. One SSE plus a page-load burst fits in a small prefix. The preface does not read the limit:

```520:523:src/core/frame.zig
pub const serverPrefaceSettings = [_]Setting{
    .{ .id = .header_table_size, .value = 4096 },
    .{ .id = .enable_push, .value = 0 },
    .{ .id = .max_concurrent_streams, .value = 256 },
```

The comment says that value mirrors `Limits.max_streams_per_connection`. Nothing checks it. Changing one and not the other advertises a cap the slot array does not have.

**[PREMISE]** Depends on the Datastar connection shape, not on h2load `-m 100`. Proposed `max_streams_per_connection = 32`, and the SETTINGS value taken from that limit.

Per connection, if the init loop remains: handler jobs fall from 165,888 bytes to 20,736. Across 1,000 connections that is about 145 MiB of faults removed, which is the 18%. Frame slabs, if left at 256 × 512, stay 128 KiB virtual per connection. Set `frame_slabs_per_connection = 32` (16 KiB virtual). In-flight HEADERS are bounded by live streams.

Idle RSS after the loop is removed and arenas are rented: the memset structures for 32 streams (slots, events, scheduler index), plus whatever pages a live SSE actually touches. The 133 KiB write-chunk mapping and the 128 KiB frame-pool mapping stay virtual until a write or a header frame touches them.

**[LOGIC]** These defaults move the printed bound and do not move this RSS, because nothing allocates them on the measured path: `request_bytes_per_connection` (8 MiB × 1,024 = 8 GiB inside `on_demand_conn`), `outbound_bytes_per_connection` (4 MiB, an admission counter over bytes that already live in the slab pool), `control_bytes_per_connection` (a counter), `tls_stream_bytes` (256 KiB, allocated only for `mode == .tls`). `outbound_bytes_per_stream = 64 KiB` should stay. Bodies already chunk to it, and the slab is returned when the stream goes idle. Shrinking it does not help a 12 KiB response, which only faults the pages it writes.

`max_streams_per_server = 4,096` is the right kind of knob: it sizes both slab pools. One SSE on each of 1,024 connections fits. Thousands of connections means raising `max_connections` (default 1,024) together with it, or the extra connections never exist. The per-connection fixed cost is what that multiplies.

## 6. Where this is worse than today

**[LOGIC]** A saturated json bench can show almost the same peak RSS. G2 releases the render buffer after the copy. It does not make the in-flight copy free. Sampling the ramp still sees those faults.

**[LOGIC]** `SmpAllocator` keeps 64 KiB slabs on a freelist after `free`. `free_all` on a 20 KiB arena chunk does not unmap them. The only RSS drop that does not depend on the child allocator is never touching the page: rent from one boot mapping and write only the prefix the request uses. If the implementation stops at `retain_with_limit` and the chunks stay under 64 KiB, steady RSS stays near today.

**[PREMISE]** Depends on P1 and on handlers that build the whole response in `req.arena`. They get `OutOfMemory` at 256 KiB instead of a multi-megabyte slot. A handler that parses a maximum signal body and then allocates the HTML in that same buffer fails even though each piece would have fit alone. The render buffer is what keeps that case working. Without it, the cap is worse than today for that handler.

**[PREMISE]** Depends on clients that really open more than 32 streams. h2load `-m 100` against this default is capped at 32 and will be slower. The preface has to move with the limit. Leaving it at 256 makes the peer open streams that `allocSlot` then refuses.

**[LOGIC]** Encoding the DATA frame into `plaintext_scratch` removes the GPA buffer only for frames that fit in 16 KiB. A larger frame still takes the immediate `leaseWriteChunk` path, including its GPA fallback when the eight write chunks are busy. That fallback is unchanged. The new hazard is encoding into the DATA slab in place: the sink is allowed to fail after `makeDataFrame` has already debited flow control, and the connection fails closed rather than retrying. A scratch copy leaves the slab intact until the sink accepts it. An in-place header does not.
