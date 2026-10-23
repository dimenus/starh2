## 1. Memory goal

**[PREMISE — stated Datastar workload; this bites when connections are numerous, long-lived, mostly idle, and only occasionally render large fragments] Pick G2: low typical RSS with a hard, measured ceiling.** Memory retained per connection is the wrong currency for this workload. Residency should follow active requests and active writers, not every connection’s historical high-water mark.

**[LOGIC] G1 makes the occasional large fragment the permanent cost of every slot.** A modest fixed request budget of 256 KiB × 256 slots is 64 MiB per connection, or 64 GiB across the current 1,024-connection default, before other connection state. Even 64 KiB per slot is 16 MiB per connection. Choosing a smaller budget simply converts valid large renders into failures.

**[LOGIC] G2 need not weaken the bound.** Meter actual request-arena child allocations against stream, connection, and server ceilings, count the server ceiling and allocator overhead in `resourceUpperBound`, and release the charge only when the handler releases the memory. P3 remains unchanged: the write path uses boot-sized pools and never allocates.

## 2. Per-request memory

**[LOGIC] The current request limits do not bound `Request.arena`.** The allocator exposed to handlers is the job arena:

```5200:5210:src/edge/connection.zig
        job.req = .{
            .method = .parse(d.method),
            .scheme = scheme,
            .authority = authority,
            .path = path,
            .query = query,
            .headers = headers,
            .body = d.body,
            .trailers = trailers,
            .arena = a,
            .peer = peerFromHandle(self.stream.socket.handle),
        };
```

`request_bytes_per_*` currently meters inbound body bytes, not decoded headers or handler scratch. Its body charge is also released during dispatch before the owned body is finally freed. Therefore it is not a handler-lifetime memory budget.

**[LOGIC] Concrete G2 design: one lazily created, budgeted arena per live request, destroyed at request completion.**

- Wrap the arena’s child allocator with accounting that charges the child allocation’s actual capacity, including arena nodes and growth—not merely the requested JSON payload.
- Suggested defaults:
  - `request_arena_bytes_per_stream = 256 KiB`
  - `request_arena_bytes_per_connection = 2 MiB`
  - `request_bytes_per_server = 256 MiB`
- Include decoded headers and framework scratch in that same budget.
- Keep inbound body memory charged until `releaseDispatchRequest`, or include it in the same request-memory ledger.
- On completion use `deinit`/`free_all`, not per-slot retention. Start with no arena cache. If allocation CPU regresses measurably, add a small byte-capped, executor-sharded cache; do not let retention scale with connections or slots.

**[LOGIC] `retain_with_limit` alone is insufficient.** A 32 KiB retained limit × 64 slots still permits 2 MiB of sticky memory per connection once every slot has been used. It improves today’s unbounded high-water mark but does not align residency with active work.

**[LOGIC] New `Request.arena` contract:** it is fallible request-lifetime scratch storage; all returned memory remains valid until the handler returns, but allocations are subject to stream, connection, and server budgets. Before response commitment, exhaustion becomes a clean 500 or application error. After commitment, the stream is aborted.

**[PREMISE — large output exceeds the 256 KiB scratch cap] A large response must be streamed rather than fully materialized.** Use `Response.start`, serialize into a reusable ≤16 KiB chunk, call `Body.writeAll`, then `finish`. The existing response API supports this. The measured ≤12 KiB responses remain ordinary arena-backed sends.

## 3. `Connection.init` loop

**[LOGIC] The loop is not redundant as written.** `borrowHeaderListAlloc` reads and resets `job.arena` before the slot is claimed or `dispatch` initializes the job:

```3563:3572:src/edge/connection.zig
        for (self.header_lease_sid, 0..) |sid, i| {
            if (sid == stream_id) return self.handler_jobs[i].arena.allocator();
        }
        if (self.discard_lease_sid == stream_id) return self.discard_arena.allocator();
        for (self.handlers, 0..) |slot, i| {
            if (!slot.in_use and self.header_lease_sid[i] == 0) {
                self.header_lease_sid[i] = stream_id;
                _ = self.handler_jobs[i].arena.reset(.retain_capacity);
                return self.handler_jobs[i].arena.allocator();
            }
```

`deinit` likewise assumes every arena was initialized.

**[LOGIC] Apart from the arena, every `HandlerJob` field is set before use.** `claimSlot` initializes only `HandlerSlot`, joins, and events. `dispatch` then overwrites the job:

```5157:5166:src/edge/connection.zig
        job.* = .{
            .conn = self,
            .arena = arena,
            .stream_id = d.stream_id,
            .owned_request = undefined,
            .req = undefined,
            .resp = undefined,
            .matched = .not_found,
        };
        const a = job.arena.allocator();
```

It subsequently fully assigns `req`, `resp`, `hctx`, `matched`, `owned_request`, `slot`, and response/context pointers before admission. `task_counted` receives its default `false` and is set before task spawn when needed.

**[LOGIC] To remove the boot loop, add an explicit arena state.** A zeroed `arena_live`/`arena_initialized` bit per slot plus `ensureJobArena(i)` must initialize the arena before both header decoding and dispatch. Completion deinitializes it and clears the bit; connection teardown deinitializes only live arenas. Then `handler_jobs` can remain allocated but untouched until actual use.

## 4. Outbound path

**[LOGIC] Pending DATA is already the correct G2 shape.** `Server.init` allocates one server-wide pool of `outbound_slabs_per_server × outbound_bytes_per_stream`: currently 4,096 × 64 KiB = 256 MiB virtual storage. A stream rents a slab only while it has pending DATA and returns it immediately after draining.

**[LOGIC] Framing still allocates.** `Session.makeDataFrame` requests a per-connection frame slab, but the default slab is only 512 bytes. A normal 16 KiB DATA frame therefore falls back to GPA allocation. The per-connection frame pool itself is 256 × 512 = 128 KiB. Both the fallback and `leaseWriteChunk`’s fallback must disappear under P3.

**[LOGIC] `flushBatch` does not perform the attributed copy itself.** `EmitBatch.copyFrame` previously copied frames into `Connection.plaintext_scratch`; `flushBatch` takes a borrowed slice and forwards it:

```4514:4527:src/edge/connection.zig
                const taken = batch.emit.take();
                // queueWire encrypts this borrowed slice synchronously. The
                // buffer is the connection's boot-counted TLS plaintext
                // scratch, so a batch adds no allocation and no new limit.
                try batch.queue(
                    taken.bytes,
                    taken.flush,
                    taken.ticket,
                    taken.ticket_slot,
                    taken.ticket_count,
                    taken.control_n,
                    taken.control_entries,
                    false,
                    complete_batch_receipt,
                );
```

That scratch is 16 KiB, allocated per connection at boot and touched when batching.

**[LOGIC] `queueWire` performs no byte copy.** It selects either the encoded portion of an owned frame or the borrowed batch and calls `sendAccountedWire`.

**[LOGIC] `sendAccountedWire` performs the final plaintext copy.**

```4991:5006:src/edge/connection.zig
        std.debug.assert(src.len <= lease.bytes.len);
        @memcpy(lease.bytes[0..src.len], src);
        if (trace.enabled) _ = trace.records.fetchAdd(1, .monotonic);
        self.pushWriteChunk(.{
            .bytes = lease.bytes,
            .len = src.len,
            .flush_barrier = flush,
            .ticket = ticket,
            .ticket_slot = ticket_slot,
            .ticket_count = ticket_count,
            .complete_batch_receipt = complete_batch_receipt,
            .outbound_release = src.len,
            .control_release = control_n,
            .control_entries = control_entries,
```

The destination is `write_chunk_storage`, allocated by `Connection.init` as:

- `inbound_wire_chunks_per_connection × WIRE_CHUNK_SIZE`
- default: 8 × 16,704 = 133,632 bytes, or 130.5 KiB per connection
- when all chunks are leased, it GPA-allocates an exact-sized fallback

This pool is per-connection reserved memory, not the server-wide DATA slab pool.

**[LOGIC] TLS adds no second dynamic ciphertext allocation.** It reads plaintext from the same write chunk and encrypts into `tls.Conn.out_buf`. `in_buf + out_buf` is 33,290 bytes; the driver also has a 16,389-byte per-connection receive buffer. The 256 KiB `tls_stream_bytes` default is an accounting allowance, not the size of those two arrays.

**[LOGIC] Proposed outbound redesign:** use one boot-sized, lowest-free-first wire-chunk pool, preferably executor-sharded with a global fallback.

- Rent the final 16,704-byte wire chunk before a drain.
- Make that lease the `EmitBatch` buffer.
- `flushBatch` transfers the lease directly to h2c `WritePump` or TLS `Pump`; remove the `sendAccountedWire` copy.
- Add `Session.makeDataFrameInto` so DATA frames are encoded directly into the leased final chunk.
- Encode control frames into pooled chunks; remove per-connection `frame_pool` and all GPA fallbacks.
- Pool exhaustion pauses scheduler emission and wakes the owning actor when a chunk returns; reserve capacity for terminal output.

**[PREMISE — pool shared across connections] This costs an atomic rent/release and potentially a cross-connection wake per wire batch.** Executor sharding limits contention, but one hot executor can become imbalanced. That cost must be benchmarked against eliminating roughly 274.5 KiB of per-connection frame/write/scratch storage and two copies.

## 5. Defaults

**[PREMISE — target is roughly four thousand mostly idle Datastar connections, usually far below 64 live streams each] Proposed starting defaults:**

- `max_connections = 4_096`
- `max_streams_per_connection = 64`
- `max_streams_per_server = 8_192`
- `cancellation_reaper_jobs = 8_192`
- `inbound_wire_chunks_per_connection = 2`
- `outbound_bytes_per_stream = 32 KiB`
- `outbound_slabs_per_server = 8_192` → 256 MiB server-wide
- `outbound_bytes_per_connection = 1 MiB`
- `request_arena_bytes_per_stream = 256 KiB`
- `request_arena_bytes_per_connection = 2 MiB`
- `request_bytes_per_server = 256 MiB`
- `request_body_bytes = 64 KiB` by default, with route overrides
- `control_entries_per_connection = 128`
- `intent_entries_per_connection = 128`

**[LOGIC] At 64 streams, the two explicit handler arrays cost 42.5 KiB per connection:** 64 × 648-byte `HandlerJob` plus 64 × 32-byte `HandlerSlot`. At 256 they cost 170 KiB. Every other stream-indexed map, event array, ticket suffix, and scratch array also drops by approximately fourfold.

**[LOGIC] Two inbound chunks cost 33,408 bytes per connection instead of 133,632.** With the shared final-wire pool, per-connection write storage, frame slabs, and the 16 KiB packing scratch disappear.

**[LOGIC] The resulting known H2 subtotal is about 75 KiB per connection—handler/job arrays plus inbound bytes—before maps, descriptors, queues, Session state, and allocator metadata.** The comparable current known subtotal is about 575 KiB: handler arrays, read/write chunks, frame pool, and plaintext scratch.

**[LOGIC] Lowering `request_body_bytes` from 256 KiB to 64 KiB reduces H1’s two body-sized buffers from 512 KiB to 128 KiB per H1 connection.** With the existing three 16 KiB head regions, that is 176 KiB rather than 560 KiB. `resourceUpperBound` should use the maximum of mutually exclusive H1/H2 connection shapes rather than summing both shapes for every connection.

**[PREMISE — clients needing more than 64 concurrent streams] Those clients will receive a lower SETTINGS limit or refusals and may open more connections.** If real Datastar traces exceed 64, raise it from evidence; do not retain 256 merely because HTTP/2 permits it.

## 6. Where this is worse than today

**[PREMISE — sustained repeated large renders on the same slots] Today’s retained arenas pay allocation and page faults once.** The G2 design frees them, so repeated near-cap renders reallocate, fault pages, and may worsen throughput and tail latency.

**[PREMISE — backing allocator does not return freed pages to the OS] Logical reclamation may not reduce RSS.** The allocator/decommit behavior must be measured; otherwise G2 only moves ownership counters.

**[PREMISE — many connections render simultaneously] Requests can fail because the server-wide request budget is exhausted even though a particular connection is below its cap.** G1 would have guaranteed every admitted slot its private budget.

**[PREMISE — high write concurrency] A shared wire pool introduces contention and cross-connection interference absent from today’s per-connection write chunks.** It needs sharding, fairness, terminal reserve, and a proven wake path.

**[LOGIC] The proposed design is operationally more complex.** It adds budget conservation, lazy arena lifetime, pooled wire ownership, and exhaustion wakeups. Every error and teardown path must return exactly one request charge and exactly one wire lease.
