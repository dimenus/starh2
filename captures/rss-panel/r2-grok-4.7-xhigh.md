Round 1: `Request.arena` is a bump allocator over a buffer rented from one server-wide pool of `max_streams_per_server` fixed 256 KiB buffers, lowest free bit, never zeroed, counted once, and returned when the slot is released, with a large render going into a second rented buffer the size of the DATA slab.

## D1

**[PREMISE]** This uses F1. A ReleaseFast starh2 process links libc (`build.zig:221`) and `std.process.Init.gpa` is then `c_allocator` (`lib/std/start.zig:794-797`). The round-1 RSS numbers came from an HttpArena entry that forced `smp_allocator`, so a free that dropped RSS there says nothing about glibc.

**[LOGIC]** The pool is the shape that lowers RSS for this workload. A free into glibc drops RSS only when that chunk was created with `mmap`. The default threshold is 128 KiB. Below it, `free` returns the chunk to the calling thread's arena; the heap gives pages back only by trimming a top chunk that is already larger than the trim threshold, and a hole in the middle stays resident. At or above the threshold, the first `free` does `munmap`, and unless the threshold was pinned with `mallopt`, that `free` raises the threshold, up to the 64-bit maximum of tens of MiB. Later requests under the new threshold, including the next buffer of the same user size once the stored chunk size is the page-rounded mapping, are ordinary heap allocations and stay. `ArenaAllocator` growth makes most of the chunks in one request small, and those never `munmap`.

**[LOGIC]** Kept as a metered arena, the freed memory leaves RSS only if each request is one block at or above a threshold pinned with `mallopt(M_MMAP_THRESHOLD, …)` (that pin is what disables the dynamic raise) and the block is `munmap`'d on release. The cost is process-wide: every other allocation past that threshold takes the same path. Every request pays `mmap`, `munmap`, and a fresh fault, which puts the 39% first-touch back on every render. The small chunks still stick in whichever executor thread's arena handled the handler. Parked SSE handlers hold their live blocks on top of that. That construction is a pool that unmaps.

**[LOGIC]** The pool's weak point is real and bounded well enough. A page written in a pool buffer stays resident for the life of the process, so RSS settles at the high-water of buffers that were rented times the bytes actually written into them. The buffer count is `max_streams_per_server`, which is already the admission cap, so the pool cannot outgrow concurrent admitted streams. Lowest-free-bit packs a stampede into a prefix, and not zeroing the buffer means a 12 KiB response faults 12 KiB. After the measured load that prefix is on the order of 4,096 × 12 KiB, about 48 MiB, and then it sits. A live SSE stream holds one buffer; with the fragment kept out of it, the resident part is the pages of its headers and signals. That follows live streams. It does not follow connections with no stream, and it does not follow every slot a connection once used. After a full-width stampede the pages do not return, including when the server is idle again. That is the same "free does not shrink RSS" outcome as glibc, without a per-thread copy of the high-water and without the threshold climb.

## D2

**[LOGIC]** 64 KiB per request buffer. A body bigger than that is written through `Response.bodyWriter()`, a `std.Io.Writer` whose drain is the existing write path into the stream's DATA slab.

**[PREMISE]** A Datastar SSE handler that renders a 200 KiB fragment every few seconds runs as a task handler. `waitForStreamSpace` returns `error.WriteFailed` while `running_inline` is set, because a complete handler parked there would be waiting for the actor it is running on:

```4161:4164:src/edge/connection.zig
            // Complete handlers run on the actor. This wait is woken by the
            // actor's drain, so parking here would never return. A body that
            // does not fit in the stream slab belongs on a task handler.
            if (self.running_inline) return error.WriteFailed;
```

Each event writes the fragment through the body writer. A drain copies at most `outbound_bytes_per_stream` (64 KiB) into the DATA slab and the handler parks until the actor drains it. The 200 KiB value is never one allocation. When the slab drains, `freePending` returns it, which is already what an idle SSE stream does between events. The request buffer stays rented for the life of the handler and holds decoded headers and signals. A signal document that does not fit in 64 KiB fails at parse. `src/datastar.zig` currently allows a 256 KiB body and parses it into `req.arena` (`max_body` at line 47, `parseFromSliceLeaky` into that arena). That parse is what the 64 KiB cap refuses. The knob is a larger `request_scratch_bytes`, and the price is a deeper sticky prefix on every stream that writes that far.

**[LOGIC]** A 256 KiB request buffer would accept `arena.alloc` of the fragment. The SSE handler does not return between events, so those pages stay on its buffer until the stream ends, and after the buffer is returned they stay in the pool. One fragment per long-lived stream is the per-connection high-water again. The SDK emitters that allocate a `[]u8` and return it have to write into the body writer instead.

## D3

**[LOGIC]** Keep the write chunks per connection. Make the free list lowest-free-bit, cut the count to 4, and delete the GPA fallback in `leaseWriteChunk`.

The free list is a FIFO. `finishBoot` pushes indices `0 .. n-1` in order, and `returnWriteChunk` puts an index back on the tail. One chunk in flight is enough to rotate through all eight, so a busy connection faults 8 × 16,704 = 133,632 bytes. Lowest-free-bit keeps a quiet connection on the same first chunk, one 16 KiB touch.

**[LOGIC]** An empty per-connection free list ends the drain turn before `makeDataFrame`. There is no debit and no substitute allocation. The missing chunk is in this connection's WritePump, or in `pump.carried` / `pending_writes`. `releaseChunk` runs on this actor's existing completion path, and the next turn drains. A server-wide pool runs dry because some other connection holds the chunks, and the completion that frees one is not the actor that needs it. The DATA slab pool refused that wake: exhaustion is unreachable only when capacity covers every admitted stream, and for wire chunks that sum is every connection's in-flight maximum added together, which is the per-connection storage again. `leaseWriteChunk` already says the actor must not park on the free list.

**[LOGIC]** Frame DATA into the leased chunk. Take the lease first, then encode into that memory, then let the sink hand the lease to the transport. `makeDataFrame` debits flow control before the sink accepts the bytes, so a lease miss must happen before that call. The pending slab is left unchanged until the sink's existing pending release. On sink failure the connection already fail-closes; that path returns the chunk. `plaintext_scratch` is the wrong destination, because TLS does not encrypt it before `pushWriteChunk` returns:

```4872:4877:src/edge/connection.zig
            // Stash only. Encrypting on this stack under session_mu overflowed
            // the coroutine (the old BoringSSL frame plus drainEmit). driveTlsTurn
            // writes after the lock drops.
            if (pump.carried == null) {
                pump.carried = chunk;
                return;
            }
```

`drivePending` reads `chunk.bytes` later (`encryptSome` at `tls.zig:1279`) and can park in the middle of a chunk (`.full` keeps `partial_off`). The comment in `flushBatch` that `queueWire` encrypts the borrowed scratch synchronously is false on TLS. The `memcpy` in `sendAccountedWire` is what makes the borrow safe today. With the frame already in the chunk, that copy and the 16 KiB scratch both go.

## Since round 1

Two changes.

The request side goes from a 256 KiB buffer plus a second 64 KiB render buffer to a 64 KiB request pool and `bodyWriter` for the rest. The second buffer would have stuck fragment-sized pages in the pool, and an SSE handler would have held them for the stream. Under glibc, freeing that buffer would not have given the pages back.

The frame goes into the leased write chunk rather than `plaintext_scratch`. The scratch kept the DATA slab intact across a failed sink, and the leased chunk still does that. It is also the allocation that is still alive when TLS encrypts.

**[LOGIC]** The settled removal of the init loop has to update `testTrapDoubleFinalize` (`connection.zig:6725-6743`). It sets `conn`, `stream_id`, `slot`, `owned_request`, and `hctx.encoder`, and it never sets `arena` or `task_counted`. `finishHandlerJob` then branches on `task_counted` (line 5372) and resets the arena (line 5377). Both are valid today only because the boot loop wrote them. `trapAdmitJob` has the same hole.

## Strongest point I missed

Astra's, on how long plaintext has to live under TLS. I treated `plaintext_scratch` as a buffer the sink could copy out of during the drain. The copy is the safety property: `pushWriteChunk` only stores `pump.carried`, and `encryptSome` reads those bytes on a later turn, including after a partial encrypt. Reusing the scratch on the next `drainEmit` while `carried` still points at it overwrites a frame the pump has not encrypted. That is what makes the scratch-first proposal wrong.
