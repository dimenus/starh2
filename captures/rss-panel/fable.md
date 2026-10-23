# Fable (Anthropic, read-only subagent) — answer to tools/rss-brief.md at 96cddb5 (condensed by the panel owner)

## Facts it reports from the code
- F1 [LOGIC] Request.arena is uncounted and unbounded: terms.handler_jobs counts the ArenaAllocator struct (HANDLER_JOB_SIZE 648 B), not its contents; reset(.retain_capacity) keeps the merged high water.
- F2 [LOGIC] Every DATA frame is GPA-allocated on the write path: connection.zig:4908 `if (n > self.frame_pool.slab_bytes) return null;` with frame_slab_bytes = 512 (limits.zig:110); makeDataFrame falls to gpa.alloc (session.zig:280), encodeDataFrameInto memcpys slab bytes in (session.zig:130), freed after the sink copies again. Contradicts the scheduler's "no allocation on the write path".
- F3 [LOGIC] resourceUpperBound is computed and discarded (server.zig:139 `_ = try ...`). ~12.3 MiB/conn on_demand_conn at defaults; counts h1_per_conn (560 KiB) for h2 connections too.
- [PREMISE] GPA is glibc malloc (link_libc; start.zig gpa = c_allocator in release). glibc keeps <128 KiB frees in per-thread arenas; plausible source of the 256 KiB-1 MiB mappings (inference).

## 1. Goal: G1, defined as "addressed at boot and counted; RSS = touched subset, kept low by touch discipline (lowest-free-bit, LIFO, no boot writes into large arrays), never by free+realloc". slab_pool.zig already states this rule.
Datastar case: per-connection retained memory dominates with many idle connections; under G2 reclaim does not return pages with glibc. G1 makes request memory per CONCURRENT request, pooled server-wide.

## 2. Per-request: (d) server-wide request slab pool + Response body writer
- Limits.request_scratch_bytes (64 KiB) and request_slabs_per_server (= max_streams_per_server, rung-4). A second SlabPool.
- HandlerJob.arena -> FixedBufferAllocator over a rented slab; Request.arena stays an Allocator. Rent where headers are decoded (borrowHeaderListAlloc free-slot branch, claimSlot) and return in releaseSlot/releaseHeaderList.
- Check at boot: request_scratch_bytes >= decoded_header_bytes + header arrays (else finishHeaderBlock OOM -> failConnection, session.zig:1341).
- Exhaustion -> RST refused_stream (unreachable at rung-4 capacity).
- Response.bodyWriter(): std.Io.Writer whose drain calls writeCb (connection.zig:6252), so bodies go handler -> stream slab; the 39% disappears rather than moves. Complete handlers above outbound_bytes_per_stream already fail (connection.zig:4161-4164); large fragments belong on task handlers.
- Rejected: (a) retain_with_limit (stopgap only; peak unbounded, per-slot retention); (b) per-slot fixed buffer (16 MiB virtual/conn; first-fit allocSlot keeps touched slots resident forever); (c) per-connection/per-executor arena pools (retention or migration).
- Cost: one CAS per rent and release.

## 3. Init loop: redundant except `arena`
dispatch writes the whole job (connection.zig:5157-5165) before reads; claimSlot/releaseSlot never touch the job. Only `arena` is read before dispatch (5153-5155, 3564, 3570-3571, 3592, 1845 deinit). A one-field write would still fault every page (648 B stride, ~41 pages, 166 KiB/conn; ~166 MiB at 1024 conns, consistent with 18% of 644 MB). Fix: never write the array at init; lease set by claimSlot/borrowHeaderListAlloc; or a job_inited bool on HandlerSlot as an interim.

## 4. Outbound trace (h2c, one body byte)
1. handler -> stream slab (fair_scheduler.zig memcpy); pooled, lowest-free-bit. Good.
2. slab -> GPA DATA frame buffer (F2), plus @memmove compaction per quantum.
3. frame -> plaintext_scratch (emit_batch.copyFrame; 16 KiB per conn, boot, counted).
4. -> write chunk (sendAccountedWire connection.zig:4993). Pool = 8 x 16,704 B (~130 KiB/conn), boot, counted. Free list is FIFO std.Io.Queue (:1672) so traffic touches all 8; read pool likewise FIFO (wire_pump.zig:141): up to ~260 KiB resident per active connection. When all 8 are in flight, leaseWriteChunk falls back to gpa.alloc (:4941): uncounted post-boot allocation on the write path.
Proposals in payoff order: (1) sink takes header-on-stack + body slice into slab, no GPA DATA frame; (2) batch directly into a leased write chunk, drop plaintext_scratch, no-free-chunk = stop the turn (backpressure); (3) LIFO or lowest-free-bit chunk free lists; (4) delete the GPA fallbacks (leaseWriteChunk, Session.allocFrame) so the counted number is real; (5) later, slab read offset instead of memmove.

## 5. Defaults
Estimate ~1.2-1.5 KiB per stream slot of tables -> ~300-400 KiB/conn at 256, mostly touched at init. Proposed: max_streams_per_connection 64; split inbound/outbound wire chunk counts, 4 each; request_scratch_bytes 64 KiB; request_slabs_per_server = max_streams_per_server; frame_slabs_per_connection 64 after the DATA fix. Asks for resourceUpperBound().terms and smaps_rollup before/after rather than trusting its arithmetic.

## 6. Understated
FixedBufferAllocator growth waste (Writer.Allocating doubling burns ~31 KiB of 64 for a 16 KiB body; HttpArena-style serialize-into-arena-then-send gets 500s past budget); long SSE handlers hold a request slab for life; glibc keeps small tables resident after close (full G1 = one mmap-sized block per connection, bigger refactor); DATA header split touches both drain sinks and TLS staging (do one step at a time); pool CAS contention unmeasured; handlers needing more scratch than the budget now fail.
