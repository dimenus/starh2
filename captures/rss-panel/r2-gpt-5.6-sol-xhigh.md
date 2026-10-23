**[LOGIC]** In round 1 I proposed metered per-request arenas charged against stream, connection, and server ceilings, freed without retention when the handler finished, with large responses streamed instead of materialized.

## D1 — Per-request memory

**[PREMISE — production uses glibc while the original RSS measurement used `SmpAllocator`] I now choose the fixed server-wide pool.** Freeing metered arenas through glibc does not reliably remove their pages from RSS; small chunks remain in per-thread arenas. The original measurements still identify first-touch sites, but they do not prove production reclamation.

**[LOGIC]** Making the metered design actually reclaim under glibc would require page-backed allocations followed by `munmap`, or page-aligned mappings plus `madvise(DONTNEED)`. That costs page rounding, syscalls, TLB churn, zero-fill faults on reuse, and less predictable request latency. A small cache would reintroduce retained RSS and require another explicit bound.

**[LOGIC]** Use one boot-allocated request pool:

- One fixed request buffer per globally admitted stream.
- Capacity equals `max_streams_per_server`.
- Lowest-free-bit rental, never FIFO rotation.
- No zeroing on rent or release.
- Count storage and bitmap once in `resourceUpperBound`.
- Rent before header decode; transfer the lease to the handler job.
- Return it after handler completion/join or rejected-header cleanup.
- Admission must precede rental, making exhaustion unreachable for valid configuration.

**[LOGIC]** This is G1, but its scaling axis is concurrent server work, not slots on every connection. An idle connection with no live handler holds no request buffer.

**[PREMISE — 64 KiB buffers and the current 4,096 global-stream default] The pool’s maximum storage is 256 MiB, which is bounded well enough.** Resident pages equal the high-water mark of bytes actually written across simultaneously leased buffers. Lowest-free-bit reuse prevents sequential one-shot requests from gradually faulting every buffer.

**[PREMISE — thousands of long-lived SSE handlers] Each SSE handler keeps one buffer leased, but rendering does not touch that buffer.** If all 4,096 buffers eventually use their full 64 KiB, 256 MiB remains resident until shutdown. That is an explicit, operator-visible ceiling rather than allocator-dependent retention. Raising the global stream limit to 8,192 raises this ceiling to 512 MiB.

## D2 — 64 KiB and a streaming body writer

**[PREMISE — normal request metadata and parsed signal state fit after the 32 KiB decoded-header allowance] Choose 64 KiB, not 256 KiB.** A 256 KiB × 4,096 pool permits a 1 GiB request high-water mark. Large response output does not justify that because output should not inhabit `Request.arena`.

**[LOGIC]** Boot validation must prove that 64 KiB covers the configured decoded-header maximum, header arrays, paths, alignment, and arena metadata. `Request.arena` then promises:

> A bounded request-lifetime bump allocator. Allocations remain valid until the handler returns, and allocation beyond the 64 KiB buffer returns `OutOfMemory`.

**[PREMISE — handlers parse large Datastar signal documents] This is a real breaking restriction.** `readSignalsFromBody` currently accepts up to 256 KiB and parses into `Request.arena`; a typed parse needing over 64 KiB will now fail. Such routes need a separately owned application-state representation or a deliberately larger server profile. The default should not reserve four times more request memory merely to preserve that uncommon case.

**[LOGIC]** Add `Response.bodyWriter()`: a `std.Io.Writer` with a small fixed staging buffer whose drain calls the response body write path. It must not allocate output in `Request.arena` or rent a second 200 KiB render buffer. Routes using it must be task handlers because complete handlers cannot park while their own actor drains output.

**[PREMISE — an SSE handler renders a 200 KiB fragment every few seconds] Its event loop does this:**

1. Keep immutable request fields and parsed signals in the 64 KiB request buffer.
2. Begin one SSE event.
3. Render incrementally into `bodyWriter`.
4. Each staging-buffer drain calls `Body.writeAll`; the bounded outbound slab copies those bytes and applies backpressure.
5. Write the final SSE event delimiter and flush.
6. Reuse the small writer staging buffer for the next event.

The 200 KiB event spans several HTTP/2 DATA frames but remains one SSE event. Its simultaneous render working set is the small writer buffer plus outbound storage, not 200 KiB in the request pool.

**[PREMISE — renderer requires a complete contiguous fragment] That renderer must change or use an explicitly configured alternative.** Silently spilling to the GPA would recreate the defect.

## D3 — Per-connection write chunks, directly encoded

**[LOGIC]** Keep write chunks per connection. A server-wide pool creates three unnecessary problems: global rent/release contention, cross-connection starvation, and a waiter registry that must wake exactly the actor whose drain was paused. A per-connection pool can only be exhausted by that connection, and its own transport completion already supplies the wake.

**[PREMISE — four transport chunks preserve measured throughput] Start with four normal chunks per connection:** 4 × 16,704 = 65.25 KiB instead of 130.5 KiB. Use a lowest-free-bit bitmap or LIFO discipline so serial traffic repeatedly touches the same chunk.

**[LOGIC]** Preserve terminal progress separately: add a small dedicated terminal-control chunk and one reserved queue position, or prevent ordinary output from consuming the final lease. Deleting GPA fallback is unsafe unless GOAWAY/RST can still be emitted under saturation.

**[LOGIC]** Exhaustion becomes backpressure:

- The scheduler stops the drain before building another frame.
- It releases `session_mu` through the normal actor loop rather than waiting on its own progress.
- h2c wakes when `WritePump` returns a chunk and posts its completion.
- TLS resumes from its CQ send completion on the same actor.
- No GPA fallback remains.

**[LOGIC]** Encode DATA directly into the leased write chunk; do not pass through `plaintext_scratch`. The safe transaction is:

1. Secure chunk capacity and all ticket/control metadata.
2. If the current batch lacks room, hand it off before touching flow control.
3. Call a `makeDataFrameInto`-shaped operation that writes the nine-byte header and payload into owned chunk space.
4. Debit flow control only once that infallible destination exists.
5. Commit the append, then shrink/compact the pending DATA slab.
6. Transfer the chunk to the transport at batch flush.

**[LOGIC]** This preserves the reason for the current fail-closed behavior without retaining the extra copy. Grok’s concern that the source slab must survive sink failure is valid under the current ordering, but scratch is not required to solve it: destination and metadata reservation must precede debit, and pending DATA must not be mutated until the append succeeds. A transport failure after ownership transfer already means the connection is closing.

## Changes since round 1

**[LOGIC]** I changed from metered G2 request arenas to a G1 server-wide fixed-buffer pool. F1 removes the reclamation premise on which my metered recommendation depended.

**[LOGIC]** I changed the request-buffer default from 256 KiB to 64 KiB because the large-output case belongs in `bodyWriter`, not `Request.arena`.

**[LOGIC]** I changed from a server-wide wire-chunk pool to smaller per-connection pools. The shared pool’s wake protocol and cross-connection contention are unjustified when connection-local completions already provide ownership and progress.

**[LOGIC]** I did not change my position that DATA should be encoded directly into its final leased chunk, but the capacity and metadata reservation must occur before flow-control debit.

## Strongest missed point

**[LOGIC]** Fable’s strongest point was that G1 does not require a fixed buffer per connection slot: a lowest-free-bit server-wide pool can be boot-bounded while charging memory only to globally concurrent requests, which removes the per-connection multiplication that drove my round-1 rejection of G1.
