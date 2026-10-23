From memory, I proposed bounded, lazily allocated per-handler render scratch, separate from immutable request storage, reclaimed at explicit render-scope boundaries or handler completion, with reclaimable backing for large allocations.


**[LOGIC] From memory, I proposed bounded, lazily allocated per-handler render scratch, separate from immutable request storage, reclaimed at explicit render-scope boundaries or handler completion, with reclaimable backing for large allocations.**

Reviewed `3dfd805`. I read the other three round-one reviews, not my own. No files edited.

## D1. Request memory: I change to the fixed pool

**[PREMISE — F1, the stated workload, and P1’s freedom to change handlers]** I now prefer **a server-wide pool of 64 KiB request buffers plus streaming response generation**. This changes my recommendation toward Fable’s G1 approach: bounded, reusable storage with controlled first touches, rather than routine allocation and reclamation.

**[LOGIC]** Metering ordinary glibc allocations bounds outstanding allocations; it does **not** bound allocator-retained RSS by that same outstanding-byte limit. Freeing an arena can release its accounting charge while glibc retains the pages. Neither a particular allocation size nor an assumed fixed mmap threshold is an adequate reclamation contract.

**[LOGIC]** My previous metered proposal would need an explicit backing-store policy to deliver its promised post-burst reclamation: dedicated anonymous mappings for the reclaimable blocks, then unmapping those mappings at scope completion. That bypasses glibc retention, but costs page rounding, mapping/unmapping operations, mapping metadata, and renewed page faults on reuse. A small-block cache can reduce those costs, but its retained capacity must then be counted separately. `malloc_trim` is not a substitute for an ownership-based reclamation guarantee.

**[PREMISE — frequently repeated small requests, with large output that can be generated incrementally]** Those costs are unattractive here once a writer removes the need to materialize large responses. A reusable request buffer becomes substantially more compelling when it holds request state rather than the largest HTML fragment ever rendered.

### Is the pool’s high-water RSS acceptable?

**[LOGIC]** At the existing 4,096-server-stream default:

| Request buffer | Pool payload capacity | Free bitmap |
|---|---:|---:|
| **64 KiB** | **256 MiB server-wide** | 512 bytes |
| 256 KiB | 1 GiB server-wide | 512 bytes |

These exclude other connection storage, inbound bodies and the existing outbound DATA pool.

**[LOGIC]** Returning a buffer does not lower RSS. Lowest-free-bit reuse prevents needless rotation through untouched buffers, but residency follows the **union of pages historically touched**, not necessarily the current working set—or even the largest simultaneously touched working set when request sizes differ. The hard payload ceiling remains the whole pool.

**[PREMISE — ordinary requests use a small prefix; large HTML uses the writer]** **Yes, 256 MiB is an acceptable worst-case retained request-pool component for this default profile; 1 GiB is not my preferred starting point.** This is a deliberate acceptance of bounded high-water residency. It does not promise a return to startup RSS after a worst-case burst. Thousands of idle SSE streams still hold request leases, but periodic 200 KiB fragments no longer expand their request-storage footprints.

**[LOGIC]** F1 also prevents a retrospective allocator diagnosis: the supplied RSS measurements used **SmpAllocator**, not glibc. Neither reviewer’s glibc explanation establishes the cause of those particular mappings. The pool proposal changes the allocation shape under either allocator; its exact RSS benefit remains unmeasured.

### Two requirements the pool proposal must not skip

**[LOGIC]** A pool of `max_streams_per_server` buffers does **not automatically make exhaustion unreachable**. The current lifetimes differ:

- `Session.finishHeaderBlock` obtains the request allocator **before** global stream admission (`session.zig:1328–1367`).
- `releaseConcurrency` releases the protocol-stream token when the stream closes (`session.zig:696–711`).
- Request storage remains necessary until handler cleanup; `releaseSlot` joins the handler before reuse (`connection.zig:3636`).

Give request leases their own admission lifetime, covering decode through final release. Keep the proposed pool size, but refuse new request ownership when it is exhausted; do not assume protocol-stream accounting proves availability.

**[LOGIC]** Refusal must preserve HPACK synchronization. The current decoder continues decoding policy-exceeding blocks, while allocation failure closes the connection (`hpack.zig:489–559`, `session.zig:1337–1345`). A bounded decode-and-discard path is therefore needed when a request cannot obtain storage. Simply returning an exhausted allocator from `borrowHeaderListAlloc` would turn ordinary admission pressure into connection failure.

**[PREMISE — P2 permits shared storage when its cost is explicit]** The pool shares only allocation metadata; each rented buffer has one owner. Reusing `SlabPool` introduces bitmap scanning/CAS contention and the shared `rented` counter’s atomic traffic. Its implementation performs more than “one CAS per request” (`slab_pool.zig:93–112,142–159`). I accept that cost at request admission/release, not per serialized chunk.

## D2. Choose 64 KiB and `Response.bodyWriter()`

**[PREMISE — P1; rendering can be incremental]** Choose **64 KiB per request** and **Fable’s writer**, not a second whole-fragment buffer and not render scopes as the primary large-body solution.

**[LOGIC]** The new `Request.arena` promise should be:

> A fallible, fixed-capacity allocator owned by this request. Allocated values remain valid until handler completion. Capacity includes framework request metadata and application allocations; response size is not limited to that capacity.

There is no automatic arena reset between SSE events. Parsed signals and request strings must remain valid. Renderers must not append each event’s temporary output to this lifetime allocator.

**[LOGIC]** Neither 64 KiB nor 256 KiB guarantees that every permitted 256 KiB signal document can be parsed. Input size is not decoded allocation size. Likewise, `decoded_header_bytes = 32 KiB` alone does not prove that headers, trailers, growing field arrays and application state fit in 64 KiB. The budget is an explicit allocation contract, not a promise derived from those unrelated size limits.

### A 200 KiB fragment every few seconds

**[PREMISE — the renderer supports incremental output]** The handler should:

1. Start one SSE response and retain its request/signal state.
2. Create a bounded body writer once, using a reusable small buffer—for example, 4 KiB charged within the request budget.
3. Render each fragment directly through a streaming Datastar event formatter into that writer.
4. End the **event**, flush the formatter and body writer, then wait for the next update. Do not call `Body.finish()` between events.
5. Reuse the same buffers for the next fragment.

**[LOGIC]** The 200 KiB crosses the response pipeline in pieces; it never exists as a 200 KiB server-side temporary string. Existing `enqueuePending` already copies input in slab-sized pieces and waits for stream capacity (`connection.zig:4209–4244`). The writer exposes that mechanism to serializers accepting `std.Io.Writer`.

**[LOGIC]** This must be a **task handler**. The existing actor-path guard is explicit:

> `connection.zig:4164`: `if (self.running_inline) return error.WriteFailed;`

A complete handler cannot park waiting for its own actor to drain output.

**[LOGIC]** The Datastar integration also needs a real streaming entry point. The vendored SDK’s `patchElements` and `patchElementsFmt` create `Io.Writer.Allocating` and return a fully materialized event (`zig-pkg/datastar…/src/datastar.zig:95–112`). Its `Message` formatter already tracks line boundaries and writes to an external writer, but its initializer is private (`:195–229`). Expose that formatter properly rather than feeding a prebuilt 200 KiB event into the new body writer. Its multiline-prefix tests establish formatting behavior that must survive the change.

**[LOGIC]** Writer buffer drains and explicit event flushes must remain distinct. With compression enabled, draining every small buffer must not accidentally demand a Brotli flush or receipt for every piece. The existing `writeCb` already distinguishes compression operation, capacity waiting and receipt behavior (`connection.zig:6240–6295`); the adapter must preserve that separation.

**[LOGIC]** A render scope only bounds *lifetime*. It does not make a non-streamable 200 KiB render fit a 64 KiB budget. A second 64 KiB buffer has the same limitation unless the renderer is already incremental. If rendering genuinely requires a larger contiguous working set, this default rejects it or requires an explicitly larger configuration. That is a real capability cost, not something the writer magically solves.

**[LOGIC]** Streaming also changes failure behavior: once response bytes are committed, a rendering failure cannot become a clean replacement response. Abort the stream without emitting the event’s successful terminator; preserve cancellation and transport errors rather than reporting a completed fragment.

## D3. Keep write chunks connection-local; encode directly into them

**[PREMISE — P2, and mostly idle connections with occasional bursts]** Keep a **small connection-local pool with lowest-free-index reuse**. Start with **four chunks total**, including explicitly reserved terminal-output capacity, rather than introducing a server-wide wire pool.

**[LOGIC]** Four current-sized chunks cost:

`4 × 16,704 = 66,816 bytes = 65.25 KiB per connection`

That halves the current eight-chunk payload reservation. Lowest-free-index reuse avoids spreading tiny serial writes across all chunks.

**[PREMISE — connections have very sparse overlapping writes]** Sol’s server-wide pool could reduce aggregate storage further. Its cost is a new cross-connection resource dependency: a returning chunk must wake an eligible waiting actor without lost wakeups, starvation, use-after-teardown or a thundering herd. Executor sharding introduces another capacity-balancing problem. I would not take that complexity before exhausting the simpler local ownership design.

### Local exhaustion still needs an explicit wake path

**[LOGIC]** The actor should own its free-index bitmap. For h2c, return the chunk index in a completion message; for TLS, return it through the actor-local completion path. Every returned lease must produce a capacity notification, including unticketed output and failure cleanup—not merely ticketed writes.

The actor already selects on write completions:

> `connection.zig:3011`: `.acks = self.write_ack_ch.asyncReceive(),`

But `applyWriteAck` currently handles accounting/tickets, not a new actor-owned chunk ledger (`:2028–2068`). That return-and-retry behavior must be wired deliberately.

**[LOGIC]** When no ordinary chunk is available, stop the drain turn and service reads, completions, cancellation and shutdown. Do not wait for a free chunk while holding `session_mu`, and do not allocate a fallback. If an already-built batch owns a chunk, publish it before waiting so the actor is not itself withholding the capacity needed for progress. Terminal reserves need both byte storage and descriptor capacity.

### Direct encoding is safe only with reserve-before-debit

**[LOGIC]** Encode DATA **directly into the leased output chunk**, not into `plaintext_scratch` followed by another copy. This is not permission to prepend a header inside the pending DATA slab or borrow mutable scratch asynchronously.

The required transaction is:

1. Determine the frame size and batching disposition.
2. Reserve destination bytes, wire accounting, handoff descriptor capacity and required receipt metadata.
3. If reservation cannot succeed, leave the next DATA frame and flow-control credit unchanged.
4. Encode into the owned destination; Session performs the flow-control debit.
5. Commit scheduler consumption and transfer ownership. Normal capacity exhaustion must no longer be possible after debit.

**[LOGIC]** Grok correctly identifies the debit hazard, but an intermediate scratch buffer does not solve it. Today `makeDataFrame` debits before the sink runs (`session.zig:553–562`), and `emitOneData` consumes/compacts pending data before invoking that sink (`fair_scheduler.zig:893–919`). The repair is transaction ordering and ownership, not another memcpy. An exceptional failure after commitment still fails the connection closed; retrying would risk duplicate debit or lost bytes.

**[LOGIC]** Preserve the existing batch policy and boundary cases. A 16 KiB DATA payload plus its nine-byte header fits a 16,704-byte write chunk but **not** a 16 KiB batch. Keep the existing standalone-frame disposition for that case. Also retain capacity for the two handoffs one scheduler pop can cause when it flushes a previous batch then emits an unbatchable frame (`tls.zig:1126–1143`).

**[LOGIC]** Direct encoding transfers a stable lease, not a borrowed slice. TLS currently stashes chunks for later processing (`connection.zig:4871–4879`); their storage cannot be recycled merely because the scheduler’s drain returned.

## What changed, and what I missed

**[PREMISE — F1 plus the streaming-render workload]** I changed from metered/reclaimable request arenas with a 256 KiB ceiling to a **64 KiB server-wide fixed request pool**, accepting its bounded high-water residency. The decisive combination is avoiding large response materialization **and** avoiding allocator-dependent reclamation. Explicit mappings remain a valid G2 alternative, but I no longer prefer their cost and machinery for this default.

**[LOGIC]** I retain my connection-local write-pool and direct-encoding recommendations, now with explicit lowest-free-index reuse and a reserve-before-debit transaction.

**[LOGIC]** The strongest point another reviewer made that I missed was **Fable’s FIFO-rotation explanation for the local wire pools**. I counted their reservation but did not identify that tiny sequential traffic can first-touch all eight chunks without ever needing eight concurrent writes. Fixing reuse order attacks residency directly, without requiring a global pool or an OS reclamation mechanism.

