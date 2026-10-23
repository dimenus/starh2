Two lifecycle details matter: header decoding uses a job’s arena before slot claim, and teardown deinitializes every arena. The outbound trace also distinguishes the server-wide DATA pool from connection-local frame, batch, and write buffers; reducing copies alone will not reclaim those reservations.


**Pick G2: bounded, reclaimable request memory, while preserving P3’s boot-sized outbound DATA pool.** Reviewed `96cddb5cf00e59869b581b71cf0e29b17d11adad`; no files changed. Numbers below are source-derived capacities, not new RSS measurements.

## 1. Why G2

**[PREMISE]** The deciding deployment fact is the supplied Datastar workload: many long-lived, mostly idle connections, with occasional expensive renders. Memory should track **currently live rendering work**, not the largest render that every handler slot has ever encountered.

G1 can also avoid touching unused reservations; reservation is not RSS. Its disadvantage appears **after a burst**: under the stated rules, backing touched by an unusually large render cannot deliberately be returned afterward.

**[PREMISE]** Under P1, changing the handler allocation contract is acceptable. I would spend that freedom on bounded request lifetimes and explicit render scopes, rather than requiring every application to fit a permanently reserved per-slot buffer.

A G1 server-wide render pool is a legitimate alternative—not every G1 design needs a buffer per slot. It trades reclamation and allocation flexibility for predictable admission and allocation-free reuse. That is attractive for sustained, uniform workloads; less attractive when rare large working sets dominate capacity planning. Large *output* alone does not require a large working set: streaming helps either goal.

## 2. Request memory: bounded lifetimes, not just bounded retention

**[LOGIC]** `retain_with_limit` alone is insufficient:

- It limits memory **after reset**, not allocation during a handler.
- A long-lived SSE handler may not return for hours, so completion-time trimming does nothing between events.
- A seemingly small per-slot cache still multiplies badly: 16 KiB retained by 32 previously used slots across 1,024 connections is **512 MiB**. Those slots need not have been simultaneously active server-wide.

### Concrete design

**[PREMISE]** For the stated workload, my starting configuration would be:

| Proposed request-working-memory limit | Value |
|---|---:|
| Per handler, including its request arena and render scopes | 256 KiB |
| Aggregate per connection | 2 MiB |
| Aggregate server-wide | 128 MiB |
| Retained backing after handler/render-scope cleanup | 0 |

These are allocation ceilings, **not reservations**. Existing request-body limits remain separate.

**[LOGIC]** Enforce those ceilings beneath the arenas, on their backing allocations. Charge actual rounded backing sizes, arena bookkeeping, and overlapping allocations during growth—not merely JSON length or requested object sizes. Include header-decode leases and the discard arena; they allocate before handler dispatch.

The existing `http/brotli.zig:27–63` provides prior art for charging backing allocations and releasing their charges. Its C-allocation adapter is not itself a reusable Zig allocator implementation.

Keep arena state exclusively owned by its current handler or actor. Aggregate admission can follow the existing atomic-accounting pattern; it does not require a shared arena protected by a new mutex. Global budget updates still have a cache-line contention cost, so perform them on backing-block changes rather than every arena suballocation.

**[LOGIC]** `Request.arena` should promise:

> Allocations remain valid until the handler finishes, subject to an explicit byte ceiling. Allocation may return `OutOfMemory`. No capacity is retained for the next request.

Add a separate, explicitly ended **render scope** for temporary event/page construction. Its backing allocator must not be the request arena—otherwise destroying the scope may leave its backing retained in the outer arena.

For SSE, persistent signal/state allocations can remain in `Request.arena`; each event uses a render scope that is destroyed after its bytes have been handed off. Never reset request storage automatically on `flush`, sleep, or another suspension: a handler may still reference it. This matters to `src/datastar.zig:23–24,53–79`, which currently promises request-lifetime parsed signals.

**[LOGIC]** Large responses should use a task handler and incremental serialization through a bounded writer buffer, with appropriate SSE framing and explicit flushes. The output may exceed 256 KiB; the simultaneous rendering working set may not.

This is not interchangeable with the current complete-handler path. In `connection.zig:4164`:

```zig
if (self.running_inline) return error.WriteFailed;
```

A complete handler cannot wait for its own actor to drain an oversized body. Routes requiring streaming must become task routes, not acquire a hidden heap spill.

Before response commitment, budget failure can produce a small error response; afterward it must abort/reset the stream, never report successful truncation. The current handler-error distinction is visible at `connection.zig:5414–5433`.

**[LOGIC]** Returning memory to an arbitrary GPA does not guarantee lower RSS. For this design, arena backing must have an explicit reclamation contract—initially reclaimable page-backed chunks, charged at their rounded sizes. Any later small-block cache needs its own bounded term. Otherwise “zero retained arena capacity” can merely move retention into the allocator.

## 3. The initialization loop is not currently redundant

**[LOGIC]** `allocSlot` and `claimSlot` initialize **HandlerSlot**, not **HandlerJob**. The arena is used before claim, and teardown visits jobs that were never claimed.

Two decisive lines outside the brief’s quoted material are:

```zig
// connection.zig:1845
for (self.handler_jobs) |*job| job.arena.deinit();
```

```zig
// connection.zig:5155
break :blk job.arena;
```

The second preserves the existing arena during dispatch; dispatch does not construct its first arena.

**[LOGIC]** Production initialization coverage is:

| `HandlerJob` field | Initialization before use |
|---|---|
| `arena` | **Depends on the boot loop.** Header decode uses it at 3564–3571; dispatch reads/resets it at 5148–5155. |
| `conn`, `stream_id` | Dispatch aggregate assignment, 5157–5165. |
| `req` | Full assignment at 5200–5211, with defaults applied. |
| `resp` | Assignment at 5212 onward; terminal, generation and context wired at 5259–5264. |
| `matched` | Initially `.not_found`, then assigned at 5247. |
| `owned_request` | Assigned at 5257 before admission/publication. |
| `slot`, `hctx` | Context assignment at 5239 onward, completed at 5261–5264. |
| `task_counted` | Aggregate assignment reapplies its `false` default from 1069; task dispatch sets it at 5294. |

`releaseSlot` does not initialize or reset the job arena. Its important lifetime barrier is:

```zig
// connection.zig:3636
if (self.handler_joins[i]) |*handle| handle.join();
```

**[LOGIC]** To remove the loop safely:

1. Add a compact initialized-arena bitmap outside the large job array.
2. Initialize an arena on **first header lease**, or before dispatch’s first arena access when no lease exists. Initializing only in `claimSlot` is too late.
3. Guard arena reset/destruction by that initialized state, including never-dispatched header leases and rollback paths.
4. Preserve the full dispatch assignments and join-before-reuse rule.
5. Update the synthetic job constructors at 6725–6742 and 6781–6809: they currently rely on boot initialization for fields including the arena and `task_counted`.

Do not remove initialization of `handlers`, lease IDs, or join handles along with the job loop.

**[LOGIC]** The existing job array is `256 × 648 = 162 KiB` per connection. Lazy initialization avoids deliberately touching unused job pages; it does **not** remove the allocation or its worst-case footprint. At 32 used jobs, the corresponding struct bytes are 20.25 KiB, before page rounding. Savings disappear if every slot eventually becomes active.

## 4. Outbound trace: several distinct destinations

**[LOGIC]** The inclusive percentages cannot be added into independent savings. In particular, both `flushBatch` functions eventually reach `sendAccountedWire`; they do not each own another large buffer.

The actual ownership/copy chain is:

| Stage | Destination and allocation | Default capacity / ownership |
|---|---|---|
| `enqueuePending` → `enqueueDataBytes` | Copies application bytes into `DataPending.slab` (`fair_scheduler.zig:670`). Allocated by `Server.init` through `SlabPool.init`. | **Server-wide pool:** `outbound_slabs_per_server × outbound_bytes_per_stream` = **256 MiB**, plus 512-byte bitmap. |
| `emitOneData` → `Session.makeDataFrame` | Copies slab bytes into an encoded DATA frame (`session.zig:120–131,526–554`). Remaining pending bytes may be moved within the same slab (`fair_scheduler.zig:902`). | Frame allocation described below. |
| Frame allocation | `Session.allocFrame` calls `Connection.rentFrame`; falls back to GPA if oversized or unavailable (`session.zig:274–280`). | **Connection-local pool:** `frame_slabs_per_connection × frame_slab_bytes` = **128 KiB**. |
| Batchable frame | `EmitBatch.copyFrame` copies into `Connection.plaintext_scratch`; original frame is released (`connection.zig:4377–4378,4578–4579`). | **One 16 KiB scratch buffer per connection**, allocated at 1550. Shared by both drain variants. |
| Both `flushBatch` functions | `take()` returns a borrowed scratch slice; `queueWire` forwards it. No new accumulator is allocated. | Same 16 KiB buffer. |
| `queueWire` → `sendAccountedWire` | Copies into `leaseWriteChunk`’s destination (`connection.zig:4993`). Owned source frames are released afterward; borrowed batch scratch is not. | **Connection-local write pool:** `inbound_wire_chunks_per_connection × WIRE_CHUNK_SIZE`. |
| h2c WritePump | Consumes queued chunks using a buffered stream writer, then flushes before completion. | Additional **16,704-byte task-local writer buffer** (`wire_pump.zig:324–326`); not every write necessarily copies through it. |
| TLS Pump | Encrypts retained plaintext chunks into `send_buf`, which aliases `Conn.out_buf`. | **16,645-byte output array**, inside heap-allocated `tls.Conn`; not another large ciphertext pool. |

**[LOGIC]** Here `WIRE_CHUNK_SIZE` is **16,704 bytes**, from `core/wire_const.zig`. Eight write chunks therefore reserve **130.5 KiB per connection**, independently of the equally sized read storage.

The 512-byte frame slabs accommodate only 503 DATA payload bytes plus the nine-byte header. Most JSON responses in the supplied workload therefore take the frame GPA path. Separately, `leaseWriteChunk` falls back to an exact-sized GPA allocation when no suitable chunk is free:

```zig
// connection.zig:4941
const buf = self.config.gpa.alloc(u8, n) catch return error.OutOfMemory;
```

These are existing source paths, not a proposal to relax P3. Smaller pools must not rely on these escape hatches to make progress.

**[LOGIC]** TLS also has a 16,645-byte input array and a separately allocated 16,389-byte ciphertext receive buffer. `Pump.plain_buf` aliases the first read chunk; `send_buf` aliases `Conn.out_buf` (`tls.zig:1424–1435`). The `tls_stream_bytes = 256 KiB` setting is not a corresponding 256 KiB staging-buffer allocation. None of these TLS buffers explains the supplied **h2c** measurements.

### Change warranted

**[LOGIC]** I would retain the server-wide pending-DATA slab design, including its lowest-free-bit reuse. It already returns slabs when SSE streams become idle (`fair_scheduler.zig:905–912`), rather than retaining one per open stream.

For the next outbound change, make the batch accumulator an **owned write-chunk lease**, and let Session encode DATA directly into available leased output space. Then transfer the lease to the transport. This removes the intermediate DATA-frame allocation and the batch-scratch-to-write-chunk copy without borrowing mutable pending slabs across an asynchronous write.

Reserve output space **before** debiting flow control. Preserve the scheduler’s ordering, 16 KiB batching policy, ticket chains, terminal-control capacity, and distinct pending/wire accounting.

**[LOGIC]** Pool exhaustion must stop draining and return the actor to completion processing—not block it awaiting its own progress, allocate a replacement, or silently drop output. Capacity must cover the builder, queued chunks, transport-held chunks, TLS carried/pending chunks, and the two-push case documented by `tls.Pump.pushes_per_pop`.

Finally, returning a chunk to either existing pool does not decommit its storage. Reduced copy count is not automatically reduced retained RSS.

## 5. Defaults and their memory implications

**[PREMISE]** Assuming a typical connection carries a few SSE streams plus a page-load burst—not hundreds of subscriptions—I would choose **64 streams per connection**. That leaves room around the measured `-m 32` workload without initializing and sizing everything for 256.

For wire buffering, target **four read and four write chunks**, but land the write-credit/progress change above before relying on that smaller capacity.

**[LOGIC]** Using today’s struct sizes, these identified allocations become:

| Existing allocation | Current defaults | Proposed sizing |
|---|---:|---:|
| Handler jobs | 162 KiB | 40.5 KiB |
| Handler slots | 8 KiB | 2 KiB |
| Read chunk storage | 130.5 KiB | 65.25 KiB |
| Write chunk storage | 130.5 KiB | 65.25 KiB |
| Frame slab pool, unchanged | 128 KiB | 128 KiB |
| Batch scratch, before lease redesign | 16 KiB | 16 KiB |
| **Identified subtotal** | **575 KiB** | **317 KiB** |

That is **258 KiB less reserved payload/struct storage per connection**, or 258 MiB across 1,024 connections. It is not a promised RSS reduction or complete connection size. Maps, tickets, queues, request backing, allocator overhead, and any new budget-state fields remain additional. Eliminating the separate batch scratch removes another 16 KiB.

**[LOGIC]** Changing `Limits.max_streams_per_connection` alone is incomplete: `frame.serverPrefaceSettings` hard-codes 256 at `frame.zig:523`, and `Session.emitServerPreface` uses that constant at `session.zig:568`. Advertise the configured limit, or clients will legitimately attempt more concurrency than the new admission policy allows.

**[PREMISE]** I would initially keep these other defaults:

- **64 KiB pending DATA per stream.** Lowering it also lowers what a complete handler can enqueue without blocking; streaming routes benefit differently.
- **4,096 server-wide slabs/streams.** At 64 KiB, the slab reservation remains 256 MiB. Do not reduce slab count independently: boot validation requires at least one slab per globally admitted stream.
- **256 small frame slabs.** Lowest-index reuse already avoids touching all of them for a small working set. Shrinking this pool blindly can increase GPA fallback rather than lower RSS.

**[LOGIC]** Two other important accounting distinctions:

`Session.intent_storage` and `Connection.intent_batch` are separate allocations, each `max(intent_entries_per_connection, 16) × @sizeOf(Intent)`. Reducing stream capacity does not shrink their default 512-entry capacities automatically.

HTTP/1 reserves `3 × h1_head_bytes + 2 × request_body_bytes` = **560 KiB** for the named head/body buffers. That matters to HTTP/1 deployments, but Server branches into H1 separately; it is not an H2 connection’s resident-buffer explanation.

**[PREMISE]** With thousands of SSE connections, provision the **global stream limit** for live subscriptions **plus page-load headroom**. Four thousand connections each holding one SSE stream nearly consume today’s 4,096-stream limit. Raising it also requires matching reaper capacity and slab count; at 8,192 slabs, the unchanged 64 KiB slab size reserves 512 MiB.

## 6. Where this design is worse

**[PREMISE]** A continuously busy JSON server with repeatable request sizes can be slower than today: retained arenas reuse warm pages; zero-retention arenas incur allocation, reclamation, and renewed first touches. G1 is stronger for that workload.

**[PREMISE]** Connections genuinely needing more than 64 simultaneous SSE streams lose concurrency, and streams occupied by subscriptions leave less room for page loads. They need a larger configured limit; the smaller default is not universally better.

**[PREMISE]** Four wire chunks may reduce batching and throughput on high-bandwidth or delayed-completion transports. Smaller capacity is a memory/throughput choice, not a free optimization.

**[LOGIC]** Streaming does not eliminate live memory while a renderer is blocked on a slow consumer. Request state and the current render scope remain live until released. The byte ceilings must still reject excessive working sets, sometimes after response commitment.

**[LOGIC]** Finally, this design deliberately leaves boot-sized outbound pools allocated under P3. It can reclaim a rare large render’s working memory; it does not promise that RSS returns to the original idle baseline after every pool has been exercised.**Pick G2: bound live memory, but reclaim request/render memory after use.** Keep P3’s boot-sized outbound DATA pool.

Reviewed `96cddb5cf00e59869b581b71cf0e29b17d11adad`; no files changed. Numbers below are source-derived capacities, not a new RSS measurement. Access to `qmdsync` and `zigstd` was denied, so I do not rely on unseen history or unverified arena-reset internals.

## 1. Why G2

**[PREMISE: the supplied workload—mostly idle connections, infrequent large fragments]** Memory should follow **concurrent rendering**, not every slot’s largest historical render. Long-lived SSE makes connection teardown an ineffective reclamation mechanism. G2 permits a connection to render something large, return that memory, and remain inexpensive while idle.

**[LOGIC]** G1 need not mean a fixed buffer for every slot: a boot-reserved pool can also exploit limited concurrent work. Nor does reserving address space automatically make it resident. However, under the stated G1 rule, once a sufficiently broad burst touches that pool, reclamation cannot lower its subsequent RSS. Choosing a smaller pool instead makes some otherwise manageable bursts fail.

**[PREMISE: occasional fragments require substantially more memory than ordinary events]** A fixed 256 KiB request allowance costs 64 MiB of capacity per connection at 256 slots, or 16 MiB at 64 slots. Those are reservations, not immediate RSS predictions, but they illustrate the wrong scaling axis. G1’s alternative is smaller budgets and more rendering failures or mandatory streaming. G2 buys burst tolerance and post-burst reclamation at the cost of allocation overhead and less predictable latency.

## 2. Request memory: separate lifetime from rendering scope

**[LOGIC]** A retention limit alone is insufficient:

- It limits memory kept **after reset**, not allocations during a request.
- An SSE handler may not return for hours.
- The existing arena also contains decoded request strings. Resetting it after an SSE event would invalidate request fields.

The coupling is explicit in `dispatch`: header strings are reused when `keep_arena` is true, and `job.req.arena` receives that same allocator (`connection.zig:5174–5209`).

**[PREMISE: P1 permits the API change; P2 favors exclusive ownership]** I recommend two storage lifetimes:

| Storage | Ownership and lifetime | Reclamation |
|---|---|---|
| Immutable request storage | Decode lease, then handler job; request fields remain valid until handler completion | Release after completion/join, including rejection and rollback paths |
| Application render scratch | Exclusively handler-owned; allocated lazily | Explicit render-scope reset, and unconditional release at handler completion |

`Request.arena` becomes the second allocator. Its promise should be:

> Allocations are bounded and valid until handler completion or the handler explicitly ends its render scope. They are never invalidated by an unrelated await. Capacity is not retained after the scope ends.

An explicit reset invalidates **all application allocations from that arena**, not request fields. Applications needing persistent SSE state must keep it outside the resettable render scope, under a separately accounted lifetime.

**[PREMISE: the measured JSON bodies are roughly 12 KiB; larger fragments can stream]** Starting scratch limits:

- **256 KiB per handler**
- **2 MiB per connection**
- **128 MiB server-wide**
- **Zero retained render-scratch capacity after scope completion**

These are ceilings, not reservations or guaranteed allocations. The connection allowance permits eight simultaneous maximum-sized scratch users; smaller users share it normally. Keep immutable request storage separately charged against the request-memory budget.

**[LOGIC]** Enforce these limits at the arena’s **backing-block allocator**, charging actual allocation capacity, alignment, bookkeeping and growth overlap—not just bytes requested by application code. Release accounting only when backing storage is actually released. `src/http/brotli.zig:27–61` provides local prior art for charging allocation overhead, though its C allocator hook is not a drop-in Zig allocator.

Per-handler state remains exclusive. Connection/global budget counters require synchronization; charge them on backing-block acquisition/release rather than every small arena allocation. This adds counter contention, not a shared mutable arena or a new arena-pool mutex.

**[LOGIC]** Reclamation must have an OS-facing contract. Returning memory to a caching GPA does not establish lower RSS. Use reclaimable page-backed storage for large scratch blocks, with page-rounded accounting; any small-block cache must itself have an explicit total ceiling. Otherwise “freed arena bytes” can merely become allocator-retained bytes.

**[LOGIC]** Large responses should use a **task handler**, incremental serialization through a bounded buffered writer, and the existing `Body.writeAll`/`flush` interface. `enqueuePending` copies bytes into scheduler-owned slabs before returning; the handler can then reuse its serialization buffer. Do not attempt this from a complete handler:

> `connection.zig:4164`: `if (self.running_inline) return error.WriteFailed;`

Complete handlers cannot wait for the actor to drain their own pending slab. A route that may exceed that slab belongs on the task path.

**[LOGIC]** Scratch exhaustion before response commitment should produce an explicit allocation error and a bounded error response; after commitment, abort/reset the stream rather than report successful truncation. Reserve the error path independently of exhausted scratch. Also update Datastar signal parsing: `src/datastar.zig:51–80` currently allocates parsed signals into `req.arena`, so signals cannot silently retain their old request-lifetime promise across explicit scratch resets.

**[PREMISE: SSE handlers adopt render scopes]** This addresses the important idle case: render fragment, enqueue/flush it, end the scratch scope, then wait. Merely replacing every `.retain_capacity` with a smaller retention policy would not accomplish that.

## 3. The `HandlerJob` initialization loop is not redundant as written

**[LOGIC]** `allocSlot`/`claimSlot` initialize **HandlerSlot**, not **HandlerJob**. `releaseSlot` joins and releases slot ownership; it does not initialize a job arena.

The production field audit is:

| `HandlerJob` field | Established before handler execution |
|---|---|
| `conn`, `stream_id`, initial `matched` | Aggregate assignment, `5157–5165`; final match at `5247` |
| `req` | Aggregate assignment, `5200–5211` |
| `resp` | Aggregate assignment, `5212–5225`; terminal, generation and context completed at `5259–5264` |
| `hctx`, `slot` | `5239–5246`, `5261–5264` |
| `owned_request` | Ownership transfer at `5257–5258` |
| `task_counted` | Default `false` at `1069`, applied by the aggregate assignment; task path sets it at `5294` |
| **`arena`** | **Read/reused from existing job state; not freshly initialized by claim or dispatch** |

The exception is directly visible:

> `connection.zig:5155`: `break :blk job.arena;`

**[LOGIC]** Initializing the arena only in `claimSlot` would still be too late. `borrowHeaderListAlloc` uses it before slot claim (`3562–3571`). Teardown also requires every arena to be valid:

> `connection.zig:1845`: `for (self.handler_jobs) |*job| job.arena.deinit();`

**[LOGIC]** To remove the large loop safely:

1. Add a compact, initialized bitmap indicating which job arenas exist—32 bytes for 256 slots.
2. Initialize an arena on its **first decode lease**, or on non-leased dispatch before any reset/read.
3. Preserve a decode-leased arena when dispatch initializes the remaining job fields.
4. Guard arena cleanup with the bitmap, including header-only/rejected requests and dispatch rollback.
5. Keep `HandlerSlot`, joins and lease-directory initialization. Their existing values are read during allocation and reuse.
6. Update direct job-construction test helpers: `testTrapDoubleFinalize` and `trapAdmitJob` currently depend on boot-initialized arena/default state (`6725–6743`, `6786–6807`).

**[LOGIC]** This removes first-touch work, not the backing array’s reservation. The declared job size is 648 bytes: 256 jobs occupy **162 KiB**, whereas 32 used jobs occupy **20.25 KiB** before page rounding. Even writing only each arena header touches pages throughout the 648-byte-stride array.

## 4. Outbound copy destinations

**[LOGIC]** Let:

- `W = WIRE_CHUNK_SIZE = 16,704` bytes.
- `N = inbound_wire_chunks_per_connection`, currently 8.
- `B = outbound_bytes_per_stream`, currently 64 KiB.

The traced destinations are distinct:

| Destination | Allocation and size | Lifetime |
|---|---|---|
| Pending DATA slab | `Server.init:197–203`; `outbound_slabs_per_server × B` | Server-wide boot pool; rented per pending writer |
| Encoded DATA/HEADERS frame | `Session.allocFrame:274–280`; connection frame pool is `frame_slabs_per_connection × frame_slab_bytes` | Frame lease until sink consumes it; GPA fallback for oversized/exhausted leases |
| Batch accumulator | `Connection.init:1550`; **16 KiB** `plaintext_scratch` | Connection-local reservation |
| Transport write chunk | `Connection.init:1557`; **N × W = 130.5 KiB** | Connection-local pool; lease owned by transport after handoff |
| h2c writer buffer | `WritePump.run:331–332`; **W** | WritePump-local buffered-writer storage |
| TLS ciphertext staging | `Pump.start:1424` aliases `Conn.out_buf` | Existing TLS connection buffer, **16,645 bytes** |

**[LOGIC]** The DATA path is:

`Request.arena → pending slab → encoded frame → optional batch scratch → write chunk → transport`

Specifically:

- `enqueueDataBytes` copies into the rented slab (`fair_scheduler.zig:670`).
- `emitOneData` calls `Session.makeDataFrame`; `encodeDataFrameInto` copies DATA behind the nine-byte frame header (`session.zig:120–130`, `526–554`).
- Remaining pending bytes are compacted **within the same slab** (`fair_scheduler.zig:902`); this is not another allocation.
- Both sinks call `EmitBatch.copyFrame`, which copies into `plaintext_scratch` (`emit_batch.zig:125`).
- Both `flushBatch` functions take a borrowed view of that scratch and pass it to `queueWire`. **They do not allocate another batch buffer.**
- `queueWire` selects the on-wire slice and calls `sendAccountedWire`; the latter performs the transport-storage copy:

> `connection.zig:4993`: `@memcpy(lease.bytes[0..src.len], src);`

Unticketed DATA can bypass the batch accumulator. The h2c buffered writer may introduce another buffered copy; not every vectored write necessarily copies through that buffer.

**[LOGIC]** These are not all server-pooled bytes. The global DATA pool is **256 MiB plus its bitmap**, counted once. The frame pool is **128 KiB per connection**, batch scratch **16 KiB**, and write storage **130.5 KiB**. The read pool is another **130.5 KiB**, although it is not an outbound destination.

**[LOGIC]** Preserve the distinction between P3’s pending-DATA invariant and the current transport/frame implementation. `Session.allocFrame` has a GPA fallback; `leaseWriteChunk` also explicitly contains:

> `connection.zig:4941`: `const buf = self.config.gpa.alloc(u8, n) catch return error.OutOfMemory;`

A DATA payload above 503 bytes cannot fit the default 512-byte frame slab. Consequently, the measured JSON path includes temporary frame allocations, not just first touches of reserved connection buffers. Smaller pools must not be justified by leaning harder on these escape paths.

**[LOGIC]** There is also an important ownership trap: despite the comment inside `ReceiptSink.flushBatch`, TLS does **not** necessarily consume the borrowed plaintext synchronously. `pushWriteChunk` stashes the owned chunk in `pump.carried` or its channel (`4871–4879`); encryption occurs later. Simply deleting the transport copy would permit the scratch buffer to be overwritten before encryption.

**[PREMISE: P1 allows scheduler/sink changes; P3 remains mandatory]** I would make the accumulator an **owned write-chunk lease**, encode DATA directly into its available space, and transfer that lease at flush. That removes the separate batch buffer and the temporary DATA-frame copy/allocation. Session remains responsible for framing and flow-control debit; destination capacity must be secured before debit.

Keep the 16 KiB packing policy, ticket chains, control occupancy, terminal ordering and scheduler-only wire exit. Pool exhaustion must stop draining and let the actor service completions—not allocate or wait indefinitely under `session_mu`. The capacity proof must include the builder, queued chunks, pump-held chunks and terminal headroom. TLS already documents that one scheduler pop can produce **two pushes** (`tls.zig:1126–1143`).

**[LOGIC]** Keep the existing server DATA pool’s lowest-free-bit reuse (`slab_pool.zig:93–112`). It already avoids FIFO rotation first-touching every slab. Returning a slab, however, does not reclaim its resident pages.

## 5. Defaults and their implications

**[PREMISE: ordinary connections need substantially fewer than 64 simultaneous streams]** Use **64 streams per connection**, not 256, as the default profile. That accommodates the measured `-m 32` workload plus substantial SSE/page-load headroom. Deployments actually needing hundreds of simultaneous streams should configure that explicitly; lazy initialization still benefits them.

Suggested initial settings:

| Setting | Current | Proposed | Relevant capacity |
|---|---:|---:|---|
| Streams/connection | 256 | **64** | Jobs: 162 → **40.5 KiB**; slots: 8 → **2 KiB** |
| Wire chunks/direction | 8 | **4**, with the lease-capacity redesign | Read + write storage: 261 → **130.5 KiB** |
| Intent entries | 512 | **256**, preserving terminal reserve | Two arrays: `1024 × sizeof(Intent)` → `512 × sizeof(Intent)` |
| Frame slabs | 256 × 512 B | **Keep initially** | **128 KiB** reserved; lowest-free reuse limits sparse touching |
| Pending DATA/stream | 64 KiB | **Keep** | Boot pool remains **256 MiB server-wide** |
| Control entries/bytes | 256 / 64 KiB | **Keep initially** | Do not erode error/control headroom to chase DATA RSS |

**[LOGIC]** For just jobs, slots, read/write storage, frame storage and existing batch scratch, those values change capacity from **575 KiB to 317 KiB per connection**. That is **258 MiB less capacity at 1,024 connections**, not a prediction of equivalent RSS savings. Removing separate batch scratch subtracts another 16 KiB per connection. Arenas, maps, intents, tickets, allocator overhead and transport state are additional.

**[LOGIC]** The stream limit must also reach the wire. `frame.serverPrefaceSettings` hard-codes `max_concurrent_streams = 256` (`frame.zig:523`), and `Session.emitServerPreface` uses that array (`session.zig:568`). Changing only `Limits` would leave clients following an advertised capacity the server does not admit. Generate SETTINGS from the effective limits.

**[PREMISE: “thousands of connections” means thousands of simultaneous SSE handlers]** Keep global admission independently sized. The current 4,096 server streams provide only four streams per connection averaged over 1,024 connections, including long-lived SSE. Do not lower that to reduce memory. Raising it also requires raising reaper capacity and the outbound slab count: boot validation requires at least one DATA slab per admitted stream. At 8,192 streams, 64 KiB slabs reserve **512 MiB globally**.

**[LOGIC]** Two other defaults should not be mistaken for measured h2c RSS:

- `tls_stream_bytes = 256 KiB` is a bound allowance, not a 256 KiB allocation in the traced TLS path. Lowering that number alone saves no resident buffer.
- HTTP/1 reserves **560 KiB** just for the three head-sized and two body-sized regions. Server protocol branching means those are **not** simultaneously allocated for an h2c connection (`server.zig:398–405`). Under G2, H1 body storage should become demand-sized/reclaimable too, but that does not explain this h2c measurement.

## 6. Where this design is worse

**[PREMISE: sustained, uniformly busy connections]** Today’s retained arenas amortize allocation and page faults. Zero-retention scratch repeatedly pays allocation, release and subsequent first-touch costs. High-rate small JSON requests may lose throughput and tail latency even while idle-after-burst RSS improves.

**[PREMISE: large renders synchronize across connections]** The 128 MiB global scratch ceiling deliberately rejects work that the current workload may have accommodated by consuming much more memory. Streaming reduces materialization, but cannot eliminate memory required by a renderer’s data model.

**[PREMISE: connections genuinely need more than 64 concurrent streams, or carry many permanent SSE streams]** The lower stream default can delay page loads or force additional connections. Sixty-four permanent SSE streams can occupy all sixty-four slots. That deployment should retain a higher configured limit rather than pretend the smaller default is universally better.

**[LOGIC]** G2 does not make live memory reclaimable while pointers still exist. Request metadata, persistent SSE state, blocked-handler buffers and leased outbound storage remain live. The API must make render-scope completion practical; otherwise reclamation merely moves from connection teardown to another event that never happens.

**[LOGIC]** Finally, fewer copies and fewer retained bytes are different achievements. The supplied inclusive fault shares cannot establish an RSS reduction percentage, and freeing allocator storage does not prove OS reclamation. The decisive measurement is **large-fragment burst → return to idle while SSE stays connected**, alongside allocation counts and mixed-stream latency—not only peak JSON throughput.

