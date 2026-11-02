# Server-wide resource inventory — starh2 @ 839dd69 (t-3737 spike, orphan handlers)

Worktree: `/Users/rsaunderson/Source/mine/starh2-wt-3737`. Read-only. Nothing was built or run.

Labels:
- **[code f:l]** — read in the code at that file and line. A short quote is given where the exact text matters.
- **[inf]** — inference. I reasoned from code, but did not run it. Each one is a hypothesis to test.
- **[arith]** — arithmetic from the defaults. The inputs are code; the product is mine.

Defaults come from `Limits` in `src/core/limits.zig:154-350`.

---

## 0. Top findings

1. **An H2 stream that sends HEADERS without END_STREAM, and then nothing, has no timer at all.** [code]
   - The body idle clock is armed only by a DATA frame without END_STREAM:
     `src/core/session.zig:1674-1678` `if (!hdr.flags.end_stream) { putBounded(&self.body_idle_ns, ...) }`. It is the only `put` into `body_idle_ns`.
   - HEADERS without END_STREAM goes to `pending_headers` and is not dispatched: `src/core/session.zig:2179-2186`.
   - The connection idle timer does not fire while a stream is open: `src/edge/connection.zig:5715` `if (self.session.active_streams != 0 ...) { self.idle_since_ns = null; return; }`.
   - `field_block_timeout_ns` covers only an unfinished header block (CONTINUATION): `src/core/session.zig:1409-1414`.
   - So the stream holds one server stream token and one 64 KiB request-pool block for ever [inf]. 16 connections x 256 such streams = 4,096 = `max_streams_per_server`, and 4,096 x 64 KiB = 256 MiB = `request_bytes_per_server` [arith]. One client, 16 TCP connections, 4,096 HEADERS frames, no further traffic. After that, every new H2 stream on the server is refused and every H1 request gets 503 [inf]. **This is the cheapest full denial I found, and it needs no broken route.**
2. **An orphan escapes every per-connection cap.** At teardown, `orphanSlot` gives the connection back its request bytes (`src/edge/connection.zig:5066-5069` `b.uncharge(link.arena.conn_charged)`). After that the arena charges no connection: `src/edge/request_pool.zig:707-714` (`chargeConn` returns true when the gate is closed). The orphan keeps a task token (`src/edge/connection.zig:1598-1601`), its request blocks, its stack, its encoder (if any) and its balancer count. So one client that closes connections serially, on a route that ignores cancel, adds 256 orphans per connection, with no per-client limit [inf]. 16 connections fill all 4,096 task tokens [arith]. The orphans' first blocks alone are 4,096 x 64 KiB = 256 MiB, which is the whole request pool [arith]. The pool is then full for every H2 request, including complete (inline) routes [inf].
3. **Task tokens are shared by HTTP/2 and HTTP/1.1.** H1 takes one per task job (`src/edge/h1.zig:828-836`). When the orphans hold them all, H1 task routes get 503 too [inf].
4. **HTTP/1.1 with a cancel-ignoring route pins a reaper worker and the connection slot.** The reaper worker calls `JoinHandle.cancel`, which waits until the task stops (`src/edge/connection.zig:1785-1792`, `:1805`). The H1 connection waits for that completion in `waitDispatch`, `reaping = true` (`src/edge/h1.zig:1127-1130`, `:1158-1165`). So each such abandoned H1 request holds 1 of 64 reaper workers, 1 of 1,024 connection slots, 1 stream token and 1 task token until the handler ends [inf]. 64 of them stop all H1 cancellation; 1,024 stop `accept` on every endpoint [inf].
5. **`accept` failing with EMFILE ends that endpoint's accept loop for the life of the process.** `src/edge/server.zig:617-623` `else => { return; }`. zio maps EMFILE to `error.ProcessFdQuotaExceeded` (`zig-pkg/zio-.../src/os/net.zig:893`). Nothing in starh2 raises `RLIMIT_NOFILE` (grep of `src` and `examples` finds no `RLIMIT`/`setrlimit`). With a Linux soft limit of 1,024 and `max_connections = 1_024`, one client opening about 1,000 connections could stop every endpoint for good [inf; check the soft limit on the deployment, and whether the io_uring path maps it the same way — `io_uring.zig:1272` maps `.MFILE` to `SystemResources`, which is also a `return`].
6. **No limit keys on the peer address.** `Request.peer` exists (`src/http/request.zig:58-64`, filled at `src/edge/connection.zig:7815` and `src/edge/h1.zig:868`), but nothing in `src` reads it except to hand it to the handler [code: grep for `peer`, `getpeername`, `per_ip` in `src`].
7. **A handler that never yields stops its executor.** Under the default `zio.RuntimeOptions` there is ONE executor (`zig-pkg/zio-.../src/runtime.zig:155` `executors: ExecutorCount = .exact(1)`). `examples/datastar_sse.zig:33` uses that default. One CPU-bound or blocking request then freezes the whole server [inf: zio coroutines are cooperative].
8. **`Server.serve` waits for orphans with no limit.** `src/edge/server.zig:411-415`. One handler that never ends keeps SIGTERM from finishing [code]. `deinit` asserts zero (`server.zig:752`).

---

## 1. Resources in `Limits` and `GlobalAccounting`

`GlobalAccounting` is `src/edge/connection.zig:1092-1189`. It is built in `Server.init` at `src/edge/server.zig:290-295`:
`.max_streams = max_streams_per_server`, `.reaper_capacity = cancellation_reaper_jobs`, `.max_outbound_bytes = outbound_bytes_per_server`, `.max_handshakes = concurrent_tls_handshakes`. Every counter is a CAS loop; release is `fetchSub` + assert [code].

### R1. Connection admission — `max_connections` = 1,024

1. **Defined** `src/core/limits.zig:158`. Counter `Server.active_connections` (`src/edge/server.zig:143`).
2. **Taken / released**
   - Room is checked before `accept`: `admissionRoom`, `server.zig:434-439`. Taken after `accept`: `admitAccepted` → `tryAdmitConnection`, `server.zig:442-449`, `:417-423`. All endpoints share the one counter.
   - Released when `connEntry` returns, after the slot is recycled: `server.zig:479-497` (`defer self.releaseConnection()`), which wakes parked accept loops (`server.zig:425-429`).
   - A full server does not accept and close. It parks the loop: `awaitAdmission`, `server.zig:455-467`; doc `server.zig:582-605` "the loop does not call accept at all: it parks until a slot is released".
3. **Per-connection cap**: one per TCP connection. Nothing survives the connection except via R5/R6 (orphans do not hold it; a stuck H1 job does, see R6).
4. **One client**
   - (a) one connection: 1.
   - (b) many: up to all 1,024, from one IP, nothing keyed on peer [code, finding 6].
   - How long a connection is held, by protocol:
     - TLS before handshake: ≤ `preface_timeout_ns` 5 s (`server.zig:564-575`).
     - h2c before preface: ≤ 5 s (`connection.zig:4532-4535`).
     - H1 waiting for a head: ≤ `field_block_timeout_ns` 5 s, absolute from the start of the head (`src/edge/h1.zig:473`). This also bounds idle keep-alive.
     - H2 with no open stream: `connection_idle_timeout_ns` 75 s, then two-phase GOAWAY (1 s phase-1, then phase-2 ends at once when nothing is left; `connection.zig:4642-4651`) [code].
     - H2 with one HEADERS-without-END_STREAM stream: **no limit** (finding 1).
     - H1 slow body: idle 15 s per read, re-armed on every byte (`h1.zig:561-573`). 1 byte per 14 s holds it for ever [inf].
     - H1 with a stuck task handler: until the handler ends (R6).
   - So 1,024 cheap connections, each with one open H2 stream or a 1-byte-per-14-s H1 body, hold every slot indefinitely [inf].
5. **When exhausted**: every accept loop on every endpoint parks. New connections wait in the kernel queue (`listen_backlog` 4,096, capped by `somaxconn`), then the client's connect times out. All protocols: H2 task, H2 complete, H1, TLS [inf from `admissionRoom`].
6. **Unusual client behaviour?** Yes, but cheap: hold sockets open, or send one HEADERS frame without END_STREAM.

### R2. Connection slots — `SlotPool`, capacity `max_connections`

1. **Defined** `src/edge/conn_slot.zig:135-170`. Created in `Server.init`, `server.zig:281` with `slot_boot_bytes` = max over endpoints of `Limits.slotBootBytes(mode)` (`limits.zig:475-480`). Boot size not computed here (needs a build).
2. **Taken** in the accept loop, `server.zig:640` (`self.slots.take()`), only for h2c and TLS (h1c takes none). **Released** by `recycle` in `connEntry`, `server.zig:487-490`. A slot that fails the clean check is quarantined for the life of the server (`conn_slot.zig:265-280`, `std.log.warn`). Safe builds only (`conn_slot.zig:117-121`).
3. One per H2/TLS connection; a TLS connection that ALPN sends to H1 keeps its slot (`conn_slot.zig:73-76`).
4. Same as R1.
5. When `take` fails (`SlotsExhausted` or OOM), the accepted connection is closed at once (`server.zig:640-645`) [code]. Normally unreachable because R1 is checked first; reachable only if slots are quarantined [inf].
6. No.

### R3. TLS handshake slots — `concurrent_tls_handshakes` = 128

1. **Defined** `limits.zig:225`. Counter `GlobalAccounting.active_handshakes`, `connection.zig:1112-1113`.
2. **Taken** with the connection slot in `admitAccepted` for TLS endpoints only (`server.zig:444-447`). **Released** right after the handshake ends or fails (`server.zig:513-527`) or on any failure before (`server.zig:503-507`, `:641`, `:692`).
3. One per TLS connection, for at most `preface_timeout_ns` = 5 s (`server.zig:564-575` `zio.withTimeout(...)`).
4. (a) 1. (b) 128 at once from one IP. A client needs no TLS at all: it opens 128 TCP connections to the TLS port and sends nothing. To keep them held: 128 new connections every 5 s = 25.6 connections/s [arith]. Each also holds an R1 slot for those 5 s.
5. TLS accept loops park (`admissionRoom`, `server.zig:436`). H2-over-TLS and H1-over-TLS clients cannot connect. h2c and h1c endpoints are not affected [code].
6. Yes, but trivial (open sockets, stay silent). Also the CPU side: full handshakes cost the server public-key work. Only the 128 concurrency bound applies; there is no rate bound [inf].

### R4. Server stream admission — `max_streams_per_server` = 4,096

1. **Defined** `limits.zig:160`. Counter `GlobalAccounting.active_streams`, `connection.zig:1093-1094`.
2. **H2 taken** when a new stream's HEADERS is decoded: `src/core/session.zig:2041-2062` (`h.tryAdmit(h.ctx)` → `tryAdmitStream`, hook at `connection.zig:2327-2333`). **H2 released** when the stream reaches `closed`: `releaseConcurrency`, `session.zig:1120-1126`, and at session deinit for the rest (`session.zig:660-663`). A peer RST releases it at once, even if the handler is still running.
   **H1 taken** at dispatch, after the body is read: `h1.zig:811-816`. **Released** in `finishSlot` (`h1.zig:1191-1204`).
3. Per connection: `max_streams_per_connection` = 256 (`limits.zig:159`, checked first at `session.zig:2043`, advertised in SETTINGS at `session.zig:881`). H1: 1. An orphan holds no stream token (session deinit releases it) [code: `session.zig:660-663`].
4. (a) 256 per H2 connection. (b) 4,096 / 256 = 16 concurrent connections fill it [arith]. Serially, nothing survives the connection.
   - Held for ever by HEADERS without END_STREAM (finding 1).
   - Held for 15 s per DATA frame by a slow body (body idle timer, `session.zig:1421-1433`): 1 DATA frame per stream per < 15 s, so about 4,096 / 14 ≈ 293 frames/s to keep all of them [arith].
   - Held by a slow reader: a stream with pending response bytes stays open; slow-consumer (30 s) resets it only if no byte moved (`connection.zig:3500-3555`). One WINDOW_UPDATE per stream per < 30 s defeats it [inf].
5. H2: a new stream gets RST_STREAM REFUSED_STREAM (`session.zig:2049-2052`, `refuseStream` → `session.zig:1210-1212`). H1: 503 and close (`h1.zig:812-815`). All routes on both protocols, task and complete. TLS handshake and accept are not affected.
6. Finding-1 variant: one HEADERS frame per stream, nothing else. No broken route needed; the route does not even need to exist (the request is never dispatched).

### R5. Task-handler tokens — `reaper_capacity` = `cancellation_reaper_jobs` = 4,096

1. **Defined** `limits.zig:248`; validation requires `cancellation_reaper_jobs >= max_streams_per_server` (`limits.zig:535`, again `server.zig:177`). Counter `GlobalAccounting.reaper_reserved`, `connection.zig:1095-1101`.
2. **H2 taken** in `tryDispatch` for any non-complete route: `connection.zig:7731-7741`. Moves into the link: `link.task_token = reaper_reserved` (`connection.zig:7845`). **Released** only when the last link reference drops: `dropLinkRef`, `connection.zig:1646-1676` (`if (token) a.releaseReaper()`). That is either the actor in `releaseSlot` after the join, or the handler's own task when it was orphaned.
   **H1 taken** in `dispatch`, `h1.zig:827-836`. **Released** in `finishSlot`, `h1.zig:1197-1200`.
3. Per H2 connection: at most 256 live at once, because a handler slot is needed (`handlers` = `max_streams_per_connection`, `connection.zig:2405`; `allocSlot`, `connection.zig:4859`). **After the connection ends, the token stays with the orphan** (`connection.zig:5045-5054` doc, `:5055-5085`). No per-connection or per-client cap applies after that [code + inf].
4. (a) 256 per connection. (b) Serially: close the connection while 256 handlers run on a route that ignores cancel, open the next one. 4,096 / 256 = 16 connections [arith]. Concurrently: the same 16.
   - If the route merely takes a long time without checking cancel (for example a 30 s upstream call), the client keeps all 4,096 tokens with 4,096 / 30 ≈ 137 requests/s, which is about 0.5 connections/s at 256 each [arith].
   - With a cancel-honouring route, the RST path cancels at once (`cancelHandler`, `connection.zig:5117-5131`), so the token is short-lived [inf].
5. H2 task routes: RST_STREAM REFUSED_STREAM (`connection.zig:7734-7739`). H1 task routes: 503 + close (`h1.zig:830-833`). Complete (inline) routes still work on both protocols [code: `complete_inline` skips the token, `connection.zig:7731`, `h1.zig:828`].
6. Needs a route that ignores cancel or runs long. The client only has to close the connection.

### R6. Reaper workers — `cancellation_reaper_tasks` = 64; job queue `cancellation_reaper_jobs` = 4,096

1. **Defined** `limits.zig:247-248`. Workers spawned in `serve`, `server.zig:351-360`. Queue `ReaperPool.jobs`, `connection.zig:1764-1780`.
2. Since 839dd69 only HTTP/1.1 uses them (`connection.zig:23` "reaper worker | pool | HTTP/1.1 only"). **Taken**: `cancelJoin` puts a job, `h1.zig:1206-1228`. If the queue is full it calls `h.cancel()` inline on the connection's own task (`h1.zig:1224-1226`). **Released**: the worker returns after `job.handle.cancel()` returns, `connection.zig:1798-1820`. That call "waits until the task actually stops" (`connection.zig:1785-1790`).
3. One H1 connection queues at most one job at a time. But its connection task then waits for the completion (`h1.zig:1127-1130`) and keeps R1, R4 and R5 [code].
4. (a) 1 worker per H1 connection with a stuck handler. (b) 64 abandoned H1 requests to a cancel-ignoring route pin all 64 workers. Later jobs queue and are never processed while the workers stay blocked [inf]. Serially: each abandoned request also keeps its H1 connection slot, so 1,024 of them also exhaust R1 [inf].
5. Workers gone: no H1 handler is canceled any more. Each later H1 disconnect during a running handler also keeps its connection slot until that handler ends by itself [inf]. A handler that writes will probably still see its terminal cause and return (`h1.zig:1158-1160` sets `.connection_closed` before `cancelJoin`) [inf].
6. Needs a route that ignores cancel. Client only disconnects.

### R7. Request memory pool — `request_bytes_per_server` = 256 MiB (resident cap)

Sub-limits: `request_bytes_per_connection` = 64 MiB (`limits.zig:193`), `request_arena_bytes` = 4 MiB per request (`limits.zig:203`), retain 64 MiB (`limits.zig:211`).

1. **Defined** `limits.zig:196`; pool `src/edge/request_pool.zig:194-237`, created `server.zig:258-262`. Block classes are powers of two, 64 KiB … 4 MiB; each class has `server_bytes / class_size` blocks (`request_pool.zig:16-19`), so only 64 blocks of the 4 MiB class exist [arith].
2. **Taken**:
   - At HEADERS decode, `borrowHeaderListMem` reserves room for the largest decode (`connection.zig:4934-4954`, `l.arena.reserve(need)`). `need` = `hpackDecodeBytes` = 100 x `@sizeOf(HeaderField)` + 32 KiB + 8 (`hpack.zig:662-666`). With `HeaderField` = two slices + 3 bools = 40 bytes [inf], that is 36,776 + 64 header → the 64 KiB class [arith]. So **every open H2 request holds at least 64 KiB**.
   - The body appends into the same arena (`session.zig:1642-1664`). A 256 KiB body sits in a 512 KiB block, so it holds about 64 KiB + 512 KiB [arith from `bodyArenaBytes`, `limits.zig:515-519`].
   - Handler allocations (`Request.arena`) also come from it, up to 4 MiB per request.
   - Charge order: request cap, then connection budget, then server (`request_pool.zig:767-787`).
   **Released**: `releaseLease` (before dispatch, `connection.zig:4976-4980`), or `dropLinkRef` → `arena.release()` (after dispatch, `connection.zig:1661-1667`).
3. Per connection: 64 MiB through `ConnBudget` (`request_pool.zig:604-625`, init at `connection.zig:2496`). **Not after the connection ends**: `orphanSlot` uncharges `conn_charged` (`connection.zig:5066-5069`), then `chargeConn` returns true when the gate is closed (`request_pool.zig:707-714`) and `dropLinkRef` sets `arena.budget = null` (`connection.zig:1655-1657`). An orphan is then capped only by 4 MiB per request and the server cap [code].
4. (a) One live connection: ≤ 64 MiB. With 256 open streams of headers only, it holds 256 x 64 KiB = 16 MiB [arith]. With 256 KiB bodies, about 113 streams reach the 64 MiB cap [arith].
   (b) Concurrently: 4 connections x 64 MiB = 256 MiB [arith]. Or 16 connections x 256 header-only streams x 64 KiB = 256 MiB (finding 1) [arith].
   Serially with orphans: 4,096 orphans x 64 KiB = 256 MiB, or 64 orphans x 4 MiB if the handler allocates its full arena [arith].
5. Over the server cap: HEADERS are decoded into scratch and the stream is answered 503 (`connection.zig:4947-4952`, `Reason.status`, `request_pool.zig:112-117`). After dispatch: refused 503 with REFUSED_STREAM, or 500 for the request's own cap (`connection.zig:7690-7712`). Every H2 route, task and complete, is affected [inf]. H1 is not: it does not use the pool (see R11).
6. Finding-1 variant needs only HEADERS frames. The orphan variant needs a cancel-ignoring route.

### R8. Server outbound byte budget — `outbound_bytes_per_server` = 256 MiB

1. **Defined** `limits.zig:187`. Counter `GlobalAccounting.outbound_bytes`, `connection.zig:1108-1109`.
2. **Taken** at a handler's DATA write as `pending`: `tryReserveOutboundBytes`, `connection.zig:2886-2913`. Moved to `wire` without re-charge when the chunk goes to the pump. **Released** in `applyOutboundRelease` (`connection.zig:2931-2955`): on the write ack, a slow-consumer kill, a reset, a writer failure or teardown.
3. Per connection 4 MiB (`limits.zig:181`), where a full connection budget makes the writer WAIT (`connection.zig:6073-6082` → `parkForBudget`). Per stream 64 KiB (`limits.zig:170`). Released at teardown, so nothing survives the connection [code, `shutdownHandlers` → `publishTeardownWakes`; inf for completeness].
4. (a) 4 MiB. (b) 256 MiB / 4 MiB = 64 concurrent connections, each with 64 streams of 64 KiB unread [arith]. The client sets a zero window (or never sends WINDOW_UPDATE). Slow-consumer kills a stream after 30 s with no progress (`connection.zig:3520-3555`). One WINDOW_UPDATE of 1 byte per stream per < 30 s counts as progress (`last_progress_ns` "advances whenever bytes enter or leave", `connection.zig:3506-3508`) [inf].
5. A server-full write is REFUSED: `error.OutOfMemory`, the stream is reset (`connection.zig:6075-6078`; doc `limits.zig:182-186`). Every H2 handler that writes, on every connection: task writes and complete handlers (`inline_shed`, `connection.zig:6064-6067`). H1 does not use this budget [inf: no `tryReserveOutbound` in `h1.zig`].
6. Needs a route that writes ≥ 64 KiB (or an SSE stream) and a client that withholds flow-control window.

### R9. Outbound slabs — `outbound_slabs_per_server` = 4,096 x 64 KiB

1. **Defined** `limits.zig:279`. Pool `server.zig:249-255`.
2. Rented when a stream first queues DATA, returned when it drains (doc `limits.zig:273-278`).
3. One per writing stream.
4. Bounded by R4: validation requires slabs ≥ `max_streams_per_server` (`limits.zig:590`), so "pool exhaustion UNREACHABLE for a valid config" [code doc]. Not a separate target.
5. Refused writes are counted in `slab_pool_refusals` (`connection.zig:859`) [code]; unreachable by design.
6. —

### R10. Compression contexts — `compression_contexts_per_server` = 128 (only if `response_compression = true`, default false)

1. **Defined** `limits.zig:286`, budget 4 MiB each (`limits.zig:295`). Pool `src/http/brotli.zig:290-340`.
2. **Taken** in `prepareCompression` (`connection.zig:8596-8632`, `pool.tryAcquire()`). **Released** in `finishHandlerJob` (`connection.zig:7943-7950`) and on response end/abort paths (`connection.zig:8739-8753`, `:9169-9171`).
3. One per compressed response, for its lifetime (SSE included). **An orphan keeps it** until the handler ends (`finishHandlerJob` runs at handler end, not at orphaning) [code].
4. (a) Up to 128 SSE streams with `accept-encoding: br`. (b) Same 128 total.
5. Other clients get identity encoding (`brotli.zig:327-331`, `noteIdentityFallback`). Degrades, never refuses [code].
6. Needs a compressible streaming route.

### R11. HTTP/1.1 memory — `h1_head_bytes`, `request_body_bytes`, `Request.arena`

1. **Defined** `limits.zig:216`, `:272`. Allocated per H1 connection from `config.gpa` in `initConn`, `h1.zig:239-262`: head 16 KiB, body 256 KiB, recv 4 KiB, write 4 KiB, header storage, field scratch, name scratch 16 KiB, path 24 KiB, carry 16 + 256 KiB, pending 64 KiB. About 660 KiB per H1 connection [arith; struct sizes are inference]. TLS H1 adds the pump buffers (`h1.zig:298-303`).
2. Taken at connection start, freed at its end.
3. One set per H1 connection; bounded by R1.
4. 1,024 x ~660 KiB ≈ 660 MiB at most [arith].
5. —
6. —
- **`Request.arena` for H1 is an `ArenaAllocator` over `gpa` with no cap** (`h1.zig:279`, handed to the handler at `h1.zig:867`). A handler that allocates per request is unbounded on H1 [code]. AGENTS.md says t-3537 moves H1 onto the pool.
- **The bound undercounts H1** [code]: `terms.h1_head` and `terms.h1_body` count head x 3 + body x 2 (`limits.zig:617-620`). `pending_storage` (64 KiB), `path_q_buf` (24 KiB), the 4 KiB buffers and the H1-TLS pump buffers are not in `resourceUpperBound`.

### Per-connection-only resources (they cannot starve another connection)

All live in the slot's boot buffer and end with the connection [code: `connection.zig:2400-2475`, `session.zig:576-647`]:

| resource | size | where |
|---|---|---|
| open streams | 256 | `session.zig:2043` |
| handler slots | 256 | `connection.zig:2405` |
| parked dispatches (no free slot) | 256 | `connection.zig:2415` |
| request leases | 513 | `connection.zig:2407` |
| frame slabs | 256 x 512 B | `limits.zig:166-167` |
| control pool | 64 KiB / 256 entries + terminal share | `limits.zig:222-223`, `:406-421` |
| intent queue | 520 | `limits.zig:282` |
| write pool / h2c write queue | 8 chunks | `limits.zig:161` |
| stream tombstones | 1,024 | `limits.zig:224` |
| HPACK table | 4,096 B + 128 entries | AGENTS.md |

What they do when full: the connection itself refuses streams or ends with ENHANCE_YOUR_CALM (`session.zig:1227`, `:1238`). Other connections are not affected [inf].

---

## 2. Resources outside `Limits`

### Executor threads

- Count: the embedder's `zio.RuntimeOptions.executors`. Default `.exact(1)` (`zig-pkg/zio-.../src/runtime.zig:155`). `examples/datastar_sse.zig:33` uses the default. `examples/bench_server.zig:1657` uses the physical CPU count.
- Under pinned scheduling a connection's actor, pumps and handlers stay on one executor (`balancer.zig:3-9`; AGENTS.md "Per-connection spawns ... use `zio.spawnInto(connPlacement(), ...)`").
- A handler that never yields (CPU loop, blocking syscall) holds that thread. No preemption [inf: cooperative coroutines]. With 1 executor, the whole server stops, accept loops included. With N executors, N such requests stop every executor [inf].
- A **complete** handler runs on the connection's actor (`connection.zig:7868-7890`). If it blocks, its connection cannot run teardown, so it is never orphaned and keeps R1 for ever [inf].
- The balancer counts handlers, not work (`balancer.zig:24-26`), so it cannot steer away from a spinning executor. Orphans keep their balancer count until they end (`finishHandlerJob`, `connection.zig:7966-7968`) [code].
- `Limits` has no knob for this. Nothing per client.

### Task stacks

- Embedder's `RuntimeOptions.stack_pool`. zio default: `maximum_size` 8 MiB, `committed_size` 256 KiB (`zig-pkg/zio-.../src/runtime.zig:137-140`). The tests use 1 MiB / 64 KiB (`connection.zig:10312-10313`).
- "Committed" is an `mprotect` READ|WRITE region; physical pages come on first touch (`zig-pkg/zio-.../src/coro/stack.zig:61-130`) [code]. The stack grows on a fault up to the maximum [code: `stack.zig:372`].
- No count limit in the stack pool [code: `stack_pool.zig` `acquire` has no cap]. Shrinks every 60 s toward recent demand (`stack_pool.zig` `shrink_interval`).
- The task count bound in `resourceUpperBound` is `max_endpoints + 7 x max_connections + max_streams_per_server + cancellation_reaper_tasks` (`limits.zig:653-659`) = 8 + 7,168 + 4,096 + 64 = 11,336 [arith]. At zio defaults: 11,336 x 256 KiB ≈ 2.8 GiB committed address space, 11,336 x 8 MiB ≈ 89 GiB reserved [arith]. **Stacks are excluded from the bound**: `committed_stack_bytes = 0` (`limits.zig:661-662`).
- **Bound mismatch** [code]: the task term uses `max_streams_per_server` for handlers, but the real cap on live task handlers (orphans included) is `reaper_capacity` = `cancellation_reaper_jobs` (`server.zig:292`). Validation allows `cancellation_reaper_jobs > max_streams_per_server` (`limits.zig:535`). With defaults they are equal.
- Spawn failure (no stack) on H2: RST REFUSED_STREAM (`connection.zig:7906-7914`). On H1: 503 (`h1.zig:929-940`) [code]. A failed stack mmap also logs (`stack.zig` `log.err` in `stackAllocPosix`) [code].
- One client's share: up to 4,096 handler stacks through R5, plus 3 tasks per h2c connection it holds [inf].

### File descriptors

- One per accepted connection, plus listeners, event-loop fds and whatever handlers open. Accepted sockets ≤ `max_connections` + one in-flight accept per endpoint loop (admission is checked before `accept`) [code: `server.zig:615-636`].
- Nothing sets `RLIMIT_NOFILE` [code: grep]. EMFILE stops the accept loop for good (finding 5).

### Kernel accept backlog — `listen_backlog` = 4,096

- `limits.zig:243`; passed to `listen` at `server.zig:337-338`, `:783-791`. Capped by `somaxconn` (Linux default 4,096 on recent kernels, macOS 128) [inf for the OS defaults].
- Fills only while R1 or R3 is full and the accept loop is parked. It is a symptom of R1/R3 exhaustion, not a separate target. Beyond it the kernel drops SYNs; clients see retransmit delays, then timeouts (doc `limits.zig:226-233`).

### Log volume

- In a production build with default runtime flags, the only log line is `std.log.warn("connection slot leaked, not reused: ...")` (`conn_slot.zig:277`), safe builds only. All `diagRawPrint` sites are gated on `diag_park`, `close_reason`, `close_probe` (`connection.zig:527`, `:547`, `:553`), which default to false and are set only by the bench and conformance servers [code]. zio logs a failed stack mmap.
- So a client cannot flood the logs [inf]. The other side: nothing logs a refusal, an orphan or an admission park either. An attack is visible only through counters (`outbound_memory.*`, `RequestPool.snapshot().refused`, `SlotPool.snapshot`, `GlobalAccounting`) [inf].

### Orphans at shutdown

- `Server.serve` polls `orphaned_handlers` every 1 ms with no limit before it stops the reapers and returns (`server.zig:399`, `:411-415`). A never-ending orphan means a never-ending shutdown [code].

---

## 3. Does anything key on the peer address?

No. `peerFromHandle` (`connection.zig:578-596`) fills `Request.peer` for the handler (`connection.zig:7815`, `h1.zig:868`). The doc says it exists "so the two encodings of one client share a key" (`request.zig:58-60`), but no server limit uses it. Every limit above is per connection, per stream, per request, or server-wide [code: grep of `src`]. `Balancer.notePlaced` gets the peer **port** for tracing only (`server.zig:672`).

---

## 4. Cheapest for one client to exhaust

Costs are from the defaults. "Broken route" means a route whose handler ignores cancel, runs long, or never yields.

| # | resource | what the client does | client cost | held for | effect on everyone else | needs a broken route? |
|---|---|---|---|---|---|---|
| 1 | Stream admission (R4) **and** request pool (R7) | 16 H2 connections, 256 HEADERS each without END_STREAM, then silence | 16 sockets, 4,096 frames, once | for ever (no timer) | every new H2 stream REFUSED_STREAM or 503; every H1 request 503 | no |
| 2 | TLS handshake slots (R3) | 128 TCP connects to the TLS port, send nothing | 25.6 connects/s | 5 s each, renewed | TLS endpoints stop accepting (H2 + H1 over TLS); h2c/h1c fine | no |
| 3 | Connection admission (R1) | 1,024 connections, each with one open H2 stream (as #1) or a 1-byte-per-14-s H1 body | 1,024 sockets; ~73 bytes/s for the H1 variant | for ever | all endpoints stop accepting; new clients time out in the backlog | no |
| 4 | File descriptors | same as #3, if soft `RLIMIT_NOFILE` ≤ ~1,024 | ~1,000 sockets | **permanent** (accept loop exits) | endpoint dead until restart | no |
| 5 | Executor threads | 1 request per executor to a CPU-bound or blocking route | 1–N requests | until the handler ends | everything on that executor frozen; whole server at 1 executor | yes |
| 6 | Task tokens (R5) + pool (R7) via orphans | 16 connections x 256 requests, close each connection | 4,096 requests | until handlers end; also blocks shutdown | H2 task routes REFUSED_STREAM, H1 task routes 503; pool full → all H2 503 | yes (ignores cancel or runs long) |
| 7 | Reaper workers (R6) | 64 H1 requests to a cancel-ignoring route, then disconnect | 64 requests | until handlers end | no H1 handler can be canceled; their connections leak (→ R1) | yes |
| 8 | Request pool via bodies (R7) | 4 connections x ~113 streams with 256 KiB bodies, 1 DATA frame per stream per < 15 s | ~113 MB upload, then ~30 frames/s | as long as it drips | all H2 requests 503 | no |
| 9 | Server outbound budget (R8) | 64 connections x 64 streams, zero window, 1-byte WINDOW_UPDATE per stream per < 30 s | 4,096 streams, ~137 frames/s | as long as it drips | every H2 write reset (overload shed) | needs a route that writes ≥ 64 KiB |
| 10 | Compression contexts (R10) | 128 brotli SSE streams | 128 streams | stream life | identity encoding only (no failure) | needs compression on |

#1 also fills R7 by itself, because each pending stream holds a 64 KiB first block [arith]. Rows 1–3 are pure protocol abuse with no help from the application. Rows 5–7 are the "single broken route" class.

---

## 5. Existing defences

| defence | where | what it bounds | gap |
|---|---|---|---|
| `preface_timeout_ns` 5 s | TLS handshake `server.zig:564-575`; h2c preface `connection.zig:4532-4535` | a silent socket before the protocol starts | per connection only; renews with a new socket |
| `field_block_timeout_ns` 5 s | H2 unfinished header block `session.zig:1409-1414` (GOAWAY ENHANCE_YOUR_CALM); H1 whole head, absolute `h1.zig:473` | slow headers | does NOT cover a finished HEADERS without END_STREAM |
| `request_body_idle_timeout_ns` 15 s | H2 `session.zig:1421-1433` (RST CANCEL), armed only at `session.zig:1675`; H1 `h1.zig:561-573` | a body that stops | re-armed by every DATA frame or byte; never armed before the first DATA |
| `slow_consumer_timeout_ns` 30 s | `connection.zig:3500-3555` | a stream with pending bytes and no progress | 1-byte WINDOW_UPDATE counts as progress [inf] |
| `connection_idle_timeout_ns` 75 s | `connection.zig:5707-5730`, deadline `:3784-3793` | an H2 connection with no open stream | any one open stream disables it; handlers do not count |
| `graceful_drain_timeout_ns` 30 s + phase-1 1 s | `connection.zig:5649-5690` | handlers after GOAWAY | ends in orphaning, not in a stop |
| `close_flush_timeout_ns` 500 ms | `connection.zig:5286` | a closing connection whose peer stopped reading | — |
| Frame rate limits (per connection) | `rates.zig:86-108`, `admit` `:128-147`, enforced `session.zig:1386-1395` (`chargeRate`) | RST 1,000 burst + 100/s; SETTINGS / PING 100 + 10/s; other non-DATA 10,000 + 1,000/s; WINDOW_UPDATE 100,000 + 10,000/s | HEADERS and DATA are exempt (`rates.zig:121-129`); buckets reset with each new connection |
| Refused/RST flood → ENHANCE_YOUR_CALM | `session.zig:1215-1240` (`queueRst`: `rstOwed` + intent room) | a peer that keeps opening past the stream limit | per connection |
| CONTINUATION count 16 | `session.zig:1967` | CONTINUATION floods | — |
| Per-connection caps | streams 256 `session.zig:2043`; handler slots 256 `connection.zig:2405`; request bytes 64 MiB `request_pool.zig:604-625`; outbound 4 MiB `connection.zig:2886-2913`; per stream 64 KiB | one connection's share | gone after the connection ends (orphans) |
| Server caps | R1–R10 above | totals | none is per client |
| Admission parks instead of refusing | `server.zig:582-605`, `:455-467` | bursts | a full server stops accepting everything |
| Cancel on RST / teardown | `cancelHandler` `connection.zig:5117-5140`; `shutdownHandlers` `:5558-5617` | handlers that honour cancel | a handler that ignores it becomes an orphan (H2) or pins a reaper worker (H1) |

---

## 6. Side findings (not resources, but found on the way)

- **Stale comment** [code]: `limits.zig:235-237` says "the accept loop closes a connection over `max_connections` or `concurrent_tls_handshakes` instead of waiting for a slot". The accept loop now parks (`server.zig:582-589`, `:455-467`).
- `tls_handshake_scratch_bytes` (`limits.zig:246`) is counted in the bound (`limits.zig:633`) but no code outside `limits.zig` reads it [code: grep]. The bound may over-count 128 x 256 KiB = 32 MiB [arith].
- Nothing in starh2 can raise or check `RLIMIT_NOFILE` against `max_connections` at boot. A boot check would turn finding 5 into a configuration error [inf].
- **Possible design direction** (inference, for the panel): each server-wide counter that a client can fill without spending anything (R1, R4, R5, R7) has no owner below "server". A per-peer quota on the same CAS counters would make rows 1, 3, 6 and 8 cost one client its own share only. `Request.peer` already normalises v4-mapped v6, so the key exists. Separately, the HEADERS-without-END_STREAM gap is a missing deadline, not a missing quota: arming the body idle clock at HEADERS time closes row 1's "for ever".
