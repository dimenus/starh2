# Resource fairness in established HTTP servers (t-3737 research)

Researched 2026-11-02. Go and hyper/h2 were read from local source (go1.27.3, hyper 1.11.0, h2 0.4.18, hyper-util 0.1.20). Envoy (v1.39.6), nginx (release-1.31.8), HAProxy (v3.3.0), h2o (master 7bbb3e39), Kestrel (aspnetcore v9.0.0, checked at v10.0.14) and Pingora (0.9.0) were read from shallow clones and docs by three research subagents. Every claim in the sections below has a URL. "UNVERIFIED (memory)" and "INFERENCE" mark everything else. **None of the servers was run.** Everything here is about what the code and docs say, not observed behaviour.

## Synthesis

### What every server agrees on
1. **Every built-in HTTP/2 DoS defence is per connection, never per client.** Go, h2, Kestrel, Envoy, nginx, HAProxy and h2o all count streams, resets, CONTINUATION frames or control frames on one connection and close that connection. Per-client protection only exists as a separate, opt-in, general limiter keyed on an identity the operator configures.
2. **No server has a per-client limit on by default.** nginx `limit_conn`/`limit_req`, HAProxy stick-tables, Envoy `local_ratelimit` with a `remote_address` descriptor, and the ASP.NET Core rate-limiting middleware are all opt-in. Go, hyper and h2o (core) have none at all.
3. **Client identity is the socket peer by default.** Forwarded headers are trusted only by explicit config: Envoy `xff_num_trusted_hops` = 0, Kestrel `ForwardLimit` = 1 with only loopback proxies trusted, nginx `realip` is not even built by default. PROXY protocol replaces the peer address at the listener (Envoy, HAProxy `accept-proxy`, nginx `proxy_protocol`).
4. **Nobody groups IPv6 by /64 out of the box.** HAProxy can do it with `ipmask(32,64)` (and fails silently without the v6 mask). Envoy has `masked_remote_address`. nginx has no prefix masking.
5. **Graceful shutdown waits forever by default in the C servers.** nginx (no default `worker_shutdown_timeout`), HAProxy (no default `hard-stop-after`), h2o (`http2-graceful-shutdown-timeout` 0), Go `Shutdown` (until the caller's context expires), and hyper-util `GracefulShutdown::shutdown()` (no timeout). Kestrel (30 s) and Pingora (5 s after a 300 s grace period) are the exceptions with built-in deadlines.

### The key design split: when does a cancelled stream stop counting?
This is the question that decides Rapid Reset and MadeYouReset, and it is the one that matters most for a runtime that cannot kill tasks.

| Model | Who | Rapid Reset / MadeYouReset outcome |
|---|---|---|
| **Slot is held until the handler actually returns** | Go (`curHandlers`, queue capped at 4× and then GOAWAY), Kestrel (`_streams.Count` vs `MaxTrackedStreams` = 2× streams, then ENHANCE_YOUR_CALM), HAProxy (`nb_sc` counts until the upper layer releases; it stops reading HEADERS when over the limit) | Not affected by MadeYouReset, by construction: it does not matter who reset the stream (Go's and HAProxy's statements to CERT) |
| **Slot is freed at reset, the work is forcibly dropped** | hyper/h2, Pingora (Rust: dropping the future cancels at the next `.await`) | Safe only because drop is real cancellation; h2 adds counters anyway (pending-accept resets 20, local error resets 1024) |
| **Slot is freed at reset, the work keeps running** | Tomcat, Netty, Varnish, h2o before fix (CERT VU#767506 "Affected") | This is exactly the MadeYouReset bug |
| **Reset-rate heuristics** | Envoy (premature reset ≤1 s with no response, ≥50% of ≥500 streams), nginx (≤2× streams per event-loop pass), h2o (reset budget + 100 ms delay) | Mitigates; depends on thresholds |

**For starh2 (a cooperative runtime), only the first row is sound.** A cancel request does not free anything. The concurrency slot, and the memory the task holds, must stay charged to the connection (and to the client) until the task really finishes. Freeing the slot at RST_STREAM time is the row that CERT calls vulnerable.

### Global overload: only Envoy has a real model
- **Envoy's overload manager** is the only general mechanism. It is off by default. Monitors: heap, cgroup memory, CPU, global downstream connections. Actions fire at operator-chosen thresholds; there is no built-in order, the operator creates one by giving each action a different threshold. Typical ladder: reduce timeouts (scaled) → close idle connections / disable keep-alive → stop accepting requests → stop accepting connections → shrink heap.
- **Victim choice** exists only in Envoy `reset_high_memory_stream`: streams above 2^N bytes go into 8 power-of-two buckets, the largest buckets are reset first, at most 50 per run, HTTP/2 only. It works because Envoy charges every buffer to a per-stream account.
- **Everyone else** has only connection-count caps: nginx `worker_connections` (closes idle keep-alive connections first, then refuses), HAProxy `maxconn` (leaves new connections in the kernel listen queue), h2o `max-connections` 1024 plus `soft-connection-limit` (closes idle), Kestrel `MaxConcurrentConnections` (default unlimited; over the cap the socket is closed with no HTTP response). Go and hyper have none (Go has `netutil.LimitListener` outside the standard library).
- **Common first action across servers: close idle connections.** nginx, h2o and Envoy all do it before refusing new work. It is the cheapest action because it cancels no work.

### Per-route / per-handler concurrency: almost nobody
- Only HAProxy has a real per-backend model: server `maxconn` counts requests, `maxqueue` and `timeout queue` (503).
- Envoy's circuit breakers, admission control and adaptive concurrency are per upstream cluster. That is the proxy equivalent of per route, but it is not per handler.
- Kestrel uses the rate-limiting middleware per endpoint (`RequireRateLimiting` with a concurrency limiter, `PermitLimit` + `QueueLimit`; default rejection 503).
- nginx `limit_conn` in a `location` rejects at once, with no queue.
- Go, hyper and h2o have nothing built in. Go's `TimeoutHandler` is a trap: on timeout it returns while the inner goroutine keeps running, so the stuck work drops out of the HTTP/2 handler count (INFERENCE from the code, not tested).

### A handler that never finishes after its client left
- **Proxies** (Envoy, nginx, HAProxy, h2o, Pingora) can always cancel: the "handler" is an upstream request, and they reset or close it when the client leaves. nginx `proxy_ignore_client_abort` is off by default; HAProxy `abortonclose` is on by default for HTTP backends.
- **Runtimes with real cancellation** (hyper, Pingora) drop the future at the next await.
- **Cooperative runtimes** (Go, Kestrel) only signal: Go cancels the request context, Kestrel fires `RequestAborted`. The handler keeps its goroutine/task, its memory and its concurrency slot until it returns.
- **Shutdown with stuck handlers in cooperative runtimes:** Go `Shutdown` returns `ctx.Err()` when the context expires and leaves the handlers running; Kestrel aborts connections at its 30 s timeout, waits at most 1 s more, and returns while handlers may still run. **Both give up and return; neither waits forever, and neither can stop the handler.** Process exit is the only hard stop.

### HTTP/2 DoS limits added per CVE
| | Rapid Reset (2023) | CONTINUATION (2024) | MadeYouReset (2025) |
|---|---|---|---|
| Go | running handlers ≤ MaxConcurrentStreams (250), queue ≤ 4×, then GOAWAY | header bytes budget (MaxHeaderBytes 1 MiB) across CONTINUATION | not affected (same handler count) |
| hyper/h2 | unaffected; pending-accept reset streams ≤ 20 (since 0.3.18) | CONTINUATION count cap from header/frame size (0.4.4) | local error resets ≤ 1024; one missed path fixed 0.4.11 |
| Kestrel | ENHANCE_YOUR_CALM count 20 (×5 window), flow-control queue 800 | decoded headers > 2× limit aborts | not affected (CERT); MaxTrackedStreams 2× streams |
| Envoy | premature-reset ratio per connection (500 streams, 50%) | fixed 1.29.2/1.29.3 | no advisory; CERT "not affected" |
| nginx | ≤ 2× max streams per event-loop pass, keepalive_requests 1000 | header buffer size | no statement found |
| HAProxy | not affected (stream held until released) | excess CONTINUATION counted as glitches | not affected (CERT) |
| h2o | reset budget + `http2-dos-delay` 100 ms | `H2O_MAX_REQLEN` | affected, fixed in 579ecfa (server resets use the same budget) |

All per connection. Since one client can open many connections, **a connection-only defence turns into a per-client defence only when combined with a per-client connection cap.** No server gives you that combination by default.

### What fits a cooperative runtime where tasks cannot be killed
Recommendations (design judgement, not measured):
1. **Count work, not streams.** Charge every handler task to its connection and to its client identity from spawn until the task actually returns. Never release on RST_STREAM, GOAWAY or connection close. This is the Go / Kestrel / HAProxy model, and it closes Rapid Reset and MadeYouReset with one rule.
2. **Bound the debt.** When a connection's running + cancelled-but-not-finished tasks exceed a cap (Go: 4× streams; Kestrel: 2×), stop reading new HEADERS (HAProxy) or send GOAWAY ENHANCE_YOUR_CALM (Go, Kestrel). Apply the same cap per client identity, so many connections from one client do not get around it.
3. **A per-client connection cap is the missing piece everyone leaves to config.** Make it built in, keyed on the peer address with IPv6 grouped by /64, and with PROXY protocol / trusted-proxy identity as explicit opt-ins. Fail loudly on a misconfiguration, not silently (the HAProxy `ipmask` trap).
4. **Per-handler (per-route) concurrency is a library primitive worth having.** Almost nobody ships one. A stuck route then fills only its own pool. A cancelled-but-still-running task must keep counting against the route pool too, or the Go `TimeoutHandler` leak comes back.
5. **Overload: copy Envoy's ladder, not its victim choice.** Close idle connections first (no work lost), then stop accepting requests, then connections. Envoy's "reset the biggest stream" only frees memory where reset actually frees memory. In a cooperative runtime a reset only asks; so the victim choice should pick the **client** (or connection) holding the most charged memory and tasks, then refuse its new work and close its idle connections, since the in-flight tasks cannot be reclaimed on demand.
6. **Shutdown: deadline, then report, then return or exit.** Follow Go/Kestrel: GOAWAY, wait up to a deadline, cancel, wait a short extra period, then return with a count of tasks still running. Do not wait forever (the nginx/HAProxy/h2o default), because one handler that ignores cancel then blocks the deploy.
7. **Slow-client timers belong per stream and per connection,** like Kestrel `MinRequestBodyDataRate` / `MinResponseDataRate` (240 B/s with 5 s grace). They turn a slow client into a closed connection, which then frees everything except tasks that ignore cancel; see point 1.

### Where servers disagree
- **Queue or reject at the limit.** HAProxy queues (with a timeout), Go queues handlers (up to 4×), Kestrel's limiter queues if configured. nginx `limit_conn` and Envoy local rate limit reject at once.
- **Reject with what.** Kestrel's rate limiter answers 503 by default, not 429. HAProxy queue timeout is 503. Kestrel's connection cap closes the socket without a response.
- **Reset accounting vs work accounting.** Envoy, nginx and h2o use reset-rate heuristics; Go, Kestrel and HAProxy account for work in progress. The heuristics needed a second fix in h2o for MadeYouReset; the work-accounting servers did not.
- **Shutdown deadline.** None (nginx, HAProxy, h2o, hyper-util) vs a default deadline (Kestrel 30 s, Pingora 5 s, Go: the caller's context).

---

The sections below are the full per-server findings with citations.


## Go net/http (go1.27.3) and its HTTP/2 server

Source read locally from the Go 1.27.3 toolchain (`/opt/homebrew/Cellar/go/1.27.3/libexec/src`), which matches tag `go1.27.3` on GitHub (tag and path checked with `gh api`). In Go 1.27 the HTTP/2 server lives in `net/http/internal/http2`; `golang.org/x/net/http2` is the older external copy. Links below are pinned to the tag.

Base URLs: `S = https://github.com/golang/go/blob/go1.27.3/src/net/http/server.go`, `H2 = https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go`.

### 1. Per-client limit
- **None built in.** `http.Server` has timeouts and `MaxHeaderBytes`, but no per-IP or per-identity limit and no global connection cap. Fields: `ReadTimeout`, `WriteTimeout`, `IdleTimeout`, `MaxHeaderBytes` ([S#L3090-L3118](https://github.com/golang/go/blob/go1.27.3/src/net/http/server.go#L3090-L3118)). `DefaultMaxHeaderBytes = 1 << 20` ([S#L939](https://github.com/golang/go/blob/go1.27.3/src/net/http/server.go#L939)).
- A global connection cap exists only as a separate helper, `netutil.LimitListener` in x/net ([netutil/listen.go#L16](https://github.com/golang/net/blob/v0.60.0/netutil/listen.go#L16)). It blocks Accept; it is not per client.
- No trusted-proxy / X-Forwarded-For / PROXY protocol handling in the standard library (UNVERIFIED (memory): `httputil.ReverseProxy` appends X-Forwarded-For on the outbound side only; nothing parses it on the inbound side).

### 2. Global overload handling
- **None.** No memory monitor, no load shedding. The only built-in bounds are per connection (below).

### 3. Per-route / per-handler concurrency
- **None.** The only concurrency bound is per HTTP/2 connection: the number of handler goroutines (see 5). `http.TimeoutHandler` gives a per-handler *time* limit ([S#L4018-L4035](https://github.com/golang/go/blob/go1.27.3/src/net/http/server.go#L4018-L4035)).
- Trap, read from source: `TimeoutHandler` runs the inner handler in a new goroutine and, on timeout, the outer handler writes 503 and returns ([S#L4058-L4100](https://github.com/golang/go/blob/go1.27.3/src/net/http/server.go#L4058-L4100)). The inner goroutine keeps running with only a cancelled context. Because HTTP/2 counts handlers by the *outer* goroutine (`runHandler` sends `handlerDoneMsg` when the outer returns, [H2#L2300-L2301](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L2300-L2301)), a stuck inner handler no longer counts toward the per-connection handler cap. Inference from reading the code, not tested.

### 4. Handler that never finishes after its client left
- **Cancellation is cooperative, by context.** Go cannot kill a goroutine.
  - HTTP/1: after the request body is read, the server does a background 1-byte read. A read error (EOF, closed conn) calls `cr.conn.cancelCtx()` ([S#L745-L782](https://github.com/golang/go/blob/go1.27.3/src/net/http/server.go#L745-L782), [S#L813-L820](https://github.com/golang/go/blob/go1.27.3/src/net/http/server.go#L813-L820)). The source comment admits a gap: a client that closes after sending a *pipelined* request does not cancel the context until a write fails.
  - HTTP/2: RST_STREAM from the client calls `st.cancelCtx()` and `closeStream` ([H2#L1555-L1568](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L1555-L1568)). `closeStream` also closes the request body pipe and the response writer's close channel, so blocked body reads and writes return errors ([H2#L1600-L1613](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L1600-L1613)). Connection close does the same for every stream (`closeAllStreamsOnConnClose`, [H2#L728-L733](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L728-L733)).
- **What it still holds:** the goroutine, its stack, anything it allocated, and on HTTP/2 a handler slot: `curHandlers` is decremented only when the handler function returns (`handlerDone`, [H2#L2271-L2296](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L2271-L2296)), not when the stream closes. So stuck handlers fill the per-connection handler cap and later requests on that connection queue.
- **Graceful shutdown:** `Server.Shutdown(ctx)` closes listeners, calls the `RegisterOnShutdown` hooks (HTTP/2 registers `GracefulShutdown`, which sends GOAWAY: [http2.go#L71](https://github.com/golang/go/blob/go1.27.3/src/net/http/http2.go#L71)), then polls every ≤500 ms and closes connections that are idle. It waits **indefinitely** for active connections; when `ctx` expires it **returns `ctx.Err()` and leaves the connections and handlers running** ([S#L3251-L3311](https://github.com/golang/go/blob/go1.27.3/src/net/http/server.go#L3251-L3311)). Shutdown does not cancel request contexts. Hijacked connections (WebSockets) are not waited for at all (doc comment, same lines).
- `Server.Close()` closes every active connection's socket ([S#L3222-L3241](https://github.com/golang/go/blob/go1.27.3/src/net/http/server.go#L3222-L3241)). That cancels contexts through the paths above, but handlers that ignore the context still run; the process exiting is the only hard stop.
- HTTP/2 graceful GOAWAY: the shutdown timer is not started until all open streams finish ([H2#L895-L901](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L895-L901)), so a stuck handler keeps the connection open until `Close` or exit.

### 5. HTTP/2 DoS defences
- `MaxConcurrentStreams`: default 250 (`defaultMaxStreams`, [H2#L61](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L61); applied in [config.go#L51](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/config.go#L51); public field doc says "defaults to at least 100", [http.go#L250-L255](https://github.com/golang/go/blob/go1.27.3/src/net/http/http.go#L250-L255)). A HEADERS over the limit gets a stream PROTOCOL_ERROR ([H2#L1912-L1927](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L1912-L1927)).
- **Rapid Reset (CVE-2023-39325, Go's ID for the CVE-2023-44487 class)**: fixed in go1.20.10 / go1.21.3 / x/net v0.17.0 ([GO-2023-2102](https://pkg.go.dev/vuln/GO-2023-2102), [go.dev/issue/63417](https://go.dev/issue/63417)). The fix bounds *running handler goroutines* per connection to `MaxConcurrentStreams`, queues the rest, and closes the connection with ENHANCE_YOUR_CALM ("too_many_early_resets") when the queue exceeds `4 × MaxConcurrentStreams` ([H2#L2250-L2269](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L2250-L2269)). Queued handlers whose stream was reset before start are dropped without running ([H2#L2278-L2285](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L2278-L2285)). **Per connection.**
- **CONTINUATION flood (CVE-2023-45288)**: fixed in go1.21.9 / go1.22.2 / x/net v0.23.0 ([GO-2024-2687](https://pkg.go.dev/vuln/GO-2024-2687), [go.dev/issue/65051](https://go.dev/issue/65051)). The framer closes the connection when a header fragment is more than twice the remaining header-list budget, or any CONTINUATION arrives after the budget is spent ([frame.go#L1795-L1815](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/frame.go#L1795-L1815)). Budget is `MaxHeaderBytes`, default 1 MiB. **Per connection.**
- **MadeYouReset (CVE-2025-8671)**: Go says it is **not affected**, because the Rapid Reset fix counts running handlers plus queued requests, not resets, so it does not matter who reset the stream ([CERT VU#767506, Go statement](https://kb.cert.org/vuls/id/767506)). This matches the code: `curHandlers` drops only when the handler returns.
- Control-frame flood: more than 10000 queued control frames (SETTINGS ack, PING ack, RST_STREAM) closes the connection (`maxQueuedControlFrames`, [H2#L63-L66](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L63-L66), [H2#L887-L892](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/server.go#L887-L892)). Per connection.
- Lost PING: `PingTimeout` default 15 s closes the connection ([config.go#L65](https://github.com/golang/go/blob/go1.27.3/src/net/http/internal/http2/config.go#L65)).

## hyper 1.11 / h2 0.4.18 / hyper-util 0.1.20 (Rust)

Source read locally from the cargo registry. Links pinned to tags: `h2 = https://github.com/hyperium/h2/blob/v0.4.18`, `hyper = https://github.com/hyperium/hyper/blob/v1.11.0`, `hyper-util = https://github.com/hyperium/hyper-util/blob/v0.1.20`.

### 1. Per-client limit
- **None in hyper or hyper-util.** hyper is a connection library; the application owns accept and spawn. Per-client limits come from tower middleware or third-party crates. `tower::limit::ConcurrencyLimit` is a concurrency cap per service, with an optional shared semaphore ([docs.rs](https://docs.rs/tower/latest/tower/limit/concurrency/struct.ConcurrencyLimit.html)). UNVERIFIED (memory): when full it applies backpressure through `poll_ready` rather than rejecting; `tower::load_shed` turns that into an immediate error.

### 2. Global overload
- **None.** No memory monitoring or shedding in hyper.

### 3. Per-route concurrency
- **None built in.** Route-level `ConcurrencyLimit` layers in tower/axum (same docs link as above).

### 4. Handler that never finishes after its client left
- **Rust cancels by dropping the future**, which takes effect at the handler's next `.await` point. A handler that blocks the thread (no await) is not cancelled. This is the closest analogue to a cooperative runtime.
- HTTP/2: while the service future is pending, hyper also polls `poll_reset`; on a client RST_STREAM it returns an error and the stream task ends, dropping the service future ([hyper src/proto/h2/server.rs#L445-L471](https://github.com/hyperium/hyper/blob/v1.11.0/src/proto/h2/server.rs#L445-L471)). Sean McArthur: "Stream concurrency still holds because canceled futures stop immediately" ([MadeYouReset post](https://seanmonstar.com/blog/hyper-http2-didnt-madeyoureset/)).
- HTTP/1: UNVERIFIED. I did not confirm whether hyper's h1 dispatcher notices a client close while the service future is pending. Check `src/proto/h1/dispatch.rs` before relying on it.
- **Graceful shutdown:** `hyper_util::server::graceful::GracefulShutdown::shutdown()` signals every watched connection and then **waits for all of them, with no timeout** ([hyper-util graceful.rs#L59-L70](https://github.com/hyperium/hyper-util/blob/v0.1.20/src/server/graceful.rs#L59-L70)). The documented example races it against a timer; when the timer wins, the application drops the futures (UNVERIFIED (memory) for the example contents: [examples/server_graceful.rs](https://github.com/hyperium/hyper-util/blob/master/examples/server_graceful.rs)). Connection-level `graceful_shutdown()` sends GOAWAY on h2 ([hyper src/proto/h2/server.rs#L185-L195](https://github.com/hyperium/hyper/blob/v1.11.0/src/proto/h2/server.rs#L185-L195)).

### 5. HTTP/2 DoS defences (h2 crate)
- hyper's server default `max_concurrent_streams = 200`, stream and connection windows 1 MiB, `max_header_list_size` 16 KiB, send buffer 400 KiB per stream ([hyper src/proto/h2/server.rs#L36-L77](https://github.com/hyperium/hyper/blob/v1.11.0/src/proto/h2/server.rs#L36-L77)).
- **Rapid Reset**: hyper says it was **unaffected**, because reset streams are cancelled before they reach the handler, and since h2 0.3.18 (April 2023) a limit on remotely reset streams waiting in the accept queue applies ([seanmonstar.com, Oct 2023](https://seanmonstar.com/blog/hyper-http2-rapid-reset-unaffected/)). The setting is `max_pending_accept_reset_streams`, h2 default 20, over which the connection gets GOAWAY ENHANCE_YOUR_CALM ([h2 src/server.rs#L944-L973](https://github.com/hyperium/h2/blob/v0.4.18/src/server.rs#L944-L973), [h2 src/proto/mod.rs#L34](https://github.com/hyperium/h2/blob/v0.4.18/src/proto/mod.rs#L34)). Note hyper passes `None` by default, so h2's default of 20 applies ([hyper src/proto/h2/server.rs#L70](https://github.com/hyperium/hyper/blob/v1.11.0/src/proto/h2/server.rs#L70), [#L146-L148](https://github.com/hyperium/hyper/blob/v1.11.0/src/proto/h2/server.rs#L146-L148)). **Per connection.**
- **"Local error reset" limit** (h2 0.4.2, January 2024: "Limit error resets for misbehaving connections", [CHANGELOG](https://github.com/hyperium/h2/blob/v0.4.18/CHANGELOG.md)): `max_local_error_reset_streams`, default 1024 per connection; past it the server sends GOAWAY ENHANCE_YOUR_CALM ([h2 src/server.rs#L915-L930](https://github.com/hyperium/h2/blob/v0.4.18/src/server.rs#L915-L930), [mod.rs#L35](https://github.com/hyperium/h2/blob/v0.4.18/src/proto/mod.rs#L35)). This is the server counting resets *it* was forced to send, which is the MadeYouReset defence in advance.
- **CONTINUATION flood**: h2 0.4.4 (April 2024) "Limit number of CONTINUATION frames for misbehaving connections" ([CHANGELOG](https://github.com/hyperium/h2/blob/v0.4.18/CHANGELOG.md)); the cap is derived from max header list size and max frame size ([h2 src/codec/framed_read.rs#L56-L112](https://github.com/hyperium/h2/blob/v0.4.18/src/codec/framed_read.rs#L56-L112)). Per connection.
- **MadeYouReset**: "negligibly affected"; one WINDOW_UPDATE code path skipped the local-error-reset count; fixed by a refactor in h2 0.4.11 / 0.3.27 (June 30, 2025); no CVE issued ([seanmonstar.com, Aug 2025](https://seanmonstar.com/blog/hyper-http2-didnt-madeyoureset/)). CERT lists "hyperium" as Not Affected ([VU#767506](https://kb.cert.org/vuls/id/767506)).
- Other per-connection caps in h2: `DEFAULT_RESET_STREAM_MAX = 50` locally-reset streams remembered for `DEFAULT_RESET_STREAM_SECS = 1` ([mod.rs#L38-L45](https://github.com/hyperium/h2/blob/v0.4.18/src/proto/mod.rs#L38-L45)); a DATA-frame budget that sends GOAWAY "too_many_data_frames" for floods of tiny DATA frames ([streams.rs#L649](https://github.com/hyperium/h2/blob/v0.4.18/src/proto/streams/streams.rs#L649)).


# Envoy: resource fairness and DoS limits

Sources and how I read them:

- Source code: tag `v1.39.6` (commit `aacd0340210b536e99ec44ebe59ab9d16b259552`, the newest tag on 2026-11-02). I cloned it and read the files. Links below use `S = https://github.com/envoyproxy/envoy/blob/v1.39.6/`.
- Docs pages: envoyproxy.io pinned to `v1.36.0`, read with WebFetch (a small model summarised each page, so a docs claim is one step weaker than a source claim). Where the source and docs both cover a point, I cite the source.
- Advisories: read through the GitHub API (`gh api repos/envoyproxy/envoy/security-advisories`).
- I tested nothing. Every "behaviour" claim below comes from reading code or docs, not from running Envoy. Claims I worked out from the code but that no comment states directly are marked **INFERENCE**.

Short answer: Envoy has **no built-in per-client (per-IP) concurrency or memory limit**. All of its hard limits are per connection, per listener, per process, or per upstream cluster. Per-IP control exists only as **request-rate** limiting (local_ratelimit with a `remote_address` descriptor), and you must configure it yourself.

---

## 1. Per-client limits

### connection_limit network filter: per filter chain, not per IP

- Config fields: `max_connections`, `delay` (wait before closing a rejected connection, "so a malicious user is not able to retry as fast as possible"), `runtime_enabled`. No field names a client or address. [S/api/envoy/extensions/filters/network/connection_limit/v3/connection_limit.proto#L26-L41](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/connection_limit/v3/connection_limit.proto#L26-L41)
- The counter is one `std::atomic<uint64_t> connections_` on the filter `Config`, so it is shared by every connection through that filter chain, across all workers. [S/source/extensions/filters/network/connection_limit/connection_limit.h#L56](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/extensions/filters/network/connection_limit/connection_limit.h#L56), [connection_limit.cc#L26-L40](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/extensions/filters/network/connection_limit/connection_limit.cc#L26-L40)
- So it is per filter chain. It does **not** key on the remote address. (Confirmed: no address lookup in the filter source; `grep remoteAddress` finds nothing.)

### Other connection limits (none per IP)

- Listener `per_connection_buffer_limit_bytes`: soft limit on each connection's read and write buffers. Default **1 MiB**. [S/api/envoy/config/listener/v3/listener.proto#L244-L247](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/listener/v3/listener.proto#L244-L247)
- Per-listener max connections and the global downstream connection limit (see section 2). Docs: global and per-listener limits are enforced independently; neither is per client. [docs v1.36 overload_manager](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/operations/overload_manager/overload_manager)
- A listener can opt out of the global limit with `ignore_global_conn_limit`. [S/api/envoy/config/listener/v3/listener.proto#L470-L471](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/listener/v3/listener.proto#L470-L471)

### local_ratelimit HTTP filter: can key on client address

- Token bucket scope: by default **one bucket shared by all workers** (per Envoy process). `local_rate_limit_per_downstream_connection: true` gives one bucket per connection. Default false. [S/api/envoy/extensions/filters/http/local_ratelimit/v3/local_rate_limit.proto#L105-L112](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/http/local_ratelimit/v3/local_rate_limit.proto#L105-L112)
- Descriptors: the request's descriptor must match a configured descriptor **verbatim**; there is no partial match. [same file #L81-L95](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/http/local_ratelimit/v3/local_rate_limit.proto#L81-L95)
- Keying on the client: the route `RateLimit.Action.RemoteAddress` action adds the entry `("remote_address", "<trusted address>")`, and `MaskedRemoteAddress` adds `("masked_remote_address", "<cidr>")` with v4 mask default 32 and v6 default 128. [S/api/envoy/config/route/v3/route_components.proto#L2239-L2270](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/route/v3/route_components.proto#L2239-L2270)
- The code reads `info.downstreamAddressProvider().remoteAddress()`, and skips the entry for a non-IP address. [S/source/common/router/router_ratelimit.cc#L131-L162](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/router/router_ratelimit.cc#L131-L162)
- Per-IP buckets without listing each IP: a "wildcard" descriptor (an entry with a key and no value) creates dynamic descriptors in an LRU cache. `max_dynamic_descriptors` default **20**, minimum 1. [local_rate_limit.proto#L185-L192](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/http/local_ratelimit/v3/local_rate_limit.proto#L185-L192)
  - **INFERENCE:** with a default of 20 cached descriptors, a large set of client IPs evicts each other's buckets. An attacker with many source addresses resets its own buckets by churning the LRU. Not tested.
- Rejection: HTTP 429 by default, plus header `x-envoy-ratelimited`. [docs v1.36 local_rate_limit_filter](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/http/http_filters/local_rate_limit_filter)
- This is a **rate** limit (requests per interval), not a limit on concurrent streams or memory per client.

### Client identity: which address is "the client"

- `use_remote_address` (HCM): if true, use the real peer address of the TCP connection. **Default false**: then Envoy uses `x-forwarded-for`. [S/.../http_connection_manager.proto#L751-L759](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#L751-L759)
- `xff_num_trusted_hops`: how many hops from the right of XFF to trust. **Default 0**. [same file #L761-L766](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#L761-L766)
- Code path: with `use_remote_address`, if `xff_num_trusted_hops > 0` take the address from XFF at that hop, else fall back to the connection's peer address; then append to XFF. Without it, run the `original_ip_detection_extensions` in order; the first that finds an address wins, and one may reject the request. The result is stored as the stream's downstream remote address. [S/source/common/http/conn_manager_utility.cc#L158-L220](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_utility.cc#L158-L220), [conn_manager_impl.cc#L1548](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L1548)
- `original_ip_detection_extensions`: cannot be combined with `use_remote_address` or `xff_num_trusted_hops`. If all fail without rejecting, Envoy uses the directly connected address. Built-in extensions: `xff` and `custom_header`. [S/.../http_connection_manager.proto#L768-L784](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#L768-L784), [S/source/extensions/http/original_ip_detection/](https://github.com/envoyproxy/envoy/tree/v1.39.6/source/extensions/http/original_ip_detection)
- PROXY protocol listener filter: overwrites the socket's remote address with the address in the PROXY header (`setRemoteAddress`), so every later layer, including `use_remote_address`, sees the original client. `allow_requests_without_proxy_protocol` lets non-PROXY connections through. [S/source/extensions/filters/listener/proxy_protocol/proxy_protocol.cc#L360](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/extensions/filters/listener/proxy_protocol/proxy_protocol.cc#L360), [proxy_protocol.proto#L99](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/listener/proxy_protocol/v3/proxy_protocol.proto#L99)
- Edge guidance: "the default settings are tailored for the service mesh use case"; at the edge set `use_remote_address: true`. [docs v1.36 best_practices/edge](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/best_practices/edge)

### New: bandwidth share filter (fairness by tenant, bytes only)

- Added in 1.39.0: "local fair-sharing of request and response bandwidth across tenants using configurable weights and per-filter-chain or per-route limits." [S/changelogs/1.39.0.yaml#L878-L882](https://github.com/envoyproxy/envoy/blob/v1.39.6/changelogs/1.39.0.yaml#L878-L882)
- Docs (latest, 1.41-dev): tenant comes from a configurable `tenant_name_selector` (example: client cert CN via CEL); weight split between tenants, then even split between a tenant's requests; token bucket shared by all workers; over-share traffic is **delayed, not rejected** (it pauses reads, which pushes back through flow control). [docs latest bandwidth_share_filter](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/bandwidth_share_filter)
- This is the closest Envoy has to weighted fairness between clients. It covers bandwidth only, not stream count or CPU.

---

## 2. Overload manager

Off unless configured in bootstrap `overload_manager`. The proto requires at least one resource monitor (`min_items: 1`). [S/api/envoy/config/overload/v3/overload.proto#L225-L242](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/overload/v3/overload.proto#L225-L242). `refresh_interval` has no default stated in the proto; docs examples use 0.25s and 5s. [docs v1.36 overload_manager](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/operations/overload_manager/overload_manager)

### Resource monitors (built-in)

From the extension registry: [S/source/extensions/extensions_build_config.bzl#L302-L306](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/extensions/extensions_build_config.bzl#L302-L306)

- `envoy.resource_monitors.fixed_heap`: heap use against `max_heap_size_bytes`.
- `envoy.resource_monitors.injected_resource`: reads pressure from a file (for tests or external signals).
- `envoy.resource_monitors.global_downstream_max_connections`: active downstream connections across all listeners. It is a "proactive" resource (checked at accept time, not polled). [S/envoy/server/overload/thread_local_overload_state.h#L18-L28](https://github.com/envoyproxy/envoy/blob/v1.39.6/envoy/server/overload/thread_local_overload_state.h#L18-L28)
- `envoy.resource_monitors.cpu_utilization`: host or container CPU.
- `envoy.resource_monitors.cgroup_memory`: cgroup v1/v2 memory use against limit.

Global downstream max connections (docs): set `max_active_downstream_connections`. If unset there is **no limit** and Envoy logs a warning at start. The old runtime key `overload.global_downstream_max_connections` still works but is deprecated. Docs advise a value below half the fd limit. [docs v1.36 overload_manager](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/operations/overload_manager/overload_manager)

### Actions

Names and one-line meanings from the source: [S/envoy/server/overload/overload_manager.h#L18-L55](https://github.com/envoyproxy/envoy/blob/v1.39.6/envoy/server/overload/overload_manager.h#L18-L55). Behaviour text from docs. [docs v1.36 overload_manager](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/operations/overload_manager/overload_manager)

| action | effect |
|---|---|
| `stop_accepting_requests` | new requests get an immediate 503 |
| `disable_http_keepalive` | HTTP/2 and HTTP/3: drain with GOAWAY and a grace period. HTTP/1: drain timer on idle connections |
| `stop_accepting_connections` | stop calling accept on listeners |
| `reject_incoming_connections` | accept, then close, without reading data |
| `shrink_heap` | periodically release free memory to the OS |
| `reduce_timeouts` | scale down chosen timers |
| `reset_high_memory_stream` | reset the streams that hold the most buffer memory |
| `close_idle_http_connections` | close idle downstream HTTP connections (in source; not in the v1.36 docs table) |

`reduce_timeouts` can scale these timers: `HTTP_DOWNSTREAM_CONNECTION_IDLE`, `HTTP_DOWNSTREAM_STREAM_IDLE`, `TRANSPORT_SOCKET_CONNECT`, `HTTP_DOWNSTREAM_CONNECTION_MAX`, `HTTP_DOWNSTREAM_STREAM_FLUSH`, each with a `min_timeout` or `min_scale`. [S/api/envoy/config/overload/v3/overload.proto#L86-L138](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/overload/v3/overload.proto#L86-L138)

### Triggers

- `threshold { value }`: action state is 1 (saturated) above the value, else 0.
- `scaled { scaling_threshold, saturation_threshold }`: 0 below scaling, 1 at or above saturation, linear between: `(pressure - scaling) / (saturation - scaling)`.
- [S/api/envoy/config/overload/v3/overload.proto#L48-L80](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/overload/v3/overload.proto#L48-L80)

### Order

- Envoy has **no built-in order or priority** between actions. Each action has its own triggers and fires on its own. The docs do not describe an order. [docs v1.36 overload_manager](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/operations/overload_manager/overload_manager)
- The order is made by the operator, by giving actions different thresholds on the same monitor. The edge best-practice example: `shrink_heap` at 0.95 heap, `stop_accepting_requests` at 0.98. [docs v1.36 best_practices/edge](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/best_practices/edge)
- One explicit precedence exists for load-shed points: `http2_server_go_away_and_close_on_dispatch` wins over `http2_server_go_away_on_dispatch` when both fire. [docs v1.36 overload_manager](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/operations/overload_manager/overload_manager)

### reset_high_memory_stream: how victims are chosen

- Needs `buffer_factory_config.minimum_account_to_track_power_of_two` (range 10..56). **If omitted, no tracking**, so the action has nothing to reset. [S/api/envoy/config/overload/v3/overload.proto#L204-L221](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/overload/v3/overload.proto#L204-L221), [S/source/common/buffer/watermark_buffer.cc#L241-L245](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/buffer/watermark_buffer.cc#L241-L245)
- Each stream has a buffer memory account. Accounts at or above `2^N` bytes go into one of **8 power-of-two buckets**: bucket 0 is `[2^N, 2^(N+1))`, bucket 7 is `>= 128 * 2^N`. Accounts move between buckets as their balance changes. [S/source/common/buffer/watermark_buffer.h#L135](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/buffer/watermark_buffer.h#L135), [watermark_buffer.cc#L267-L276](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/buffer/watermark_buffer.cc#L267-L276)
- On each invocation with pressure `p` (0..1, from a scaled trigger), it clears `min(floor(p*8)+1, 8)` buckets, **largest bucket first**, and resets at most **50 streams per invocation** (`kMaxNumberOfStreamsToResetPerInvocation`). Within a bucket the order is hash-set order, so it is not "biggest first" inside one bucket. [S/source/common/buffer/watermark_buffer.cc#L20](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/buffer/watermark_buffer.cc#L20), [#L203-L238](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/buffer/watermark_buffer.cc#L203-L238)
- The victim gets a downstream stream reset with reason `OverloadManager` and response flag `OverloadManager`. [S/source/common/http/conn_manager_impl.cc#L2141-L2147](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L2141-L2147)
- Docs: the 50-stream cap is per worker; the feature "currently only works with HTTP2". [docs v1.36 overload_manager](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/operations/overload_manager/overload_manager)
- Note: the header comment in `watermark_buffer.h` still says "Is the account balance >= 1MB?" with a TODO to update it; the real threshold is the configured power of two. [S/source/common/buffer/watermark_buffer.h#L171-L192](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/buffer/watermark_buffer.h#L171-L192)

### Load-shed points

Names and comments from source: [S/envoy/server/overload/load_shed_point.h#L13-L60](https://github.com/envoyproxy/envoy/blob/v1.39.6/envoy/server/overload/load_shed_point.h#L13-L60). Each takes its own triggers in `OverloadManager.loadshed_points`. [overload.proto#L188-L201](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/overload/v3/overload.proto#L188-L201)

- `tcp_listener_accept`: close new TCP connections before the listener filter chain.
- `hcm_ondata_creating_codec`: close the connection before creating the codec, on first data.
- `http1_server_abort_dispatch`: stop HTTP/1 at the codec (docs: local reply if no response started, then close).
- `http2_server_go_away_on_dispatch`: send GOAWAY while dispatching HTTP/2; the connection drains.
- `http2_server_go_away_and_close_on_dispatch`: GOAWAY and close now.
- `http3_server_go_away_on_dispatch`, `http3_server_go_away_and_close_on_dispatch`: same for HTTP/3.
- `http_connection_manager_decode_headers`: reject a new stream with a local reply, after header parse, before filters.
- `http_downstream_filter_check`: local reply before the router creates the upstream request.
- `connection_pool_new_connection`: stop opening new upstream connections; a request with no spare capacity fails.

---

## 3. Per-route concurrency limits

Envoy has **no per-handler concurrency limit**. The concurrency limits are per **upstream cluster**. To limit one route, you route it to its own cluster. (**INFERENCE** from the scopes below; I found no per-route concurrency field.)

### Circuit breakers (per cluster, per priority)

- `max_connections` default 1024, `max_pending_requests` default 1024, `max_requests` (parallel requests) default 1024, `max_retries` default 3, plus `retry_budget` and `max_connection_pools`. Per-host thresholds support only `max_connections`. [S/api/envoy/config/cluster/v3/circuit_breaker.proto#L77-L140](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/cluster/v3/circuit_breaker.proto#L77-L140)
- Docs: limits are per upstream cluster and per priority, shared across workers, eventually consistent (can be briefly exceeded). On overflow the router sets `x-envoy-overloaded` and bumps an overflow stat. On by default. [docs v1.36 circuit_breaking](https://www.envoyproxy.io/docs/envoy/v1.36.0/intro/arch_overview/upstream/circuit_breaking)
- UNVERIFIED (memory): the overflowed request gets a 503 local reply.

### Admission control filter (success-rate based)

- Rejects with probability `max(0, (rq_count - rq_success_count / sr_threshold) / (rq_count + 1)) ^ (1 / aggression)`. Defaults: `sampling_window` 30s, `aggression` 1.0, `sr_threshold` 95%, `rps_threshold` 0, `max_rejection_probability` 80%. [S/api/envoy/extensions/filters/http/admission_control/v3/admission_control.proto#L72-L102](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/http/admission_control/v3/admission_control.proto#L72-L102)
- Docs: success rate is tracked per worker thread, so rejection can differ by thread. [docs v1.36 admission_control_filter](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/http/http_filters/admission_control_filter)
- It looks at upstream outcomes; it does not separate one client from another.

### Adaptive concurrency filter (latency based)

- Gradient controller: `max_concurrency_limit` default 1000, minRTT sampling `request_count` default 50, `jitter` 15%, `min_concurrency` 3 (limit used while measuring minRTT), `buffer` 25%, `sample_aggregate_percentile` p50; rejected requests get 503 by default. [S/api/envoy/extensions/filters/http/adaptive_concurrency/v3/adaptive_concurrency.proto#L34-L117](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/http/adaptive_concurrency/v3/adaptive_concurrency.proto#L34-L117)
- Docs: it limits outstanding requests "to all hosts in a given cluster". [docs v1.36 adaptive_concurrency_filter](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/http/http_filters/adaptive_concurrency_filter)

---

## 4. A stream whose client left; drain and shutdown

### Downstream reset cancels the upstream request

- Any downstream stream reset goes through `ActiveStream::onResetStream`. Its comment lists the cases: Envoy sends an app-level reset, the codec sends a reset, the codec receives a reset, or the overload manager resets it. It then calls `filter_manager_.onDownstreamReset()` and destroys the stream. [S/source/common/http/conn_manager_impl.cc#L2109-L2149](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L2109-L2149)
- When the whole downstream connection closes (remote or local), `resetAllStreams` calls `onResetStream(ConnectionTermination)` for every stream. [conn_manager_impl.cc#L598-L636](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L598-L636)
- The router filter's `onDestroy` cancels host selection and calls `resetAll()` on every in-flight upstream request, and cancels shadow streams. [S/source/common/router/router.cc#L1333-L1349](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/router/router.cc#L1333-L1349)
- So: client goes away, the upstream request is reset. Envoy does not let it run to completion. For our server the lesson is that the "upstream" is our handler, and the matching move is to ask the handler task to cancel.

### Timeouts (downstream, HCM and protocol options)

| setting | default | source |
|---|---|---|
| connection `idle_timeout` (no active streams) | 1 hour; 0 disables | [protocol.proto#L312-L332](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L312-L332) |
| `max_connection_duration` | not set (no max); on expiry, start drain | [protocol.proto#L334-L339](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L334-L339) |
| `max_stream_duration` | not set; on expiry, reset the stream regardless of other timeouts | [protocol.proto#L388-L390](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L388-L390) |
| `max_requests_per_connection` | no limit | [protocol.proto#L405-L409](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L405-L409) |
| `stream_idle_timeout` | 5 minutes; 0 disables; 408 if no response headers yet, else reset; timer reset by each header/data event | [http_connection_manager.proto#L587-L621](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#L587-L621) |
| `request_timeout` (whole request received) | disabled | [#L640-L645](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#L640-L645) |
| `request_headers_timeout` | disabled | [#L647-L653](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#L647-L653) |
| `drain_timeout` (gap between first GOAWAY and final GOAWAY) | 5 s | [#L655-L665](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#L655-L665) |
| `delayed_close_timeout` | 1000 ms | [#L679-L712](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#L679-L712) |

- `drain_timeout`: Envoy sends a GOAWAY with max stream id, keeps accepting streams for the grace period, then sends the final GOAWAY. Draining starts on idle timeout, `max_connection_duration`, or server drain. [http_connection_manager.proto#L655-L665](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/extensions/filters/network/http_connection_manager/v3/http_connection_manager.proto#L655-L665)
- Edge guide example values: idle 3600s, `stream_idle_timeout` 300s, `request_timeout` 300s, route `idle_timeout` 15s. [docs v1.36 best_practices/edge](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/best_practices/edge)

### Server drain (CLI)

- `--drain-time-s` default **600**; `--parent-shutdown-time-s` default **900**; `--drain-strategy` `gradual` (default) or `immediate`. [S/source/server/options_impl.cc#L149-L158](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/server/options_impl.cc#L149-L158)
- Gradual: each `drainClose()` check returns true with probability `elapsed / drain_time` (random), and always true after the deadline. Immediate: always true. If the server is health-check failed, always true. [S/source/server/drain_manager_impl.cc#L44-L99](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/server/drain_manager_impl.cc#L44-L99)
- Drain is cooperative: it asks connections to close (GOAWAY or `Connection: close`). Hard stop comes from the parent shutdown time in hot restart. **INFERENCE** about the hard stop; I did not read the hot-restart code.

---

## 5. HTTP/2 DoS defences

All of these are **per connection**. Envoy has no per-client aggregation of any of them. (The premature-reset counters are fields on `ConnectionManagerImpl`, one instance per downstream connection: [conn_manager_impl.h#L692-L718](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.h#L692-L718).) An attacker with many connections gets a fresh budget on each one; the only cross-connection backstops are the connection limits and the overload manager.

### Rapid Reset, CVE-2023-44487

Advisory GHSA-jhv4-f7mr-xx76, 2023-10-10. Fix in 1.27.1 / 1.26.5 / 1.25.10 / 1.24.11 (the advisory's two version lists disagree; the changelogs carry the fix in 1.27.1). [advisory](https://github.com/envoyproxy/envoy/security/advisories/GHSA-jhv4-f7mr-xx76), [S/changelogs/1.27.1.yaml#L4-L19](https://github.com/envoyproxy/envoy/blob/v1.39.6/changelogs/1.27.1.yaml#L4-L19)

Mechanism 1, premature reset counting (in the HCM, so it covers HTTP/2 and HTTP/3):

- Runtime keys: `overload.premature_reset_min_stream_lifetime_seconds` (default **1**), `overload.premature_reset_total_stream_count` (default **500**). [S/source/common/http/conn_manager_impl.cc#L62-L65](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L62-L65), defaults at [#L717](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L717) and [#L754](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L754)
- A stream is "premature" if it lived no longer than the minimum lifetime (whole seconds) **and** had no response status code when destroyed. [#L706-L727](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L706-L727)
- Every closed stream (except internal-redirect recreation) counts toward the total; premature ones also count as bad. [#L330-L336](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L330-L336)
- Rule: below the total, close if `premature * 2 >= limit` (already certain to cross 50%); at or above the total, close if `premature * 2 >= closed`. Action: stat `downstream_rq_too_many_premature_resets`, then **abort** the connection (`ConnectionCloseType::Abort`, "too_many_premature_resets"). [#L729-L779](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L729-L779)
- The old kill switch `envoy.restart_features.send_goaway_for_premature_rst_streams` was removed in 1.31.0. [S/changelogs/1.31.0.yaml#L295-L297](https://github.com/envoyproxy/envoy/blob/v1.39.6/changelogs/1.31.0.yaml#L295-L297)
- A recursion / stack-overflow bug in this drain path was fixed in 1.34.5, 1.35.1 and 1.36.0. [S/changelogs/1.35.1.yaml#L9-L13](https://github.com/envoyproxy/envoy/blob/v1.39.6/changelogs/1.35.1.yaml#L9-L13)

Mechanism 2, per-I/O-cycle request cap:

- Runtime key `http.max_requests_per_io_cycle`. Default `UINT32_MAX`, so **off by default**. [conn_manager_impl.cc#L66-L69](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L66-L69), [#L148-L149](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L148-L149)
- Over the cap, new streams on that connection are deferred to the next event-loop iteration (a schedulable callback), in order; one stream always goes through per deferred pass. [#L2640-L2683](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L2640-L2683)
- Changelog: "mitigates CPU starvation by connections that simultaneously send high number of requests by allowing requests from other connections to make progress"; advise 1 under attack. [S/changelogs/1.27.1.yaml#L13-L19](https://github.com/envoyproxy/envoy/blob/v1.39.6/changelogs/1.27.1.yaml#L13-L19)
- This is the one Envoy mechanism that is explicitly about fairness **between connections** on a worker.

Mechanism 3, nghttp2 RST_STREAM token bucket (made configurable in 1.39.0):

- `stream_reset_burst` default **1000**, `stream_reset_rate` default **33/s**. When empty, nghttp2 sends GOAWAY `INTERNAL_ERROR` and closes. nghttp2 server only; no effect on oghttp2. [S/api/envoy/config/core/v3/protocol.proto#L811-L829](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L811-L829), [S/changelogs/1.39.0.yaml#L868-L877](https://github.com/envoyproxy/envoy/blob/v1.39.6/changelogs/1.39.0.yaml#L868-L877)

Advisory also suggests lowering `max_outbound_frames` to about 1000 and `max_outbound_control_frames` to about 500 against the early-error-response variant. [advisory](https://github.com/envoyproxy/envoy/security/advisories/GHSA-jhv4-f7mr-xx76)

### CONTINUATION flood, 2024

- CVE-2024-27919 (GHSA-gghf-vfxp-799r): memory exhaustion, because the codec "does not reset a request when header map limits have been exceeded". A regression in 1.29.0/1.29.1 only; fixed in 1.29.2. [advisory](https://github.com/envoyproxy/envoy/security/advisories/GHSA-gghf-vfxp-799r)
- CVE-2024-30255 (GHSA-j654-3ccm-vfmm): CPU exhaustion, the codec accepted unlimited CONTINUATION frames after the header map limit, about 1 core per 300 Mbit/s. Fixed in 1.29.3 / 1.28.2 / 1.27.4 / 1.26.8. [advisory](https://github.com/envoyproxy/envoy/security/advisories/GHSA-j654-3ccm-vfmm)
- Header limits involved: `max_headers_count` default 100; header size default 60 KiB (`max_request_headers_kb` on HCM, `max_response_headers_kb`); nghttp2 per-field wire limit `max_header_field_size_kb` default 64 KB. [protocol.proto#L357-L385](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L357-L385), [#L768-L801](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L768-L801)
- Related, 2026: GHSA-22m2-hvr2-xqc8 (CVE-2026-47774): cookie fragments were merged after the header-size check, and oghttp2 HPACK limits counted encoded bytes not decoded bytes, so a client could hold large decoded headers per stream and OOM the process. Lesson for us: **count decoded header bytes, and count every header including cookies, before accepting.** [advisory](https://github.com/envoyproxy/envoy/security/advisories/GHSA-22m2-hvr2-xqc8)

### MadeYouReset, CVE-2025-8671

- **No Envoy advisory found.** The full published advisory list for envoyproxy/envoy (GitHub API, read 2026-11-02) has no entry for CVE-2025-8671, and no changelog in v1.39.6 mentions it or "MadeYouReset". [advisory list](https://github.com/envoyproxy/envoy/security/advisories)
- **INFERENCE (from code, not tested):** Envoy's premature-reset counter likely covers this attack anyway. `onResetStream` runs for resets the codec *sends* as well as ones it receives ([conn_manager_impl.cc#L2111-L2116](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L2111-L2116)), and the count in `doDeferredStreamDestroy` does not look at who sent the reset, only at lifetime and whether a response code exists ([#L330-L336](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L330-L336), [#L706-L727](https://github.com/envoyproxy/envoy/blob/v1.39.6/source/common/http/conn_manager_impl.cc#L706-L727)). So server-sent resets caused by a client's bad frames would also count. The nghttp2 token bucket counts only *received* RST_STREAM, so it would not catch this. Check by running the MadeYouReset PoC against Envoy before relying on this.

### http2_protocol_options defaults

All from [S/api/envoy/config/core/v3/protocol.proto](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto), message starts [#L572](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L572).

| option | default | what happens on breach | line |
|---|---|---|---|
| `hpack_table_size` | 4096 | | [#L592-L596](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L592-L596) |
| `max_concurrent_streams` | **1024** ("for safety") | protocol limit | [#L598-L610](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L598-L610) |
| `initial_stream_window_size` | **16 MiB** (min 65535); also the soft per-stream codec buffer limit (watermark) | | [#L612-L626](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L612-L626) |
| `initial_connection_window_size` | **24 MiB** | | [#L628-L631](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L628-L631) |
| `max_outbound_frames` | 10000 pending outbound frames | close connection, stat `http2.outbound_flood` | [#L644-L648](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L644-L648) |
| `max_outbound_control_frames` | 1000 pending PING/SETTINGS/RST_STREAM | close, `http2.outbound_control_flood` | [#L650-L655](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L650-L655) |
| `max_consecutive_inbound_frames_with_empty_payload` | 1 (empty HEADERS/CONTINUATION/DATA without END_STREAM) | close, `http2.inbound_empty_frames_flood` | [#L657-L663](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L657-L663) |
| `max_inbound_priority_frames_per_stream` | 100; budget = `100 * (1 + opened_streams)` over connection lifetime | close, `http2.inbound_priority_frames_flood` | [#L665-L676](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L665-L676) |
| `max_inbound_window_update_frames_per_data_frame_sent` | 10; budget = `5 + 2*(opened_streams + 10*outbound_data_frames)` | close, `http2.inbound_window_update_frames_flood` | [#L678-L693](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L678-L693) |
| `max_metadata_size` | 1 MB per stream | | [#L760-L761](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L760-L761) |
| `stream_reset_burst` / `stream_reset_rate` | 1000 / 33 per s (nghttp2 server) | GOAWAY + close | [#L811-L829](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L811-L829) |

- For "opened_streams", downstream counts a stream as opened only when Envoy has received complete response headers from upstream. [#L669-L674](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L670-L673)
- Invalid HEADERS: by default the **whole connection** is terminated; `override_stream_error_on_invalid_http_message` makes it reset only that stream. [#L695-L718](https://github.com/envoyproxy/envoy/blob/v1.39.6/api/envoy/config/core/v3/protocol.proto#L695-L718)
- `per_connection_buffer_limit_bytes`: listener default 1 MiB (see section 1). Edge guide recommends 32 KiB, `max_concurrent_streams` 100, stream window 64 KiB, connection window 1 MiB, and says defaults suit a service mesh, not an edge. [docs v1.36 best_practices/edge](https://www.envoyproxy.io/docs/envoy/v1.36.0/configuration/best_practices/edge)
  - Note the gap: default `max_concurrent_streams` 1024 x 16 MiB stream window is far beyond the 24 MiB connection window, so the connection window is the real cap on unread request body per connection. **INFERENCE** from the numbers.

---

## What carries over to our server (my reading, not Envoy's claims)

1. Envoy's "no single client starves the server" is built from **per-connection** budgets plus **process-wide** pressure actions. It never aggregates a client across connections except via rate-limit descriptors or the 1.39 bandwidth-share filter.
2. The pressure response is "reset the biggest memory holders first, 50 at a time, more buckets as pressure rises". That is a cheap, coarse, log-bucketed victim pick; it needs per-stream memory accounts.
3. `max_requests_per_io_cycle` is the CPU-fairness primitive: cap new streams per connection per loop turn, defer the rest. Off by default.
4. Premature-reset counting judges by **outcome** (short life and no response), not by who sent the reset.
5. Client gone means the upstream work is reset at once. In our cooperative runtime the matching step is to request cancel on the handler task and stop charging its buffers to anyone.


# Resource fairness in nginx, HAProxy and h2o

Input to the t-3737 fairness report. Research only, done 2026-11-02.

## Pinned sources

Source links are pinned so the line anchors stay valid:

- nginx: tag `release-1.31.8` → `https://github.com/nginx/nginx/blob/release-1.31.8/...`
- HAProxy: tag `v3.3.0` → `https://github.com/haproxy/haproxy/blob/v3.3.0/...` (the docs are `doc/configuration.txt` at the same tag)
- h2o: master at commit `7bbb3e39bf03027344f36b4650e5383c3b6b1014` (2026-11-02) → `https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/...`. h2o has had no release tag since 2019 (`v2.3.0-beta2`), so master is the only honest pin.

Labels:
- "UNVERIFIED (memory)": I did not check it against a source.
- "Inference": my own reasoning from cited code. It is not a claim from the vendor.

---

## 1. Per-client limits

### nginx

**Built in, but off by default.** Both modules are compiled in by default. Neither does anything until you declare a zone and a limit.

- `limit_conn_zone` + `limit_conn` set the number of concurrent "connections" per key. Default `limit_conn_status 503`. The directives can go in `http`, `server` or `location` ([docs](https://nginx.org/en/docs/http/ngx_http_limit_conn_module.html)).
- `limit_req_zone` + `limit_req` limit the request rate per key with a "leaky bucket". The default burst is 0. `nodelay` and `delay=` control queueing. Default `limit_req_status 503` ([docs](https://nginx.org/en/docs/http/ngx_http_limit_req_module.html)).
- **Identity.** The doc examples key on `$binary_remote_addr`, which is 4 bytes for IPv4 and 16 bytes for IPv6 ([limit_conn docs](https://nginx.org/en/docs/http/ngx_http_limit_conn_module.html)). The key can be any complex value, meaning variables and text. The handler evaluates it with `ngx_http_complex_value` ([source L204](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/modules/ngx_http_limit_conn_module.c#L204), [limit_req L227](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/modules/ngx_http_limit_req_module.c#L227)). Both handlers run in the PREACCESS phase ([limit_conn L750](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/modules/ngx_http_limit_conn_module.c#L750), [limit_req L1094](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/modules/ngx_http_limit_req_module.c#L1094)).
- **HTTP/2 streams count separately: confirmed.**
  - The docs say: "In HTTP/2 and HTTP/3, each concurrent request is considered a separate connection" ([docs](https://nginx.org/en/docs/http/ngx_http_limit_conn_module.html)).
  - The source agrees. The state is per request (`r->main->limit_conn_status`), and it is released by a cleanup on the request pool ([L194](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/modules/ngx_http_limit_conn_module.c#L194), [L220](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/modules/ngx_http_limit_conn_module.c#L220), [L300](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/modules/ngx_http_limit_conn_module.c#L300)).
  - So `limit_conn` is really a **concurrent-requests** limit. It is not a TCP-connection limit.
  - A connection is counted "only if it has a request being processed by the server and the whole request header has already been read" ([docs](https://nginx.org/en/docs/http/ngx_http_limit_conn_module.html)). So idle keep-alive connections and slow-header (slowloris) connections are **not** counted.
- **When the zone is full.**
  - limit_conn: "the server will return the error to all further requests" (fails closed) ([docs](https://nginx.org/en/docs/http/ngx_http_limit_conn_module.html)).
  - limit_req: it evicts the least recently used state first, and errors only if eviction does not free space ([docs](https://nginx.org/en/docs/http/ngx_http_limit_req_module.html)).
- **Behind proxies: `ngx_http_realip_module`.** It is "not built by default", so you need `--with-http_realip_module` ([docs](https://nginx.org/en/docs/http/ngx_http_realip_module.html)).
  - `set_real_ip_from` lists the trusted proxy addresses or CIDRs, or `unix:`.
  - `real_ip_header` defaults to `X-Real-IP`. It can also be `X-Forwarded-For` or `proxy_protocol`. `proxy_protocol` needs the `proxy_protocol` parameter on `listen`.
  - `real_ip_recursive` defaults to `off`, which takes the *last* address in the header. With `on`, it takes the "last non-trusted address".
  - The original peer address stays available in `$realip_remote_addr` ([docs](https://nginx.org/en/docs/http/ngx_http_realip_module.html)).
  - The module registers handlers in POST_READ and PREACCESS ([source L526-L533](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/modules/ngx_http_realip_module.c#L526-L533)). So limit_conn and limit_req see the rewritten address, as long as the realip directives apply to that request. (Inference about ordering within the phase.)
- **IPv6 /64 grouping: not built in.** No nginx variable or directive masks an address to a prefix. I found nothing in the limit or realip docs above. A key built with `map` and a regex over `$remote_addr` would be needed. That is **UNVERIFIED (memory)** as a working recipe. The only hard fact is that the key accepts any complex value (source above).

### HAProxy

**Built in, as a general mechanism. Nothing is enabled by default.** You declare a `stick-table` and track sources into it.

- **Tracking.**
  - `tcp-request connection track-sc0 src`, or the `session`, `content` and `http-request` variants. The `<key>` "is a sample expression" ([configuration.txt L16336-L16358](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L16336-L16358)).
  - Doc example: track, then reject with `{ sc0_conn_rate gt 10 }` ([L13680-L13698](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L13680-L13698)).
- **Counters a table can store** ([L29933-L30085](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L29933)):
  - `conn_cur`: concurrent connections, exact. Incremented when a connection matches and decremented when it leaves.
  - `conn_rate(period)`.
  - `http_req_rate(period)`: counts requests, valid or not. "different from sessions when keep-alive is used".
  - `glitch_cnt` and `glitch_rate(period)`: front-connection protocol anomalies ([L29954-L29975](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L29954)).
  - Matching fetches: `sc_conn_cur`, `sc_glitch_cnt`, `sc_glitch_rate`, `sc_http_req_rate`, `src_glitch_rate` ([L23281-L23326](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L23281)).
- **IPv6 masking.**
  - The `ipmask(<mask4>[,<mask6>])` converter lets hosts in a prefix share one table entry.
  - CIDR form works, for example `64`.
  - "If no mask6 is given IPv6 addresses will fail to convert". So `src,ipmask(24)` alone silently fails on IPv6, and you must write `ipmask(32,64)` or similar ([L20914-L20921](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L20914)).
  - The doc also shows `http-request set-src src,ipmask(24)` followed by `track-sc0 src` ([L16115-L16127](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L16115)).
- **Behind proxies.**
  - `accept-proxy` on `bind` reads PROXY v1/v2. Every address use then sees the PROXY address, "with the only exception of 'tcp-request connection' rules which will only see the real connection address" ([L16496-L16508](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L16496)). So per-client tracking behind an L4 proxy must use `tcp-request session` or later. The doc example does exactly this ([L13983-L13990](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L13983)).
  - `http-request set-src hdr(x-forwarded-for)` rewrites the source address from a header ([L16105-L16127](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L16105)).
  - `option forwardfor` is the outbound side. It *adds* XFF towards servers. It is not a trust mechanism ([L9810](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L9810)).
- **maxconn levels.**
  - Global `maxconn` is per process. If unset, it is computed from fd limits and memory ([L3804-L3826](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L3804)).
  - Frontend `maxconn` defaults to 0, which means use the global value ([L9082-L9110](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L9082)).
  - Server `maxconn` is covered in section 3.

### h2o

**No per-client limit in the core.** None of the base, HTTP/1 or HTTP/2 directives is keyed on client identity ([http2_directives.mt](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/http2_directives.mt), [base_directives.mt](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/base_directives.mt)).

The only per-client option is an **opt-in mruby script**, `dos_detector.rb` ([doc](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/dos_detection.mt)):
- It counts requests per IP over a window. The defaults are period 10 s, threshold 100 and ban 300 s. When over the threshold it returns 403.
- It is a request-rate limiter only. There is no concurrency limit.
- With `:forwarded` set, it takes the **first** X-Forwarded-For entry ([dos_detector.rb L67-L69](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/share/h2o/mruby/dos_detector.rb#L67-L69)). The client can spoof that value. (Inference from the code. This is not a vendor statement.)
- Its state is an in-process `LRUCache` ([L36](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/share/h2o/mruby/dos_detector.rb#L36)). Whether that cache is shared across h2o worker threads: **UNVERIFIED**. My guess is that it is per thread, because mruby contexts are per thread (memory).

---

## 2. Global overload

### nginx

- `worker_connections` defaults to 512 per worker. It counts *all* connections, upstream ones included, and is capped by `RLIMIT_NOFILE` ([core docs](https://nginx.org/en/docs/ngx_core_module.html)).
- **What happens when it runs out** (source):
  1. Throttle. After each accept, `ngx_accept_disabled = connection_n/8 - free_connection_n`. While this is positive, the worker skips taking the accept mutex. This sheds new connections to other workers once free slots drop below 1/8 ([ngx_event_accept.c L139](https://github.com/nginx/nginx/blob/release-1.31.8/src/event/ngx_event_accept.c#L139), [ngx_event.c L220](https://github.com/nginx/nginx/blob/release-1.31.8/src/event/ngx_event.c#L220)). This only matters when `accept_mutex` is in use, and its default is `off` ([core docs](https://nginx.org/en/docs/ngx_core_module.html)).
  2. Reclaim idle connections. `ngx_get_connection` first calls `ngx_drain_connections`. When free slots are below 1/16, this closes "reusable" connections, meaning idle keep-alive ones. It logs "worker_connections are not enough, reusing connections" ([ngx_connection.c L1223](https://github.com/nginx/nginx/blob/release-1.31.8/src/core/ngx_connection.c#L1223), [L1405-L1425](https://github.com/nginx/nginx/blob/release-1.31.8/src/core/ngx_connection.c#L1405)).
  3. Refuse. If no slot is left, it logs an ALERT, "worker_connections are not enough", and closes the new socket ([L1226-L1232](https://github.com/nginx/nginx/blob/release-1.31.8/src/core/ngx_connection.c#L1226)).
- No request queue exists. Overload means connections are refused or idle ones are evicted. (Inference from the above.)

### HAProxy

- **Global or frontend `maxconn` reached.** "Proxies will stop accepting connections" ([L3812](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L3812)). For a frontend: "Excess connections will be queued by the system in the socket's listen queue and will be served once a connection closes" ([L9091-L9093](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L9091)). So the backlog is the kernel's.
- **Rate limits.** `maxconnrate` and `maxsessrate` are per process. The docs warn there is "not necessarily ... a fair share between frontends", and that lowering `tune.maxaccept` improves fairness ([L3828-L3836](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L3828)).
- **Memory cost.** About 33 kB per established connection, from two 16 kB buffers ([L9097-L9102](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L9097)).
- **Slow-header defence.** `timeout http-request` puts a hard limit on receiving the whole request, then sends 408. The docs explain that `timeout client` is an *inactivity* timeout, so a slowloris client can defeat it ([L14318-L14345](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L14318)).

### h2o

- `max-connections` defaults to 1024. It is global and counts TCP and QUIC together ([base_directives.mt L568-L578](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/base_directives.mt#L568)).
- When the limit is reached, "new connection attempts are ignored until existing connections close" ([L869-L880](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/base_directives.mt#L869)).
  - In the source, `on_accept` stops accepting, so the connection stays in the kernel backlog ([src/main.c L4540-L4546](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/src/main.c#L4540)).
  - It also accepts at most 10 connections per loop pass, to bound the TLS handshake latency to about 10 ms ([L4527-L4528](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/src/main.c#L4527)).
  - The comment admits the limit can be overshot by up to `num_threads` ([L4541-L4544](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/src/main.c#L4541)).
- `soft-connection-limit`: above this count, connections idle for at least `soft-connection-limit.min-age` are closed. The min-age defaults to 30 s ([base_directives.mt L869-L899](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/base_directives.mt#L869); code `close_idle_connections` [src/main.c L4515-L4522](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/src/main.c#L4515)). It plays the same role as nginx's drain of reusable connections.

---

## 3. Per-route concurrency

- **HAProxy has the strongest model.**
  - Server `maxconn` defaults to 0, meaning unlimited. In HTTP mode it "limits the number of concurrent requests instead of the number of connections". The excess "will be queued" ([L18343-L18359](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L18343)).
  - `maxqueue` defaults to 0, meaning an unlimited queue. When it is full, requests are redispatched to other servers ([L18361-L18373](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L18361)).
  - `timeout queue` limits the time a request waits in the queue, then returns 503. If unset it equals `timeout connect` ([L14368-L14390](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L14368)).
  - A route means a backend selected by `use_backend`, so one slow backend fills its own queue only. (Inference.)
- **nginx.** `limit_conn` is allowed in `location`, so a zone keyed on something constant, or on `$server_name`, gives a per-location concurrency cap. That pattern is **UNVERIFIED (memory)**. The context fact is from the [docs](https://nginx.org/en/docs/http/ngx_http_limit_conn_module.html). There is no queue: the excess gets `limit_conn_status` (503) at once. For upstreams, server-level `max_conns` and `queue` exist. **UNVERIFIED (memory)**: I believe `queue` was NGINX Plus only for years. Not checked.
- **h2o has no per-path concurrency limit** in the directive docs I read. It has per-connection limits only:
  - `http2-max-concurrent-requests-per-connection`, default 100, maximum 256.
  - `http2-max-concurrent-streaming-requests-per-connection`, default 1.
  - Both are in [http2_directives.mt L261-L289](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/http2_directives.mt#L261).
  - Requests above the limit wait in `_pending_reqs`. `can_run_requests` checks the half-closed count against that limit ([connection.c L164-L167](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/lib/http2/connection.c#L164)).
  - So h2o separates "streams the protocol allows" (`http2-max-streams`, default 100) from "requests the handlers run".

---

## 4. A request whose client left, and graceful shutdown

### nginx

- `proxy_ignore_client_abort` defaults to `off`. It "Determines whether the connection with a proxied server should be closed when a client closes the connection without waiting for a response" ([proxy docs](https://nginx.org/en/docs/http/ngx_http_proxy_module.html)).
  - With `off`, the upstream module keeps watching the client read event so it can notice the close ([ngx_http_upstream.c L655-L665](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/ngx_http_upstream.c#L655)).
  - With `off`, the upstream connection is closed. The docs do not state this outright; it follows from the description.
- **Graceful shutdown.**
  - `worker_shutdown_timeout` has **no default**, shown as "—" in the docs. With no timeout, graceful shutdown waits for open connections ([core docs](https://nginx.org/en/docs/ngx_core_module.html)).
  - The timer is armed only when the value is non-zero ([ngx_cycle.c L1443-L1449](https://github.com/nginx/nginx/blob/release-1.31.8/src/core/ngx_cycle.c#L1443)). The default is 0 ([nginx.c L1194](https://github.com/nginx/nginx/blob/release-1.31.8/src/core/nginx.c#L1194)).
  - So the answer is yes: by default it waits forever for active connections. Idle keep-alive connections are closed. The idle part is **UNVERIFIED (memory)**.
  - Changelog history: 1.17.4 says it "might not work when using HTTP/2", and 1.27.1 says "new HTTP/2 connections might ignore graceful shutdown" ([CHANGES](https://nginx.org/en/CHANGES)).
  - SIGQUIT as the graceful signal: **UNVERIFIED (memory)**.

### HAProxy

- **`option abortonclose`** ([L9239-L9300](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L9239)):
  - In a backend, a half-closed client connection aborts a request "not yet sent to a server (i.e. when it's pending in the queue or when trying to connect)".
  - If the request is already at the server, the server connection is half-closed, and the server "will then decide how to proceed".
  - It is enabled by default for HTTP backends and TLS listeners. In a frontend, it skips TLS handshakes pending on half-closed connections.
- **`hard-stop-after <time>`.** This is the maximum time to stay alive after soft-stop (SIGUSR1) ([L2724-L2740](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L2724)). The default is `TICK_ETERNITY`, meaning none ([src/haproxy.c L160](https://github.com/haproxy/haproxy/blob/v3.3.0/src/haproxy.c#L160)).
- **`grace <time>`.** This delays the soft-stop after SIGUSR1 while listeners still accept, so health checks can see `stopping` ([L2575-L2590](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L2575)).
- **`close-spread-time`** also exists ([L2108](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L2108)). I did not read it.

### h2o

- **Client abort.** When the request is disposed, the proxy generator calls `do_close`, which calls `client->cancel(client)` on the upstream HTTP client ([lib/core/proxy.c L311-L335, L815-L818](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/lib/core/proxy.c#L311)). So a client that leaves cancels the upstream. I see no "ignore client abort" knob. (Inference from code.)
- **Graceful shutdown.**
  - On SIGTERM it stops the listeners, then calls `h2o_context_request_shutdown`. It then waits until all connections close ([src/main.c L4842-L4870](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/src/main.c#L4842)).
  - `http2-graceful-shutdown-timeout` defaults to 0, which means "H2O will wait for the peer to close the connections" ([http2_directives.mt L393-L400](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/http2_directives.mt#L393)).
  - When it is set, a straggler timer closes the connection ([connection.c L75-L95](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/lib/http2/connection.c#L75)).
  - HTTP/1 only disables keep-alive on shutdown ([lib/http1.c L535-L536](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/lib/http1.c#L535)).
  - A global hard shutdown timeout in h2o: **UNVERIFIED**. I did not find one.

---

## 5. HTTP/2 DoS

### Rapid Reset (CVE-2023-44487)

**nginx: affected and patched, per connection.**
- Commit `6ceef19`, "HTTP/2: per-iteration stream handling limit", dated 2023-10-10 ([commit](https://github.com/nginx/nginx/commit/6ceef192e7af1c507826ac38a2d43f08bf265fb9)). It shipped in 1.25.3 (2023-10-24), listed as "improved detection of misbehaving clients when using HTTP/2" ([CHANGES](https://nginx.org/en/CHANGES)).
- **Mechanism.** At most `2 * max_concurrent_streams` new streams are allowed per event-loop read pass. The limit applies "even if max_concurrent_streams is not yet reached - for example, if corresponding streams are handled synchronously or reset". Refused streams are capped at max(max_concurrent_streams, 100).
  - Code: the counter is reset to 0 on each read handler call ([L354](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2.c#L354)). A new stream over `2*concurrent_streams` gets REFUSED_STREAM ([L1267-L1281](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2.c#L1267)).
- **F5's guidance** ([blog](https://f5.com/company/blog/http-2-rapid-reset-attack-impacting-f5-nginx-products)): keep `keepalive_requests` at 1000 and `http2_max_concurrent_streams` at 128. It also suggests `limit_req`. The risk rises when keep-alive is set far above the default.
- **Older per-connection flood guards** in the source:
  - Traffic ratio: close the connection if `total_bytes/8 > payload_bytes + 1MB` ("traffic-based flood detection", 2019) ([L431-L436](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2.c#L431), [commit af0e284](https://github.com/nginx/nginx/commit/af0e284b967d0ecff1abcdce6558ed4635e3e757)).
  - At most 10000 queued control frames ([L2986-L3016](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2.c#L2986)).
  - Idle frames: more than `10*keepalive_requests` closes the connection ([L4929](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2.c#L4929)).
- nginx's security advisory page does **not** list CVE-2023-44487 ([advisories](https://nginx.org/en/security_advisories.html)).

**HAProxy: not affected, per its vendor blog.**
- "Instead of only counting known, established streams at the protocol level, HAProxy counts allocated resources" ([blog](https://cdn.haproxy.com/blog/haproxy-is-not-affected-by-the-http-2-rapid-reset-attack-cve-2023-44487)).
- "HAProxy will count a stream as present until its resources are completely released", and "new stream creation remains pending" until the count drops below the limit.
- **Source.** `h2_frt_has_too_many_sc()` compares `nb_sc` with the max concurrent streams. `nb_sc` is the number of stream connectors still attached to the upper layer ([mux_h2.c L1061-L1065](https://github.com/haproxy/haproxy/blob/v3.3.0/src/mux_h2.c#L1061)). When it is exceeded, the demuxer sets `H2_CF_DEM_TOOMANY` and stops parsing new HEADERS ([L2094-L2095](https://github.com/haproxy/haproxy/blob/v3.3.0/src/mux_h2.c#L2094), [L3477-L3478](https://github.com/haproxy/haproxy/blob/v3.3.0/src/mux_h2.c#L3477)). It resumes only when the upper layer detaches a stream ([L5546-L5554](https://github.com/haproxy/haproxy/blob/v3.3.0/src/mux_h2.c#L5546)).
- This is backpressure on reading, not a reject. **This is the design most relevant to a cooperative runtime.** A reset stream keeps its slot until its handler actually lets go. (Inference about relevance.)

**h2o: affected and patched.**
- Advisory [GHSA-2m7v-gc89-fjqf](https://github.com/h2o/h2o/security/advisories/GHSA-2m7v-gc89-fjqf), fixed by [PR #3291](https://github.com/h2o/h2o/pull/3291) (merge `28fe151`).
- **Mechanism.** Each connection has a `reset_budget`, starting at `max_concurrent_requests_per_connection`. A peer reset decrements it, and a normal stream close increments it ("penalize connections that generate resets for >50% of requests"). At zero, the connection stops running pending requests for `http2-dos-delay` ms, default 100 ([connection.c L335-L350](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/lib/http2/connection.c#L335), [L205-L206](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/lib/http2/connection.c#L205), [doc L417-L428](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/http2_directives.mt#L417)).
- It delays the connection. It does not kill it.

### CONTINUATION flood (2024, VU#421644)

- **The researcher's page** lists "**NOT** affected: Nginx, Jetty, HAProxy, NetScaler, Varnish, Cloudflare". h2o is not mentioned ([nowotarski.info](https://nowotarski.info/http2-continuation-flood/)). CERT lists NGINX and HAProxy as "Unknown" and does not list h2o ([VU#421644](https://kb.cert.org/vuls/id/421644)).
- **nginx bounds the header block** by `large_client_header_buffers`, as size × number ([ngx_http_v2.c L1261](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2.c#L1261)). It answers with ENHANCE_YOUR_CALM if one field is larger than a buffer, or if the total header is larger than the limit ([L1527-L1533](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2.c#L1527), [L1744-L1750](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2.c#L1744)). The docs say the limits "are similarly applied in HTTP/2" ([core docs](https://nginx.org/en/docs/http/ngx_http_core_module.html)). nginx also has a `max_headers` directive (2024) ([ngx_http_core_module.c L255](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/ngx_http_core_module.c#L255)).
- **HAProxy** has a vendor blog: "HAProxy is Resilient to the HTTP/2 CONTINUATION Flood" ([blog](https://haproxy.com/blog/haproxy-is-resilient-to-the-http-2-continuation-flood)).
  - In 2.8.7 and 2.9.6 (2024-03-13) it added "count excess of CONTINUATION frames as a glitch" and the glitches threshold ([2.9.6 changelog](https://www.haproxy.org/download/2.9/src/snapshot/CHANGELOG-2.9.6-20240313)).
  - **So glitches came in 2.8.7 and 2.9.6, not in 3.0.**
  - CONTINUATION frames larger than `tune.bufsize` are glitches and errors ([mux_h2.c L5997-L6021](https://github.com/haproxy/haproxy/blob/v3.3.0/src/mux_h2.c#L5997)).
- **h2o** caps the continuation buffer at `H2O_MAX_REQLEN` = 8192 + 4096 × `H2O_MAX_HEADERS` ([connection.c L790](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/lib/http2/connection.c#L790), [header.h L45](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/include/h2o/header.h#L45)). I found no h2o CONTINUATION advisory.

### MadeYouReset (CVE-2025-8671)

- **The attack.** The client makes the *server* reset streams by sending invalid frames, such as a bad WINDOW_UPDATE or DATA on a closed stream. "Streams reset by the server are considered closed, even though backend processing continues" ([CERT VU#767506](https://kb.cert.org/vuls/id/767506)).
- **HAProxy: "Not Affected"** (vendor statement, 2025-07-30). It counts and closes streams as soon as an RST is seen in either direction, enforces limits on *allocated* streams, and can kill faulty connections above a threshold ([VU#767506](https://kb.cert.org/vuls/id/767506)).
- **h2o: affected and patched.**
  - Advisory [GHSA-mrjm-qq9m-9mjq](https://github.com/h2o/h2o/security/advisories/GHSA-mrjm-qq9m-9mjq), commit `579ecfa`, "Count server resets as wasted requests".
  - The fix adds `reset_by_peer_action`. It is set on server-sent RSTs caused by peer errors (bad request, DATA on a closed stream, bad WINDOW_UPDATE). Those resets now drain the same `reset_budget` ([commit](https://github.com/h2o/h2o/commit/579ecfaca155d1f9f12bfd0cff6086dcda4b9692); current [connection.c L335-L340](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/lib/http2/connection.c#L335)).
  - Fastly's h2o fork was affected too ([VU#767506](https://kb.cert.org/vuls/id/767506)).
- **nginx: no statement.** CERT lists NGINX and F5 as "Unknown" ([VU#767506](https://kb.cert.org/vuls/id/767506)), and nginx.org has no advisory ([advisories](https://nginx.org/en/security_advisories.html)). F5's advisory K000152001 would not load for me. Inference: nginx's per-iteration limit counts *new* streams regardless of who reset them, so it probably bounds this attack. I have not tested this.

### Other newer items (seen in passing)

- **h2o GHSA-qcrr-wrhc-pgq9** (2026-06), "HTTP/2 state amplification". Fixed in [PR #3597](https://github.com/h2o/h2o/pull/3597): close the connection if a request has more than 1000 fields, and send 400 above 100 fields ([advisory](https://github.com/h2o/h2o/security/advisories/GHSA-qcrr-wrhc-pgq9)).
- **HAProxy's "HTTP/2 Bomb" post** (CVE-2026-49975): HPACK amplification plus stalled streams. HAProxy says it is "architecturally safe". The post says nginx is fixed in 1.29.8 (HAProxy's claim, not checked). It mentions `tune.h2.fe.max-rst-at-once` (HAProxy 3.4+) ([blog](https://cdn.haproxy.com/blog/haproxy-cve-2026-49975-http2-bomb)).

### Per connection, or per client?

All of the built-in HTTP/2 defences are **per connection**:

- nginx: the per-iteration stream limit, flood counters and keepalive_requests.
- HAProxy: glitches threshold and max-total-streams.
- h2o: reset_budget and dos-delay.

Per-client protection needs the general mechanism: nginx `limit_req`/`limit_conn`, or a HAProxy stick-table with `glitch_rate`/`http_req_rate` keyed on `src`. h2o has no built-in per-client protection.

### Directive reference

**nginx** ([http2 docs](https://nginx.org/en/docs/http/ngx_http_v2_module.html), [core docs](https://nginx.org/en/docs/http/ngx_http_core_module.html)):

| directive | default | note |
|---|---|---|
| http2_max_concurrent_streams | 128 | per connection; source [L346-L347](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2_module.c#L346) |
| http2_recv_buffer_size | 256k | "per worker input buffer"; http context only |
| http2_chunk_size | 8k | response slicing; larger values hurt prioritization (HOL) |
| http2_body_preread_size | 64k | per-request body buffer before processing |
| keepalive_requests | 1000 | was 100 before 1.19.10; replaced http2_max_requests in 1.19.7; on reaching it HTTP/2 sends GOAWAY ([L1341-L1349](https://github.com/nginx/nginx/blob/release-1.31.8/src/http/v2/ngx_http_v2.c#L1341)) |
| keepalive_time | 1h | max lifetime of a keep-alive connection; also triggers GOAWAY (same lines) |
| keepalive_timeout | 75s | replaced http2_idle_timeout |
| large_client_header_buffers | 4 8k | one buffer = max HPACK field; replaced http2_max_field_size/header_size |
| client_header_timeout | 60s | replaced http2_recv_timeout |
| client_body_timeout | 60s | |
| send_timeout | 60s | |

The 1.19.7 removals are in [CHANGES](https://nginx.org/en/CHANGES).

**HAProxy** ([configuration.txt L4151-L4372](https://github.com/haproxy/haproxy/blob/v3.3.0/doc/configuration.txt#L4151); defaults in [mux_h2.c L474-L484](https://github.com/haproxy/haproxy/blob/v3.3.0/src/mux_h2.c#L474)):

| directive | default | note |
|---|---|---|
| tune.h2.max-concurrent-streams | 100 | common default |
| tune.h2.fe.max-concurrent-streams | (common) | "recommended not to change" |
| tune.h2.fe.max-total-streams | 0 (none) | graceful GOAWAY after N; hard violation at N + max-concurrent → ENHANCE_YOUR_CALM ([L3480-L3488](https://github.com/haproxy/haproxy/blob/v3.3.0/src/mux_h2.c#L3480)) |
| tune.h2.fe.glitches-threshold | 0 (off) | kill the connection at N glitches; the doc suggests "hundreds or thousands" ([code L1677-L1689](https://github.com/haproxy/haproxy/blob/v3.3.0/src/mux_h2.c#L1677)) |
| tune.glitches.kill.cpu-usage | 0 | only kill when the thread CPU is above this |
| tune.h2.fe.rxbuf | ~1600k | shared per-connection upload buffer; one buffer per stream guaranteed; "does not cause any unfairness" (unlike raising initial-window-size) |
| tune.h2.fe.initial-window-size | tune.bufsize (16384) | |

**h2o** ([http2_directives.mt](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/srcdoc/configure/http2_directives.mt)):

| directive | default |
|---|---|
| http2-max-streams | 100 (advertised; above it, REFUSED_STREAM, [L693-L696](https://github.com/h2o/h2o/blob/7bbb3e39bf03027344f36b4650e5383c3b6b1014/lib/http2/connection.c#L693)) |
| http2-max-concurrent-requests-per-connection | 100 |
| http2-max-concurrent-streaming-requests-per-connection | 1 |
| http2-idle-timeout | 10 s |
| http2-input-window-size | 16 MiB |
| http2-dos-delay | 100 ms |
| http2-graceful-shutdown-timeout | 0 (wait for the peer) |

---

## Takeaways for a cooperative runtime (inference, not vendor claims)

1. **Count a stream until its handler releases it, not until the protocol closes it.** HAProxy's `nb_sc` together with read backpressure resists Rapid Reset and MadeYouReset by design. nginx and h2o needed patches that add counters.
2. **Separate "streams admitted" from "handlers running".** h2o's `max-streams` versus `max-concurrent-requests`, and HAProxy's server maxconn with its queue. This gives a place to queue work without refusing streams.
3. **Make every built-in limit per connection.** Per-client limits are a separate, opt-in layer keyed on an identity you trust. That identity needs explicit proxy trust and IPv6 prefix masking. HAProxy's `ipmask` without a mask6 fails silently on IPv6, and nginx has no masking at all.
4. **Use a hard deadline for the whole request header**, like HAProxy's `timeout http-request`, as well as idle timeouts. nginx `limit_conn` does not count connections that are still sending headers.
5. **Do not wait forever by default during graceful shutdown.** All three do (nginx, HAProxy, h2o). That is acceptable when a supervisor sends SIGKILL, but it is not a fairness guarantee.


# Kestrel and Pingora: resource fairness

Scope: per-client limits, global limits, per-route limits, stuck handlers, shutdown, HTTP/2 DoS.

How this was checked:

- Kestrel source was read at tag `v9.0.0` (sparse clone). Key values were re-checked at `v10.0.14` and they are the same (EYC count 20, `MaxTrackedStreams`, header 2x check, flow-control queue 4x, abort wait 1000 ms, Http2Limits defaults).
- Pingora source was read at tag `0.9.0`. The `h2` crate was read at `v0.4.16`, which is the minimum Pingora 0.9.0 asks for.
- "Inference" means I read the code and drew a conclusion, but did not run it.
- "UNVERIFIED (memory)" means I did not find a source.

Link bases:

- `K` = https://github.com/dotnet/aspnetcore/blob/v9.0.0/src/Servers/Kestrel/Core/src
- `M` = https://github.com/dotnet/aspnetcore/blob/v9.0.0/src/Middleware
- `P` = https://github.com/cloudflare/pingora/blob/0.9.0
- `H` = https://github.com/hyperium/h2/blob/v0.4.16

---

## KESTREL

### 1. Per-client limits

**Kestrel has no built-in per-client limit.** The only connection limits are global counters: `MaxConcurrentConnections` and `MaxConcurrentUpgradedConnections`, both `long?`, both default null. See `K/KestrelServerLimits.cs#L42-L43` and the docs at `#L208-L256`. Per-HTTP/2-connection limits exist (section 5), but a client can open many connections.

Per-client fairness comes from the app's middleware:

- **Rate limiting middleware** (`Microsoft.AspNetCore.RateLimiting`). `PartitionedRateLimiter.Create<HttpContext,string>` takes any key. The docs show keys on `Connection.RemoteIpAddress`, on user identity, on API key and on path. Source: https://learn.microsoft.com/en-us/aspnet/core/performance/rate-limit (section "Rate limiting partitions").
  - The default rejection status is **503**, not 429: `RejectionStatusCode = StatusCodes.Status503ServiceUnavailable`. Source: `M/RateLimiting/src/RateLimiterOptions.cs#L35-L41`.
  - The counters are in memory and per instance. "The effective limit is the configured limit multiplied by the number of instances." Source: the rate-limit docs page above.
  - Docs warning, quoted: "If you create partitions by using user input, your app becomes vulnerable to Denial of Service (DoS) attacks. For example, if you create partitions by using client IP addresses, your app becomes vulnerable to Denial of Service attacks that use IP Source Address Spoofing." The chained-limiter sample also warns: "Each distinct key creates and caches its own limiter, so partitioning on unbounded user-controlled input can exhaust memory." Source: the same docs page.
- **Forwarded headers middleware.** It decides what `RemoteIpAddress` means behind a proxy. Defaults are in `M/HttpOverrides/src/ForwardedHeadersOptions.cs`:
  - `ForwardLimit = 1` (#L76).
  - `KnownProxies = { IPv6Loopback }` (#L81).
  - `KnownNetworks = { 127.0.0.0/8 }` (#L86).
  - `ForwardedHeaders` has no initializer (#L69), so it is `None` by default. This is an inference from the C# default for an enum.

  So out of the box only loopback proxies are trusted, and one hop is processed. Docs: https://learn.microsoft.com/en-us/aspnet/core/host-and-deploy/proxy-load-balancer. UNVERIFIED (memory): `ASPNETCORE_FORWARDEDHEADERS_ENABLED=true` turns it on and clears the known lists. I did not extract that text.

### 2. Global limits and defaults

All of these are in `K/KestrelServerLimits.cs` unless the row says otherwise.

| Limit | Default | Source |
|---|---|---|
| MaxConcurrentConnections | null (no limit) | #L42, docs #L208-L230 |
| MaxConcurrentUpgradedConnections | null (no limit) | #L43, #L232-L256 |
| MaxResponseBufferSize | 64 KiB | #L16, #L44-L66 |
| MaxRequestBufferSize | 1 MiB | #L20, #L68-L87 |
| MaxRequestLineSize | 8 KiB | #L23 |
| MaxRequestHeadersTotalSize | 32 KiB | #L26 |
| MaxRequestHeaderCount | 100 | #L33 |
| MaxRequestBodySize | 30,000,000 bytes | #L30 |
| KeepAliveTimeout | 130 s | #L37, #L170-L174 |
| RequestHeadersTimeout | 30 s | #L39, #L188-L192 |
| MinRequestBodyDataRate | 240 B/s, 5 s grace | #L358-L360 |
| MinResponseDataRate | 240 B/s, 5 s grace, enforced **per write** | #L362-L384 |
| Http2.MaxStreamsPerConnection | 100 | `K/Http2Limits.cs#L15` |
| Http2.HeaderTableSize | `Http2PeerSettings.DefaultHeaderTableSize` (4096: UNVERIFIED (memory)) | `K/Http2Limits.cs#L16` |
| Http2.MaxFrameSize | `Http2PeerSettings.DefaultMaxFrameSize` (16 KiB: UNVERIFIED (memory)) | `K/Http2Limits.cs#L17` |
| Http2.MaxRequestHeaderFieldSize | 32 KiB | `K/Http2Limits.cs#L18` |
| Http2.InitialConnectionWindowSize | 1 MiB | `K/Http2Limits.cs#L19` |
| Http2.InitialStreamWindowSize | 768 KiB, which is 3/4 of the connection window | `K/Http2Limits.cs#L20` |
| Http2.KeepAlivePingDelay | `TimeSpan.MaxValue` (off) | `K/Http2Limits.cs#L21` |
| Http2.KeepAlivePingTimeout | 20 s | `K/Http2Limits.cs#L22` |

Notes on these limits:

- **What happens when a connection is over the limit.** `ConnectionLimitMiddleware` calls `TryLockOne()`. If that fails, it logs `ConnectionRejected` and calls `connection.DisposeAsync()`. The socket is closed right away. There is no 503 and no HTTP response at all. Source: `K/Middleware/ConnectionLimitMiddleware.cs#L32-L41`. The middleware is installed only when the limit is set. Source: `K/Internal/KestrelServerImpl.cs#L197` and `#L431-L445`.
- **Upgraded connections over the limit.** Here `Upgrade()` throws `InvalidOperationException(UpgradedConnectionLimitReached)`. The app sees this as an exception. Source: `K/Internal/Http/HttpProtocol.FeatureCollection.cs#L271-L274`.
- **MinResponseDataRate per write.** Doc text: "Whenever the server writes a chunk of data, a timer is set to the maximum of the grace period ... or the length of the write in bytes divided by the data rate ... The connection is aborted if the write has not completed by the time that timer expires." Source: #L373-L380. Neither data rate applies to upgraded connections (#L350, #L370).
- **Pipe pause thresholds.** `MaxResponseBufferSize` is the point where "write calls begin to block or return tasks that don't complete" (#L44-L48). The HTTP/2 per-stream request body pipe pauses at `InitialWindowSize + 1`. Source: `K/Internal/Http2/Http2Stream.cs#L654`. The connection window of 1 MiB equals the Sockets transport `MaxReadBufferSize`, according to the comment at `K/Http2Limits.cs#L19`.

### 3. Per-route concurrency

- **Rate limiting middleware, per endpoint.** Use `.RequireRateLimiting("policy")` or `[EnableRateLimiting]` / `[DisableRateLimiting]`. Limiter types are fixed window, sliding window, token bucket and concurrency. The concurrency limiter has `PermitLimit`, `QueueLimit` and `QueueProcessingOrder`. `RateLimiter.CreateChained` can put a token bucket and a concurrency limiter on one endpoint. A rejection anywhere in the chain disposes the earlier leases. Source: https://learn.microsoft.com/en-us/aspnet/core/performance/rate-limit.
  - With `GlobalLimiter` plus a named policy, both apply. The docs say "The global limiter still runs on the endpoint with this attribute applied."
- **The old `ConcurrencyLimiter` middleware is `[Obsolete]`**: "Concurrency Limiter middleware has been deprecated ... Update the app to use concurrency features in rate limiting middleware." When the queue is full it writes an empty 503. Source: `M/ConcurrencyLimiter/src/ConcurrencyLimiterMiddleware.cs#L13` and `#L81`.
- **Inference:** a per-route concurrency limit counts a stuck handler until it returns. The lease is held for the whole request, so a handler that ignores cancellation keeps its permit. The route fills up, but other routes are not affected. That is the isolation a per-route concurrency limiter buys.

### 4. A handler that never finishes after the client left

- **HTTP/2 client RST_STREAM.**
  - `AbortRstStreamReceived()` lowers the connection's client-visible active-stream count **at once**, then calls `Abort`. Source: `K/Internal/Http2/Http2Stream.cs#L560-L567`.
  - `AbortCore` stops output, cancels `RequestAborted` and poisons the request body pipe. Source: `#L613-L626`.
  - The handler is never stopped by force. It must observe `RequestAborted`, or fail on its next body read or write.
- **The server-side reset path does the same.** `ResetAndAbort` lowers the client count, writes RST_STREAM and runs `AbortCore`. Source: `K/Internal/Http2/Http2Stream.cs#L595-L611`.
- **Where a stuck handler stays visible.** The stream stays in the `_streams` dictionary until the app completes and the drain period ends. Only `OnStreamCompleted` puts it on the completed queue. Source: `K/Internal/Http2/Http2Stream.cs#L187` and `K/Internal/Http2/Http2Connection.cs#L1410-L1452`.
  - So **a stuck handler frees its MAX_CONCURRENT_STREAMS slot, but still counts in `_streams.Count`**. That count is checked against `MaxTrackedStreams = max(2 x MaxConcurrentStreams, 100)`, which is 200 by default. Source: `K/Internal/Http2/Http2Connection.cs#L219-L222`.
  - Inference: on one connection, a client can hold about 200 running handlers at most. Above that, new streams get ENHANCE_YOUR_CALM (section 5).
- **Connection teardown waits for handlers.** After a connection-level abort, Kestrel aborts every stream. It then loops `while (_serverActiveStreamCount > 0) await _streamCompletionAwaitable;`, which means it waits for handlers to finish. The exception is when the EYC limit is on and the error is a connection error. Source: `K/Internal/Http2/Http2Connection.cs#L455-L470`. The comment there links issue #51307: "For some reason, this loop doesn't terminate when we're trying to abort."
- **HTTP/1.1.** Inference, not traced line by line: the connection's execution task includes the request loop, which awaits the app. So a stuck handler keeps its connection task alive.

**Graceful shutdown.**

- **The default `HostOptions.ShutdownTimeout`** was 5 s in .NET 5 and .NET 6. It has been **30 s since .NET 7**, not .NET 8. Sources:
  - https://github.com/dotnet/runtime/blob/v6.0.0/src/libraries/Microsoft.Extensions.Hosting/src/HostOptions.cs#L18 (5 s)
  - https://github.com/dotnet/runtime/blob/v7.0.0/src/libraries/Microsoft.Extensions.Hosting/src/HostOptions.cs#L18 (30 s)
  - https://github.com/dotnet/runtime/blob/v9.0.0/src/libraries/Microsoft.Extensions.Hosting/src/HostOptions.cs#L24 (30 s)
  - API page: https://learn.microsoft.com/en-us/dotnet/api/microsoft.extensions.hosting.hostoptions.shutdowntimeout
- **Kestrel's stop sequence.**
  1. Unbind the listeners.
  2. Run `CloseAllConnectionsAsync(token)`, which is graceful (GOAWAY or keep-alive close) and lasts until the host token fires.
  3. If that fails, run `AbortAllConnectionsAsync()`.

  Source: `K/Internal/Infrastructure/TransportManager.cs#L120-L150`.
- **The abort step waits at most 1 second, then returns anyway:** `Task.WhenAny(allAbortedTask, Task.Delay(1000))`. It logs `NotAllConnectionsAborted`. Source: `K/Internal/Infrastructure/TransportConnectionManager.cs#L72-L91`.
- **So yes, the host can return while handlers still run.** The fact is that Kestrel gives up after about 30 s + 1 s. The inference is that .NET has no forced task kill, so a handler that ignores `RequestAborted` keeps running until the process exits.

### 5. HTTP/2 DoS

**Rapid Reset, CVE-2023-44487.**

- Advisory: https://github.com/dotnet/aspnetcore/issues/51264. Fixed in 6.0.23 and 7.0.12; see https://advisories.gitlab.com/pkg/nuget/microsoft.aspnetcore.all/CVE-2023-44487.
- The mechanism in the source:
  - When `_streams.Count > MaxTrackedStreams` (default 200), a new stream gets RST_STREAM(ENHANCE_YOUR_CALM). Source: `K/Internal/Http2/Http2Connection.cs#L1306-L1321`.
  - Each EYC is counted. If the count goes over `5 x MaxEnhanceYourCalmCount` within a 5-tick window (ticks are about 1 s), the connection is aborted with GOAWAY(ENHANCE_YOUR_CALM) and `ConnectionEndReason.StreamResetLimitExceeded`. Source: `#L1312-L1318`, with the reset each window at `#L1399-L1402`.
- The knob is the AppContext switch **`Microsoft.AspNetCore.Server.Kestrel.Http2.MaxEnhanceYourCalmCount`**. The default is 20, "empirically derived". A value of zero or less turns it off. It is not a public option; see the TODO that links issue #51308. Source: `K/Internal/Http2/Http2Connection.cs#L64-L102`.
- The second knob is the AppContext switch **`Microsoft.AspNetCore.Server.Kestrel.Http2.MaxConnectionFlowControlQueueSize`**. The default is `4 x MaxTrackedStreams`, which is 800. If the output scheduling channel is full, the connection is aborted with INTERNAL_ERROR: "This can happen if a client resets streams faster than we can clear them out". Sources: `K/Internal/Http2/Http2FrameWriter.cs#L36-L72`, `#L136-L143` and `#L164-L173`. The TODO links issue #51309.
- **Scope: all of this is per connection.** Nothing is tracked per client IP.
- The normal stream limit: when the client goes over MAX_CONCURRENT_STREAMS, it gets REFUSED_STREAM as a stream error. Source: `#L1290-L1300`.

**CONTINUATION flood (April 2024).**

- CERT VU#421644 lists Microsoft as "Not Affected" (April 2, 2024). Source: https://kb.cert.org/vuls/id/421644.
- The guard in the source: the decoded header size is counted per header, and the connection gets a PROTOCOL_ERROR when it goes over `2 x MaxRequestHeadersTotalSize`, which is 64 KiB by default. The check happens during HPACK decode, before END_HEADERS. Source: `K/Internal/Http2/Http2Connection.cs#L1570-L1582`.
- Inference: this does not bound a stream of empty CONTINUATION frames, which decode to no header bytes. I did not trace how Kestrel buffers those.

**MadeYouReset, CVE-2025-8671.**

- CERT VU#767506 lists Microsoft as "Not Affected" (statement dated September 16, 2025). Source: https://kb.cert.org/vuls/id/767506.
- Inference about why: the server-side reset path frees the client stream count (`Http2Stream.cs#L607`). But the stream stays in `_streams` while its handler runs, so the same `MaxTrackedStreams` and EYC guard above caps it at about 200 per connection.
- I found no Microsoft advisory that names Kestrel for this CVE.

---

## PINGORA (0.9.0)

### Per-client limits

**Pingora has no built-in per-client limit.** The `pingora-limits` crate gives building blocks that the app calls in its own filters:

- `Rate` is a two-interval estimator. It interpolates between the previous and current window counts, as described in Cloudflare's "counting things" post. Source: `P/pingora-limits/src/rate.rs#L15-L60`.
- `Inflight` counts in-flight work. `incr(key, n)` returns a `Guard` and the estimate, and the count goes down when the guard is dropped. Source: `P/pingora-limits/src/inflight.rs#L23-L74`.
- Both are keyed by any `Hash` value and sit on a lock-free Count-Min Sketch, so counts are estimates. Source: `P/pingora-limits/src/estimator.rs#L15-L25`.
- The quickstart keys a `Rate` on an `appid` header inside `request_filter` and returns 429. Source: `P/docs/user_guide/rate_limiter.md`. Docs: https://docs.rs/pingora-limits.

There is also a connection filter: the `ConnectionFilter::should_accept(&SocketAddr)` trait. It runs after TCP accept and before TLS. If it returns false the connection is dropped. It needs the `connection_filter` feature flag. It is a hook, not a limit. Source: `P/pingora-core/src/listeners/connection_filter.rs`.

### Global and overload limits

- **There is no global max-connections setting.** A grep of `pingora-core/src` outside `connectors/` finds no `Semaphore`, `max_conn` or `connection_limit`. Inference: overload control is left to the app.
- `HttpServerOptions.keepalive_request_limit` is the maximum number of requests per downstream connection. The default is **no limit**, unlike nginx. Source: `P/pingora-core/src/apps/mod.rs#L76-L85`.
- `h2_idle_timeout` defaults to None. Source: `#L87-L91`.
- **HTTP/2 server defaults are bounded since a recent release**:
  - `max_header_list_size` 64 KiB and `max_concurrent_streams` 100. Source: `P/pingora-core/src/protocols/http/v2/server.rs#L55-L75`.
  - The CHANGELOG says "Default HTTP/2 server limits are bounded rather than unbounded." Source: `P/CHANGELOG.md#L25`.
- **A per-connection malformed-stream budget**: `MAX_MALFORMED_STREAMS_PER_CONN = 32`. When it is reached, the connection is torn down with a `warn!` log. It is not configurable (there is a TODO). "Valid streams do not reset it." Sources: `P/pingora-core/src/protocols/http/v2/server.rs#L59-L63` and `#L367-L380`.

### Graceful shutdown

- **`grace_period_seconds`.** When it is not set, the default is `EXIT_TIMEOUT = 300 s`. The server sleeps this long so existing sessions can finish. Sources: `P/pingora-core/src/server/mod.rs#L61-L63` and `#L803-L812`.
- **`graceful_shutdown_timeout_seconds`.** When it is not set, the default is **5 s**. It is passed to tokio `Runtime::shutdown_timeout`. A quick shutdown uses 0. Source: `#L815-L838`.
- **Inference about what this means.** tokio `shutdown_timeout` drops the remaining tasks, so a stuck session future is cancelled at its next `.await` point. A task that spins without yielding is not stopped.
- `CLOSE_TIMEOUT = 5 s` is the wait before the listening sockets close during an upgrade. Source: `#L64-L66`.
- Field docs: `P/pingora-core/src/server/configuration/mod.rs#L94-L97`. User guide: `P/docs/user_guide/graceful.md`, which says "A request that can finish within the grace period is guaranteed not to be terminated."
- **HTTP/2 on shutdown.** The connection loop selects on the shutdown watch first (`biased`) and calls `conn.graceful_shutdown()`, which sends GOAWAY. It then keeps accepting streams that are already in flight. Source: `P/pingora-core/src/protocols/http/v2/server.rs#L135-L175`.

### When the downstream disconnects

- **HTTP/1.**
  - `read_body_or_idle` returns `ConnectionClosed` ("Prematurely before response header is sent") on client FIN when `abort_on_close` is true, which is the default. Sources: `P/pingora-core/src/protocols/http/v1/server.rs#L204` and `#L1217-L1229`. Docs: `#L1403-L1410`.
  - The proxy body loop `tokio::select!`s on this downstream read and on the upstream side. Source: `P/pingora-proxy/src/proxy_h1.rs#L553-L580`.
  - Inference: the error ends the proxy future, and dropping it cancels the upstream work at its await point.
  - Exception: during cache fill the downstream error is ignored, so that the upstream can finish writing the cache. Source: `#L563-L568`.
- **HTTP/2.**
  - `watch_h2_stream_close()` resolves on client RST_STREAM. Source: `P/pingora-core/src/protocols/http/server.rs#L699-L710`.
  - The H2 proxy races the upstream body write against it. The stated reason is that a write blocked on upstream flow control "would otherwise keep the downstream stream handles referenced while a downstream RST_STREAM goes unobserved, pinning the downstream connection window credit". Source: `P/pingora-proxy/src/proxy_h2.rs#L1080-L1100`.
- **Error mapping.** Downstream `WriteError`, `ReadError` and `ConnectionClosed` map to status 0, which means no response is attempted. Upstream errors map to 502. Source: `P/pingora-proxy/src/proxy_trait.rs#L640-L660`.
- **The key difference from Kestrel.** Rust futures can be dropped, so Pingora cancels at the await point by dropping. Kestrel signals a token and waits for the app.

### HTTP/2 DoS (through the `h2` crate)

Pingora asks for `h2 >= 0.4.16`. Source: `P/Cargo.toml#L44`.

- **Rapid Reset.**
  - `h2` `max_pending_accept_reset_streams` defaults to 20. Doc text: streams reset before the app accepts them "don't count towards the max concurrent streams ... When the number of remotely-reset streams sitting in the pending-accept queue reaches this maximum value, a connection error with the code of `ENHANCE_YOUR_CALM` will be sent". Sources: `H/src/server.rs#L927-L943` and `H/src/proto/mod.rs#L34`.
  - This option was added in h2 0.3.17 (`H/CHANGELOG.md#L133-L136`).
  - Also in h2: `max_concurrent_reset_streams` (default 50) and a reset duration of 1 s. Source: `H/src/proto/mod.rs#L42-L45`. The 0.4.12 CHANGELOG says "Fix default limits on max stored reset streams and duration to more reasonable values."
- **MadeYouReset.**
  - `max_local_error_reset_streams` defaults to 1024. Doc text: "Too many of these often indicate a malicious client ... the server will issue GOAWAYs with an error code of `ENHANCE_YOUR_CALM`". Sources: `H/src/server.rs#L909-L922` and `H/src/proto/mod.rs#L35`. This was added in h2 0.4.2 as "Limit error resets for misbehaving connections" (`H/CHANGELOG.md#L92-L94`).
  - Cloudflare's post says: "Versions of h2 prior to 0.4.11 were potentially susceptible to MadeYouReset". It also says Cloudflare itself was not exposed because "Pingora does not itself terminate inbound HTTP connections to Cloudflare's network." Source: https://blog.cloudflare.com/madeyoureset-an-http-2-vulnerability-thwarted-by-rapid-reset-mitigations/.
  - The Pingora CHANGELOG bumped the minimum h2 version for this. Source: `P/CHANGELOG.md#L236`.
- **CONTINUATION flood.** h2 caps the number of CONTINUATION frames per header block at `max(header_list_max / frame_max x 1.25, 5)`. This was added in 0.4.4: "Limit number of CONTINUATION frames for misbehaving connections." Sources: `H/src/codec/framed_read.rs#L110-L117` and `H/CHANGELOG.md#L82-L84`. Pingora's CHANGELOG also notes the h2 update for RUSTSEC-2024-0332 (`P/CHANGELOG.md#L432`).
- **Scope: every h2 limit above is per connection.** There is nothing per client in h2 or pingora-core.

---

## What this means for the design (inference, for the larger report)

- **Neither server has per-client limits in the core.** Both leave them to app code, and Kestrel's docs warn that IP-keyed partitions can be spoofed and can use unbounded memory.
- **Both bound HTTP/2 abuse per connection only.** A client with N connections gets N times the budget. Kestrel's connection cap is global and off by default. Pingora has no connection cap.
- **Kestrel's real guard against stuck handlers is `MaxTrackedStreams`** (200), not MAX_CONCURRENT_STREAMS. A reset stream with a live handler still counts toward it.
- **Kestrel's shutdown gives up after about 1 s of abort waiting and returns**, even with handlers still running. Pingora's runtime shutdown drops tasks after 5 s, following a 300 s grace period by default.
- **A cooperative runtime like ours behaves like Kestrel, not Pingora.** We can only signal cancellation. So the accounting must keep counting a cancelled-but-running handler, the way Kestrel's `_streams.Count` does, rather than freeing its slot when the reset is sent.
