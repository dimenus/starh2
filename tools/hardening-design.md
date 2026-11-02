# starh2 hardening: no client and no broken route can starve the server

Task t-3759 (widens t-3737). Status: DESIGN v2, after one panel round (Opus,
Fable, gpt-6-astra; `tools/t3759-panel/design-*.md`, and the Opus and Fable
answers in the t-3737 session). Nothing here is built except the t-3737 spike
(`839dd69`).

Evidence:

- Resource inventory at `839dd69`, file:line and arithmetic: `tools/t3759-inventory.md`
- What Envoy, nginx, HAProxy, h2o, Go, hyper, Kestrel, Pingora do, with
  sources: `tools/t3759-research.md`
- The t-3737 stuck-handler panel: `tools/t3759-panel/`

## Premises (settled)

1. No external users; breaking changes are free. No shims.
2. zio is cooperative: a task can be asked to cancel, never forced. A handler
   that never yields holds its executor thread; nothing in the server can
   take it back.
3. Memory a running task can reach is never freed or reused.
4. The spike's shape stands: an HTTP/2 handler reaches only its `StreamLink`
   and enters the connection through a gate; the connection's slot is freed
   when the connection ends.
5. A process-wide panic is never the answer to client or handler behaviour.
6. Share-nothing over shared state; zio primitives only (AGENTS.md). One
   exemption, named below: the executor watchdog runs on an OS thread.

## The goal, stated as a test

For every resource in the inventory except executor time (row 5): one client
identity, acting alone with many connections, serially or at once, cannot hold
more than its share while the server is under pressure; and one route whose
handler misbehaves cannot hold more than the route's share. Every other client
and route keeps working. Each claim is an attack test on real sockets that
fails today and passes after.

Executor time is not contained, because premise 2 makes it impossible: N
requests to a route that never yields stop N executors. That row is detected
and reported, and the goal says so.

Not a goal: a distributed attack from many identities. Past the shares, the
server-wide limits and the overload ladder decide.

## What the evidence says

- Nothing keys on the peer address (inventory section 3). Every limit is per
  stream, per connection or server-wide, and per-connection limits vanish when
  the connection ends.
- The cheapest attacks need no broken route (inventory section 4): HEADERS
  without END_STREAM then silence (no timer at all; 16 connections fill every
  stream token and the 256 MiB pool); silent TLS connects (128 hold every
  handshake slot); 1,024 sockets (the accept loop parks for every endpoint);
  an accept error such as EMFILE (the endpoint's accept loop exits for good);
  slow bodies; a zero window with 1-byte WINDOW_UPDATEs.
- Broken-route attacks: orphans hold task tokens, pool bytes and compression
  contexts; on HTTP/1.1 an abandoned request on a cancel-ignoring route pins
  one of 64 reaper workers; a non-yielding handler freezes an executor.
- Every surveyed server's HTTP/2 defences are per connection; per-client
  limits exist only as opt-in limiters. Go, Kestrel and HAProxy charge a
  handler until it really returns, which is why MadeYouReset missed them.
  Only Envoy has an overload model (research, synthesis).

## The rule everything follows

**A resource is charged from the moment it is taken until it is really
released, to every party that holds it.** An orphan handler is still charged.

Two things the round-1 draft merged, kept apart (all three reviewers):

- **Protocol-open streams** are released when the stream closes, RST
  included, as today (`session.zig:1120-1126`). A browser may reopen within
  `SETTINGS_MAX_CONCURRENT_STREAMS` after a reset; Datastar resets SSE streams
  on page change.
- **Task handlers** are held until the handler returns, orphan included.

The parties ("accounts"):

| account | lives | keyed by |
|---|---|---|
| server | until the last holder ends (see Lifetime) | — |
| client | while anything is charged to it | peer identity (below) |
| client aggregate (IPv6 only) | while anything is charged to it | the /56 of the identity |
| route | server | route index (`Router.match` will return it; today `Match.found` carries only the handler) |
| connection | until its socket ends | — |

A charge is a small fixed list of account pointers taken at admission. The
take is all-or-nothing in a fixed order (client, aggregate, route, server),
each a bounded CAS, undone in reverse on refusal. Release walks the same list.
A connection-level charge moves off the connection when the connection ends
and stays on the others.

Validation at boot, for every byte and count resource: stream ≤ connection ≤
client ≤ server (the draft broke this: a 64 MiB connection pool limit above a
32 MiB client share is dead).

### The client table

- Fixed size, sized at boot: capacity = `max_connections` +
  `concurrent_tls_handshakes` + the task-handler limit, because an entry
  exists only while something is charged and every charge is bounded. No
  allocation after boot.
- Open addressing with backward-shift deletion (no tombstones: a client that
  cycles through identities must not turn lookups into scans). The hash is
  keyed with a random seed at boot (keys are attacker-chosen).
- Insert at accept races a delete of the same key when its last charge drops
  on another executor. The table needs a stated protocol for that; the
  implementer proposes it and a test drives the race. Alternative to weigh in
  the build: place every connection of one identity on executor
  `hash(identity) mod N`, so each executor owns its own table with no atomics
  and one client's CPU is confined to one executor. Its cost: behind a proxy
  without PROXY protocol, all traffic lands on one core.

## Client identity

Per endpoint, `client_identity = .peer | .proxy_protocol | .none`:

- `.peer` (default): the socket peer address. IPv4 per address. IPv6 by /64,
  plus a /56 aggregate account with 4x the /64 share, because one subscriber
  usually gets a /56 or a /48 and 8 /64s would otherwise give 8 shares.
- `.proxy_protocol`: PROXY protocol v2 only, accepted only from configured
  source addresses or on a Unix socket. Shares are charged **after** the
  header is parsed, never to the proxy's address. Connections still waiting
  for the header have their own small cap and a deadline. A v2 `LOCAL`
  command (proxy health checks) is keyed to the socket address.
- `.none`: no per-client shares on that endpoint; logged at boot. This is the
  honest setting behind an L7 proxy without PROXY protocol (an ALB, a CDN):
  there, per-client isolation does not exist, and the config says so instead
  of seven numbers someone must remember to raise.
- No X-Forwarded-For: identity is needed at accept and during the handshake.
- Unix socket endpoints without PROXY: `.none` by default.

## Per-client shares, applied under pressure

Users behind one NAT share an identity (an office, a mobile carrier). A share
that always applies refuses them on an idle server; this is why no surveyed
server defaults a per-client limit (Fable, astra). So a share bites only under
pressure:

**A client over its share is refused only while the server resource is at or
above its pressure mark** (default: 2x the share). One attacker still cannot
hold more than the mark; a second client is still served; a crowd behind one
NAT is not refused while there is room. Cost: one atomic load per admission.

| resource | server default | client share | refusal |
|---|---|---|---|
| connections | 1,024 | 128 | close at accept (after the PROXY header on proxy endpoints) |
| TLS handshakes in progress | 128 | 16 | close at accept |
| TLS handshakes started per second | — | token bucket, burst 32, 16/s | close at accept |
| protocol-open streams | 4,096 | 512 | RST_STREAM REFUSED_STREAM |
| task handlers, orphans included | 4,096 | 512 | RST_STREAM REFUSED_STREAM / H1 503 |
| request pool bytes | 256 MiB | 32 MiB | 503 (lease) |
| outbound bytes | 256 MiB | 32 MiB | the existing per-stream shed |
| compression contexts | 128 | 16 | identity encoding |

The handshake rate is new (Opus): a share of handshakes in progress does not
bound how many full handshakes per second one client forces, each an ECDHE
plus a signature.

## Protocol work that holds nothing

HEADERS and DATA are exempt from the frame rate limits (`rates.zig:121-129`),
and an empty or padding-only DATA frame charges no flow-control bytes. A
client can spend parse work without holding anything a share counts. Add
per-connection buckets for HEADERS and for DATA frames that carry no body
bytes, with ENHANCE_YOUR_CALM when exceeded; padding never counts as body
progress. Measure the cost of a flood before and after, on one executor.

## Route shares

Each task route has a limit on task handlers (orphans included) and on the
request-pool bytes they hold, reserved at dispatch and held until the handler
returns. Over either: 503 on that route only. A hard limit, not a breaker.
Default: the server limit divided by the number of task routes, so a Datastar
app with one SSE route can use the whole server; each route can override.
Compression contexts are charged to the route too.

This is what contains many honest clients on one broken route; client shares
cannot, because each client stays under its share.

## Missing deadlines

- **Request body:** the clock starts at HEADERS without END_STREAM, not at the
  first DATA. A minimum rate after a grace period (Kestrel
  `MinRequestBodyDataRate`: 240 B/s after 5 s), counting body bytes only (not
  padding, not empty frames), and paused while the server itself is not
  granting window. Same for HTTP/1.1.
- **Response:** the minimum rate is measured only while bytes are queued for a
  stream and the client is not taking them (Kestrel times outstanding writes).
  An SSE stream that sends one event a minute is not slow; a stream with
  pending bytes and a 1-byte window is.
- **TLS handshake:** the 5 s deadline stays; the shares and the rate above
  stop one client holding the slots.

## Running handlers per connection

There is no new cap: a handler keeps its slot until it returns, there are
`max_streams_per_connection` slots, and 256 more requests can park before
REFUSED_STREAM (`connection.zig:7626-7631`). That is already Kestrel's 2x.
The change: when every slot holds a handler whose stream has ended and the
park queue is full, send GOAWAY ENHANCE_YOUR_CALM instead of refusing one
stream at a time.

## Admission and overload

- Over a share under pressure: refuse that client. Never park the accept loop
  for everyone because of one client.
- Pressure-scaled idle timeouts instead of a server-wide idle list (Fable,
  after Envoy `reduce_timeouts`): each actor reads one atomic pressure level
  and shortens `connection_idle_timeout_ns` and the HTTP/1.1 keep-alive wait
  when the server is near full. The oldest idle connection goes first by
  construction; no list, no cross-executor signal.
- Last rung before parking: GOAWAY to the oldest connection of the client
  holding the most connections (Opus). Closing a busy connection does free
  its slot, protocol-open streams, outbound bytes and pre-dispatch leases.
  (The draft's "refuse the top holder" is cut: with shares in place it frees
  nothing.)
- Accept errors never end the loop. EMFILE, ENFILE, ENOBUFS, ENOMEM: count,
  log at most once a minute, back off 10 ms doubling to 1 s, retry.
- Boot: read RLIMIT_NOFILE, raise the soft limit toward the hard limit
  (clamped: on macOS the hard limit can be RLIM_INFINITY and `setrlimit`
  above `kern.maxfilesperproc` fails), and refuse to start when it is below
  `max_connections` plus a margin. File descriptors a handler opens are not
  bounded by the server; that is the application's.

## Stuck handlers

- HTTP/2: the spike's link and gate, with the task charge (client, aggregate,
  route, server) held until the handler returns.
- HTTP/1.1 moves to the same model in its own commit: link, gate, request
  arena from the request pool (t-3537). The reaper and its
  `JoinHandle.cancel` wait are deleted. H1 request data now lives in buffers
  the connection owns (`h1.zig:239-262`), so the reaper cannot go before the
  link and pool move.
- Non-yielding handlers are detected, not contained. The watchdog cannot run
  on the executor it watches (a frozen executor runs no timers), and the
  default runtime has one executor. It runs on an OS thread: each executor
  bumps a switch counter, and zio's existing monitor thread
  (`runMetricsMonitor`, zio `runtime.zig:1789`) samples it and logs an
  executor whose counter has not moved for a limit, with its current task.
  This is zio-fork work: a task in that repo, and starh2 consumes it. Tests
  run with zio's `debug_cancel`, which reports a task that swallowed
  `error.Canceled`. `zio.blockInPlace` is the documented way to call blocking
  code.

## Lifetime and shutdown

An orphan reaches more than the connection (all three reviewers): the
accounting, the request pool, the client table, the route counters, the
balancer, the compression pool, and the zio runtime that runs it. Today the
first ones are value fields of the embedder's `Server` (`server.zig:132-157`),
and `examples/datastar_sse.zig` keeps `Server` on its stack. zio's
`Runtime.deinit` asserts every task stack is back (`runtime.zig:1898`) and
joins the worker threads, which a non-yielding handler never lets finish.

So while a handler still runs, the only safe end is the end of the process.
Returning from `serve` would buy nothing: the caller could not run
`Server.deinit` (the orphan still writes that memory, and a `defer` would run
it anyway) nor `Runtime.deinit`. A reference-counted heap core was considered
and rejected (Ryan, 2026-11-02): it only lets the process keep running after a
stuck shutdown, which nothing needs, and it still leaves the runtime problem.

- **`serve` never returns while a handler still runs.** Shutdown: GOAWAY, wait
  for the drain deadline, request cancel of every handler, wait a short grace
  (1 s, as Kestrel). If handlers remain, log them (count, routes, oldest age)
  and keep waiting, logging again at a fixed interval. The wait ignores
  cancel. The process supervisor's kill ends it, and the log has already
  named the cause. Never a panic, never a use-after-free, no API change.
- This is the `serve` contract, written beside it: `serve` returns only when
  no handler can touch the server again.
- Every `deinit` check becomes a run-time check that logs, not a
  `std.debug.assert`; underflow of any account is a counted run-time error.

## Observability

Counters per resource and per refusal reason, and orphans by age (count,
oldest, bytes). Top holders are read on demand by scanning the fixed client
table, not tracked. One global rate-limited log line for refusals and one for
orphans older than a limit; no per-client or per-route log state.

## What stays out

- Distributed attacks beyond per-client shares.
- Preempting a non-yielding task (premise 2).
- Request-rate limiting per client (nginx `limit_req`): the shares bound what
  a client holds; the protocol-work buckets bound frames it sends; how often
  it may call a route is the application's.
- Task stacks: not a share. `resourceUpperBound` counts committed stack x the
  task-handler limit, and the embedder's `stack_pool` setting is named there.

## Tests

- One attack test per inventory row, real sockets, h2c and TLS where both
  apply, HTTP/1.1 where it applies. Each fails on `839dd69` (or master) and
  passes after, and shows a second, well-behaved identity still served during
  the attack. Each prints its refusal counters, so a test whose attack never
  engaged is visible.
- Identities in tests: 127.0.0.1 against ::1 for the plain path; PROXY v2
  headers to inject identities elsewhere. No test hook for identity. (macOS has
  only 127.0.0.1 on lo0.)
- Rapid Reset and MadeYouReset shapes; empty and padding-only DATA floods;
  a HEADERS flood to complete routes; the handshake rate; the NAT case (many
  connections from one identity on an idle server are not refused).
- **Charge conservation, the biggest risk (Opus, Fable):** five accounts,
  eight resources, every refusal and exit path. After every test, attack tests
  included, assert at quiescence that the client table is empty and every
  account is zero, as run-time checks. One soak drives every refusal rung with
  random RST, close and GOAWAY timing over h2c, TLS and HTTP/1.1, against
  routes that honour and ignore cancel, then asserts zero. Mutation: delete
  each release and each undo line in turn; the suite fails for each.
- Bench against master, `failed == 0` read first, no regression beyond noise.
  Mac and Linux.

## Order of work

One branch, a series of commits, each with its own attack tests and green on
its own, so the series bisects. Pushed together when the last one is done
(Ryan: no half solution).

1. Missing deadlines (body at HEADERS, minimum rates in and out) and accept
   robustness with the RLIMIT_NOFILE check. Need no accounts.
2. The shutdown contract: bounded drain, cancel, grace, then an unbounded
   logged wait; run-time `deinit` checks.
3. Accounts, the client table, client identity, PROXY protocol v2.
4. Per-client shares under pressure; the handshake rate.
5. Route shares; the spike folded in with its charges.
6. Protocol-work buckets.
7. HTTP/1.1 onto links and the request pool; reaper deleted.
8. Overload: pressure-scaled idle timeouts; GOAWAY to the top holder.
9. Observability; the executor watchdog (after the zio-fork task).

## Decisions (Ryan, 2026-11-02)

- Q1. Identity: deferred by Ryan to the design ("I don't know what the
  appropriate identifier is"). The proposal stands and is the item the second
  panel round must stress-test: `client_identity` per endpoint (`.peer`
  default, IPv6 /64 plus a /56 aggregate; `.proxy_protocol` v2 from trusted
  sources, charged after the header; `.none` behind an L7 proxy without PROXY).
  No X-Forwarded-For.
- Q2. Agreed: shares apply only under pressure (mark = 2x share). Client share
  1/8; route share = server / number of task routes.
- Q3. Agreed: HTTP/1.1 moves to the link model in this series, its own commit.
- Q4. Agreed: non-yielding handlers are detected and logged; the watchdog runs
  on zio's monitor thread (zio-fork task first).
- Q5. Decided: `serve` never returns while a handler still runs; after the
  shutdown limit it logs the stuck handlers and keeps waiting. No heap core,
  no `HandlersStuck` error.
- Q6. Agreed: a bisectable series, pushed together.
