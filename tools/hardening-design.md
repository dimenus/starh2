# starh2 hardening: no client and no broken route can starve the server

Task t-3759 (widens t-3737). Status: DESIGN, for panel review and Ryan's
decision. Nothing here is built except the t-3737 spike (`839dd69`).

Sources, read them before you argue with a number:

- Inventory of every finite resource at `839dd69`, with file:line and
  arithmetic: `tools/t3759-inventory.md`
- What Envoy, nginx, HAProxy, h2o, Go, hyper, Kestrel and Pingora do, with
  sources: `tools/t3759-research.md`
- The t-3737 panel on stuck handlers (two rounds): `tools/t3759-panel/`.

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
6. Share-nothing over shared state; zio primitives only (AGENTS.md).

## The goal, stated as a test

For every resource in the inventory: one client, acting alone with many
connections, serially or at once, cannot take more than its share; and one
route whose handler misbehaves cannot take more than the route's share. Every
other client and route keeps working. Each claim is an attack test on real
sockets that fails today and passes after.

Not a goal: protecting against a distributed attack from many addresses. Past
the per-client shares, the server-wide limits and the overload ladder decide.

## What the evidence says

- Today nothing keys on the peer address (inventory section 3). Every limit is
  per stream, per connection or server-wide, and per-connection limits vanish
  when the connection ends.
- The cheapest attacks need no broken route (inventory section 4):
  1. HEADERS without END_STREAM, then silence: no timer at all. 16
     connections x 256 streams hold every stream token and, at 64 KiB each,
     the whole 256 MiB request pool. Then every request on the server fails.
  2. 128 silent TCP connections hold every TLS handshake slot.
  3. 1,024 sockets hold every connection; the accept loop parks, so all
     endpoints stop accepting.
  4. An accept error such as EMFILE ends that endpoint's accept loop for the
     life of the process.
  8. Slow request bodies (one DATA per < 15 s) fill the pool.
  9. Zero window plus 1-byte WINDOW_UPDATEs hold the outbound budget; a tiny
     update counts as progress.
- The broken-route attacks: orphans fill task tokens, the pool, compression
  contexts (rows 6, 10); on HTTP/1.1 an abandoned request on a route that
  ignores cancel pins one of 64 reaper workers (row 7); a non-yielding handler
  freezes an executor (row 5).
- Every surveyed server's HTTP/2 defences are per connection; per-client
  limits exist only as opt-in limiters (nginx `limit_conn`, HAProxy stick
  tables, Envoy local rate limit). Go, Kestrel and HAProxy keep charging a
  handler until it really returns, which is why MadeYouReset did not hit them;
  servers that released at RST while the work ran on were hit (research,
  synthesis 2). Only Envoy has an overload model (research, synthesis 3).

## The rule everything follows

**A resource is charged from the moment it is taken until it is really
released, to every party that holds it.** RST_STREAM, GOAWAY and connection
close end a protocol state; they do not release a charge whose holder still
runs. An orphan handler is still charged.

The parties ("accounts"):

| account | lives | keyed by |
|---|---|---|
| server | process | — |
| client | while anything is charged to it | peer identity (below) |
| route | server | route index (`Router.match` returns it) |
| connection | until its socket ends | — |
| stream | until its handler returns and its frames are gone | — |

A charge is a small fixed list of account pointers taken at admission. The
take is all-or-nothing in a fixed order (client, route, server), each a
bounded CAS on a counter, undone in reverse on refusal. Release walks the same
list. A connection-level charge moves off the connection when the connection
ends and stays on the client, route and server.

The client table is a fixed-size open-addressing table in the server, sized
at boot: an entry exists only while something is charged to it, and every
charge is bounded by the server limits, so the table cannot overflow
(capacity = `max_connections` + `concurrent_tls_handshakes` + the task-handler
limit). No allocation after boot, no eviction.

## Client identity

- Default: the socket peer address. IPv4 per address; IPv6 grouped by /64
  (no surveyed server does this by default; a /64 is what one subscriber
  usually gets, and per-address IPv6 limits are free to evade).
- Unix socket endpoints: one identity per endpoint, unless PROXY protocol.
- Behind a proxy: PROXY protocol v2, opt-in per endpoint, accepted only from a
  configured set of source addresses (or on a Unix socket). No
  X-Forwarded-For: identity is needed before HTTP is parsed, at accept and
  during the TLS handshake.
- A proxy without PROXY protocol makes every user one client. The per-client
  shares would then throttle everyone. Boot cannot detect this. Proposal: the
  per-client share is a config value per resource, and the docs on the
  endpoint say to raise it to the server limit when no PROXY header is used.
  **Decision for Ryan (Q1).**

## Per-client shares

Each server-wide limit gets a per-client share. Proposed default: 1/8 of the
server limit, with a floor that a normal browser or Datastar page never
reaches. **Decision for Ryan (Q2).**

| resource | server default | client share | refusal when over |
|---|---|---|---|
| connections | 1,024 | 128 | close at accept (before TLS) |
| TLS handshakes in progress | 128 | 8 | close at accept |
| open streams | 4,096 | 512 | RST_STREAM REFUSED_STREAM |
| task handlers, orphans included | 4,096 | 512 | RST_STREAM REFUSED_STREAM / H1 503 |
| request pool bytes | 256 MiB | 32 MiB | 503 (lease) |
| outbound bytes | 256 MiB | 32 MiB | the existing per-stream shed |
| compression contexts | 128 | 16 | identity encoding (as today) |

Local development and benches run every client from 127.0.0.1. They get
explicit config, not a loopback exemption: an exemption would let a proxy on
the same host bypass every share.

## Route pools

Each route has a task-handler limit, reserved at dispatch and held until the
handler returns (orphan included), plus the request-pool bytes its handlers
hold. Over either: 503 on that route only. Hard limit, not a breaker: a
short-lived orphan holds its place for microseconds, so no grace period and
no age logic. Default: 1/4 of the task-handler limit and 1/4 of the pool.
A route that needs more (a busy SSE route) raises its own limit.

This is what contains many honest clients on one broken route: the client
shares cannot, because each client stays under its share.

## Missing deadlines

- **Request body:** the body clock starts at HEADERS without END_STREAM, not
  at the first DATA. Add a minimum rate after a grace period, as Kestrel
  (`MinRequestBodyDataRate`, 240 B/s after 5 s): an idle clock reset by every
  byte lets one byte per 14 s hold a stream for ever. Same for HTTP/1.1.
- **Response:** slow-consumer progress is bytes actually sent per second over
  a window, not "any WINDOW_UPDATE". Minimum rate as Kestrel's
  `MinResponseDataRate`.
- **TLS handshake:** keep the 5 s deadline; the per-client share above stops
  one client holding all 128.
- **HEADERS-only stream with END_STREAM and a handler:** covered by the task
  charge and shares, not a timer; a handler may legitimately run for hours (SSE).

## Admission and overload

- Over a per-client share: refuse that client, never park the accept loop for
  everyone.
- Server full (connections): first close idle connections, oldest first (an
  H2 connection with no open stream, an H1 keep-alive between requests), as
  nginx and Envoy do; only then park. This is the first rung of Envoy's
  ladder.
- Pool or outbound near full: refuse new work from the client holding the
  most, before refusing anyone else (Envoy's victim choice adapted: a reset
  frees nothing on demand here, because tasks cannot be killed, so the action
  is refusal and closing that client's idle connections, not stream resets).
- Accept errors: never leave the loop. EMFILE, ENFILE, ENOBUFS, ENOMEM: log
  once per burst, back off 10 ms doubling to 1 s, retry.
- Boot: read RLIMIT_NOFILE, raise the soft limit to the hard limit, and refuse
  to start when the hard limit is below `max_connections` plus a margin for
  listeners, the event loop and handlers.

## Stuck handlers

- HTTP/2: the spike's link and gate, with the charges above (client, route,
  server) held until the handler returns.
- HTTP/1.1 moves to the same model: link, gate, request arena from the request
  pool (t-3537). The reaper and its `JoinHandle.cancel` wait are deleted. A
  request abandoned on a cancel-ignoring route no longer pins a worker.
- RST_STREAM requests a cancel (spike) and the stream's charges stay until the
  handler returns. Per connection, the number of handlers still running after
  their stream ended is capped (2x max streams, as Go and Kestrel); over it,
  stop reading new HEADERS on that connection and send GOAWAY
  ENHANCE_YOUR_CALM. The per-client task share caps the same debt across
  connections.
- Non-yielding handlers: nothing can stop them. Detect and report: a per
  executor heartbeat timer; when an executor misses it by more than a limit,
  log the executor and the task running on it (route if known). Tests run
  with zio's `debug_cancel` so a handler that swallows `error.Canceled` is
  reported. Examples default to more than one executor. `zio.blockInPlace` is
  the documented way to call blocking code. Nothing is refused here: it is an
  application bug to make visible.

## Shutdown

GOAWAY, wait for the drain deadline, request cancel of every handler, wait a
short grace (1 s, as Kestrel), then `serve` returns `error.HandlersStuck`
naming the count and routes. Cancelling `serve` ends the wait early, with the
same result. `deinit` with handlers still running frees nothing they can reach
(request pool, accounts, balancer, compression pool) and logs it: a bounded
leak at process end, never a panic, never a use-after-free. All the `deinit`
checks become run-time checks, not `std.debug.assert`.

## Observability

Counters per resource and per refusal reason; per-route and per-client
current holdings for the top holders; orphans by age (count, oldest, bytes).
Log lines are rate-limited: a client or route is logged when it first hits a
share or first has an orphan older than a limit, then at most once a minute.
Every refusal is countable; none is silent.

## What stays out

- Distributed attacks beyond per-client shares: the server limits and the
  overload ladder are the answer.
- Preempting a non-yielding task: impossible in zio (premise 2).
- Rate limiting of requests per client (nginx `limit_req`): an application
  concern; the shares above bound what a client holds, not how often it asks.

## Tests

One attack test per inventory row, on real sockets, h2c and TLS where both
apply, HTTP/1.1 where it applies. Each must fail on `839dd69` (or master) and
pass after, and must show a second, well-behaved client still served during
the attack. Plus: Rapid Reset and MadeYouReset shapes against the
running-handler cap; PROXY protocol identity; the accept back-off with an
injected EMFILE. Mutation-test each defence. Mac and Linux. Bench against
master with no regression beyond noise.

## Order of work (one branch, lands as a whole)

1. Accounts, client identity, route index from the router.
2. Missing deadlines (body at HEADERS, minimum rates in and out).
3. Per-client shares on connections, handshakes, streams, tasks, bytes.
4. Route pools; the spike folded in with its charges.
5. HTTP/1.1 onto links and the request pool; reaper deleted.
6. Accept robustness and the RLIMIT_NOFILE boot check.
7. Overload ladder: close idle connections, refuse the top holder.
8. Shutdown and `deinit`.
9. Observability.

## Decisions for Ryan

- Q1. Identity: socket peer, IPv6 /64, PROXY protocol v2 opt-in from trusted
  sources, no X-Forwarded-For. Per-client shares configurable for deployments
  behind a proxy without PROXY protocol.
- Q2. Default per-client share 1/8 of each server limit; route default 1/4.
- Q3. HTTP/1.1 moves to the link model in this change (reaper deleted).
- Q4. Non-yielding handlers: detect and log only.
