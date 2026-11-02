# starh2 hardening: no client and no broken route can starve the server

Task t-3759 (widens t-3737). Status: DESIGN v3, after two panel rounds (Opus,
Fable, gpt-6-astra). Round-1 and round-2 reviews: `tools/t3759-panel/`
(astra's in files; the Opus and Fable reviews are summarised in the v2 and v3
commit messages). Ryan's decisions are at the end. Nothing here is built
except the t-3737 spike (`839dd69`).

Evidence:

- Resource inventory at `839dd69`, file:line and arithmetic: `tools/t3759-inventory.md`
- What Envoy, nginx, HAProxy, h2o, Go, hyper, Kestrel, Pingora do, with
  sources: `tools/t3759-research.md`

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
   exemption: the executor watchdog runs on an OS thread.

## The goal, stated as a test

For every resource in the inventory except executor time:

- One client identity, acting alone with many connections, serially or at
  once, holds at most its **cap**, 1/4 of the resource: it is admitted freely
  up to its share (1/8), and past the share only while the server is below the
  pressure mark (1/4 of the resource in use). A second identity is then still
  admitted up to a full share.
- One route whose handler misbehaves holds at most the route's share.

What the goal does not promise, said plainly:

- Pressure gating limits new admissions; it does not take back what an
  identity already holds. Once one identity has pushed use to the mark, every
  other identity is held to its share (1/8) for new work, even with 3/4 of the
  server free. A crowd behind one NAT that holds more than its share is
  refused at that point. This is the trade Q2 accepted.
- Executor time is not contained (premise 2): N requests to a route that never
  yields stop N executors. It is detected and reported.
- A distributed attack from many identities is not contained. Past the caps,
  the server-wide limits decide.
- Admission is check-then-reserve, not one atomic snapshot: concurrent
  admissions can pass a mark by the number in flight, and a byte charge by
  its own size. Tests assert bounds with that tolerance.

## The rule everything follows

**A resource is charged from the moment it is taken until it is really
released, to every party that holds it.** An orphan handler is still charged.

Kept apart:

- **Protocol-open streams** are released when the stream closes, RST
  included, as today (`session.zig:1123` releases only on `.closed`).
- **Task handlers** are held until the handler returns, orphan included
  (`connection.zig:1667` releases the token when the link is freed).

The accounts:

| account | lives | keyed by |
|---|---|---|
| server | until `serve` returns (Q5: never while a handler runs) | — |
| client | while anything is charged to it, or its rate state is not yet refilled | identity (below) |
| client aggregate (IPv6 only) | same | the /48 of the identity |
| route | server | route index (`Router.match` will return it) |
| connection | until its socket ends | — |

A charge is a small fixed list of account record indices taken at admission,
in a fixed order (connection, client, aggregate, route, server), each a
bounded CAS, undone in reverse on refusal. Release walks the same list. When
the connection ends, its own charge is released and the rest stay. Arena growth
after dispatch, orphan growth included, is charged to client, aggregate, route
and server; only the connection charge is behind the gate
(`request_pool.zig:707-714` today drops the connection charge at orphaning;
the others must not drop). The route is unknown when the header lease is taken,
so a request's bytes are charged to its route at dispatch and refused there.

**Underflow:** a release that would take any account below zero is a bug. The
subtraction is refused (the counter saturates at zero), the event is counted
and logged, and nothing is freed on the strength of it.

### Pressure

One pressure mark per resource, at 1/4 of the server limit, read from the
server account. Every account (client, aggregate) is gated on that same
resource-level mark; the mark is never recomputed from an account's own share.
A client over its share is refused only while the resource's server use is at
or above the mark.

### The client table

- **Stable records:** a fixed slab of account records sized at boot, and an
  open-addressing index (backward-shift deletion, no tombstones; the hash is
  keyed with a random seed at boot) that maps a key to a record index. Only
  index entries move; records do not, so a charge's cached record index stays
  valid. Each record has a generation, bumped on reuse.
- **Concurrency:** one `zio.Mutex` guards lookup-or-insert together with the
  record's first reference, and release-to-zero together with removal. A
  charge on a record already referenced is a lock-free CAS. Invariant: whoever
  holds a record index holds a reference, so a count of zero means no holder,
  and the only way to obtain an index is the locked lookup; removal re-reads
  the count under the lock. Contention is at the accept rate only.
- **Rate state outlives charges:** a record whose only remaining content is a
  not-yet-refilled rate bucket stays in the table (otherwise a client that
  reconnects serially regains its burst every time). Such rate-only records
  hold no charge and are evictable: when the slab is full, the rate-only
  record with the oldest refill time is reused.
- **Capacity:** `max_connections` + `concurrent_tls_handshakes` + the
  task-handler limit + a rate-only reserve. If an insert still fails (a bug or
  an exhausted rate reserve with no evictable record), refuse that connection
  and count it; never admit uncounted.
- Rejected: one table per executor with identity pinned to executor
  `hash(identity) mod N`. It defeats the balancer's placement and puts a
  whole CGNAT address or a proxy's traffic on one core; the IPv6 aggregate
  would still be shared across executors.

## Client identity (Q1)

Per endpoint, `client_identity = .peer | .proxy_protocol | .none`:

- `.peer`, the default on TCP endpoints. The key comes from the normalised
  `Request.Peer` (v4-mapped addresses are already `.ip4`,
  `connection.zig:591-593`; keying raw `in6` bytes by /64 would merge every
  IPv4 client on a dual-stack socket). IPv4 per address. IPv6 by /64 (privacy
  addresses rotate inside one /64), plus an aggregate account on the /48
  (configurable prefix), share 1/4, on the same resource mark: one /48 then
  caps at 1/4 like one address. A /56 aggregate would not stop a free tunnel
  /48, which holds 256 /56s.
- `.none`, the default on Unix socket endpoints, and the only honest setting
  behind an L7 proxy that pools connections (an ALB, Cloudflare HTTP), with or
  without PROXY: a pooled upstream connection carries many users. Logged at
  boot.
- `.proxy_protocol`: PROXY v2 only, and only with a non-empty list of trusted
  source addresses (a boot error otherwise), or on a Unix socket. For proxies
  that keep one upstream connection per client (NLB, HAProxy or nginx
  `stream`, Envoy TCP). A connection from an untrusted source is closed at
  accept, never keyed to its own address. TLVs are ignored for identity. The
  header is capped at a small fixed size (the v2 length allows 64 KiB) and
  closed over it. `LOCAL` (health checks) goes to one bounded infrastructure
  account per listener.
  - Connections waiting for the header: the trusted-source check comes first;
    then a small cap of their own (not the TLS handshake slots: the handshake
    permit is taken only after the header) and the preface deadline. The cap
    bounds occupancy, not fairness: a stalled trusted proxy can fill it, and
    the log names the proxy.
- `Request.Peer.unknown` (`getpeername` failed): one shared identity, not an
  exemption.
- No X-Forwarded-For: identity is needed at accept, before HTTP.
- Dual-stack users are two identities (at most two shares); HTTP/2
  coalescing shares one identity across origins; nothing splits by
  `:authority`.

**Failure mode of the default:** `.peer` left on behind an L7 proxy. Every
user is one identity, the server caps that identity at 1/4 under pressure, and
nothing at boot can see it. Mitigation: the rate-limited refusal log line names
the identity, its share of server use and the endpoint's mode
(`refused 10.0.0.5: 92% of connections, endpoint :443 client_identity=.peer`).

## Shares

| resource | server default | share (cap = mark) | refusal |
|---|---|---|---|
| connections | 1,024 | 128 (256) | close at accept (after the PROXY header) |
| connection starts, every endpoint | — | bucket: burst 128, 16/s; gated on connection pressure | close at accept |
| TLS handshakes in progress | 128 | 16 (32) | close at accept |
| TLS handshakes started | — | bucket: burst 128, 16/s; gated on handshake pressure | close at accept |
| protocol-open streams | 4,096 | 512 (1,024) | RST_STREAM REFUSED_STREAM |
| task handlers, orphans included | 4,096 | 512 (1,024) | RST_STREAM REFUSED_STREAM / H1 503 |
| request pool bytes | 256 MiB | 32 MiB (64 MiB) | 503 (lease) |
| outbound bytes | 256 MiB | 32 MiB (64 MiB) | the existing per-stream shed |
| compression contexts | 128 | 16 (32) | identity encoding |

`request_bytes_per_connection` moves from 64 MiB to 32 MiB, so boot
validation `stream ≤ connection ≤ client ≤ server` holds with the defaults.

A bucket's state lives in the client record and outlives the client's charges
(above). The connection-start bucket covers h2c and h1c too, because the
per-connection frame buckets renew with each new connection.

## Protocol work that holds nothing

HEADERS and DATA are exempt from the frame rate limits (`rates.zig:121-129`).

- No HEADERS bucket: one would refuse `h2load -c 1 -m 100`. A HEADERS that
  opens a stream is paid for by stream turnover; refused ones already feed the
  refusal-flood path (`rstOwed` → ENHANCE_YOUR_CALM).
- DATA that delivers no body to a live stream is charged to the existing
  "other non-DATA" bucket (10,000 burst, 1,000/s): zero-length DATA, and DATA
  on a closed stream, which today only replenishes connection credit
  (`session.zig:1605-1618`). Padding is charged to flow control like the
  payload (`session.zig:1579-1582`), so it is not free, but it never counts as
  body progress.
- Tiny non-empty DATA is bounded by the body minimum rate below, not by a
  bucket; a test shows the cost of a 1-byte DATA flood on one executor.
- The bucket sizes are checked against a browser loading a Datastar page.

## Route shares

Each task route has a limit on task handlers (orphans included), on the
request-pool bytes they hold, and on compression contexts, reserved at
dispatch and held until the handler returns. Over it: 503 on that route only.
Default: server limit / number of task routes, overridable per route. Boot
warns when a route's share is below the client share.

## Missing deadlines

- **Request body:** the clock starts at HEADERS without END_STREAM, not at the
  first DATA. Minimum rate after a grace period (Kestrel: 240 B/s after 5 s),
  counting body bytes only, paused only while the server's advertised receive
  credit prevents progress. Same for HTTP/1.1.
- **Response:** the minimum rate is measured only while bytes for the stream
  are queued anywhere between the scheduler and the socket and the client is
  not taking them. Today `checkSlowConsumers` skips a stream with
  `pw.len == 0` (`connection.zig:3530`) even when its bytes are still in the
  write queue; the timer follows the bytes to completion. An SSE stream that
  sends one event a minute is not slow.
- **TLS handshake:** the 5 s deadline stays.

## Running handlers per connection

A handler keeps its slot until it returns; there are
`max_streams_per_connection` slots and a park queue of the same size. Changes:

- As soon as every slot holds a handler whose stream has ended, send GOAWAY
  ENHANCE_YOUR_CALM: parked requests on that connection can never run. One
  counter: slots whose stream ended while the handler runs (up in
  `cancelHandler` and stream close, down in `finishHandlerJob`).
- A parked request whose stream is reset releases its lease when the RST
  arrives, not when a slot frees (`dispatchParked`, `connection.zig:7645-7648`).

## Admission and overload

- Over a share under pressure: refuse that identity. Never park the accept
  loop for everyone because of one identity.
- Pressure-scaled idle timeouts (after Envoy `reduce_timeouts`): one atomic
  pressure level; each actor shortens `connection_idle_timeout_ns` and the
  HTTP/1.1 keep-alive wait while pressure is on. A parked actor does not see
  the atomic change, so while pressure is on `nextDeadlineNs` caps its sleep
  at 1 s and re-reads it. Under pressure this is polling, bounded and only
  then.
- Cut: the "GOAWAY to the top holder's oldest connection" rung. With caps,
  filling the connection table takes four identities at their caps, which is
  the distributed case; the rung would need a per-client connection list and
  a cross-executor signal for that case only.
- Accept errors never end the loop. EMFILE, ENFILE, ENOBUFS, ENOMEM: count,
  log at most once a minute, back off 10 ms doubling to 1 s, retry.
- Boot: read RLIMIT_NOFILE, raise the soft limit toward the hard limit
  (clamped: macOS can report RLIM_INFINITY, and `setrlimit` above
  `kern.maxfilesperproc` fails), and refuse to start below `max_connections`
  plus a margin. Descriptors a handler opens are the application's.

## Stuck handlers

- HTTP/2: the spike's link and gate; the task charge (client, aggregate,
  route, server) held until the handler returns.
- HTTP/1.1 moves to the same model in its own commit: link, gate, request
  arena from the request pool (t-3537). The reaper and its
  `JoinHandle.cancel` wait are deleted. H1 request data lives in buffers the
  connection owns today (`h1.zig:239-262`), so the reaper cannot go before
  the link and pool move.
- Non-yielding handlers are detected, not contained. The watchdog runs on an
  OS thread, not a timer on the executor it watches. zio's monitor thread
  starts only with metrics on and a positive interval (zio
  `runtime.zig:1774-1782`), so the zio-fork task makes a watchdog thread of
  its own, enabled by its own option. It distinguishes a parked executor from
  one stuck in a task, and publishes the current task's identity in a way that
  is safe to read from another thread. Tests run with zio's `debug_cancel`
  (reports a swallowed `error.Canceled`); `zio.blockInPlace` is the documented
  way to call blocking code.

## Shutdown (Q5)

`serve` never returns while a handler still runs.

1. GOAWAY, then wait for the drain deadline.
2. Request cancel of every handler, and log right then what is still running
   (count per route, how long the drain ran), because a supervisor's kill can
   come before the next step (Kubernetes' default grace is 30 s, the same as
   the drain). The log names the drain deadline beside it.
3. Wait a short grace (1 s, as Kestrel). If handlers remain, log again, then
   keep waiting and log at an interval. The wait ignores cancel and uses zio's
   `debug_cancel` exemption.
4. Every wait in `serve` is covered, including `connection_group.await`
   (`server.zig:392-399`), where HTTP/1.1 connections wait until the H1
   commit lands.

With one executor and a non-yielding handler, the logging task never runs;
only the watchdog thread can report. Said beside `serve`.

Every `deinit` check becomes a run-time check that logs.

## Observability

Counters per resource and per refusal reason; orphans per route and how long
the oldest has run (the link records its start time). Top holders are read on
demand by scanning the fixed record slab. One global rate-limited log line for
refusals, naming identity, share of use and endpoint mode; one for orphans.

## What stays out

- Distributed attacks beyond the caps.
- Preempting a non-yielding task (premise 2).
- Request-rate limiting per client: the caps bound what an identity holds, the
  buckets bound connection starts and empty DATA; how often it may call a
  route is the application's.
- Task stacks: not a share. `resourceUpperBound` counts committed stack x the
  task-handler limit, and names the embedder's `stack_pool` setting.

## Tests

- One attack test per inventory row on real sockets: h2c and TLS where both
  apply, HTTP/1.1 where it applies. Each fails on `839dd69` (or master),
  passes after, and shows a second identity admitted up to a full share during
  the attack. Each prints its refusal counters, so an attack that never
  engaged is visible.
- Identities: 127.0.0.1 against ::1 for the plain path; PROXY v2 headers
  elsewhere. No test hook for identity.
- Rapid Reset and MadeYouReset shapes; empty, closed-stream and 1-byte DATA
  floods; the connection-start and handshake rates; the NAT case at idle and
  at 30% use; a serial-reconnect test that the rate bucket does not refill on
  reconnect.
- **Charge conservation**, the biggest risk: after every test, the record slab
  holds no charged record and every account is zero (run-time checks). A soak
  drives every refusal rung with random RST, close and GOAWAY timing over h2c,
  TLS and HTTP/1.1, against routes that honour and ignore cancel; it asserts
  zero at the end and peak bounds during, and covers same-key delete and
  reinsert. Mutation: delete each release and each undo line in turn; the
  suite fails for each.
- Because `serve` waits for handlers, a leaked charge becomes a hang: every
  test runs `serve` under an outer per-test deadline that fails loudly with the
  stuck count. The "keeps waiting" test runs in a child process.
- Bench against master, `failed == 0` read first, no regression beyond noise.
  Mac and Linux.

## Order of work

One branch, a series of commits, each green with its own attack tests, so the
series bisects; pushed together when the last is done.

1. Missing deadlines (body at HEADERS, minimum rates in and out), accept
   robustness, the RLIMIT_NOFILE check. No accounts needed.
2. The shutdown contract, the per-test `serve` deadline, run-time `deinit`
   checks.
3. Account records, the client table, client identity, PROXY protocol v2.
4. Shares under pressure; the connection-start and handshake buckets.
5. Route shares; the spike folded in with its charges.
6. Empty and closed-stream DATA in the frame bucket; the running-handler
   GOAWAY; parked leases released on RST.
7. HTTP/1.1 onto links and the request pool; reaper deleted.
8. Pressure-scaled idle timeouts.
9. Observability; the executor watchdog (after the zio-fork task).

## Decisions (Ryan, 2026-11-02)

- Q1. Identity: deferred by Ryan to the design. Settled by the panel's second
  round as above: `.peer` default on TCP (IPv4 per address, IPv6 /64 plus a
  /48 aggregate), `.none` default on Unix sockets and behind pooling L7
  proxies, `.proxy_protocol` v2 only with trusted sources. The IPv6 aggregate
  width is the one point the panel split on (Opus /48, Fable /56 with a fixed
  share, astra opt-in only); v3 takes /48, configurable.
- Q2. Agreed: shares apply only under pressure. Client share 1/8, cap 1/4;
  route share = server / number of task routes.
- Q3. Agreed: HTTP/1.1 moves to the link model in this series, its own commit.
- Q4. Agreed: non-yielding handlers are detected and logged by a zio watchdog
  thread (zio-fork task first).
- Q5. Decided: `serve` never returns while a handler still runs; it logs the
  stuck handlers and keeps waiting. No heap core, no `HandlersStuck` error.
- Q6. Agreed: a bisectable series, pushed together.
