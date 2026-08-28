# Brief: the HTTP/1.1 channel

Build an HTTP/1.1 edge channel for starh2. The channel serves proxies, probes,
webhook senders, and standard-library clients. Browsers stay on h2 over TLS.

This brief is spec-hardened. Every property has a mechanical check. The grader
re-runs every check itself. A claim in your report without a named test behind
it does not count.

## 1. Premises — settled, do not re-argue

- This stack is malleable and has no external consumers. Propose and land the
  breaking change. Do not add a compatible shim beside the better design.
- Prefer share-nothing. A new mutex is a design smell. An h1 connection has one
  stream, so it needs no `FairScheduler` and no `Session`.
- Do not fight `std.Io` or zio. Use a buffered writer with an explicit `flush`,
  `std.Io` ownership and cancellation, and zio tasks.
- The parser style is settled: pico-style. One bounded contiguous head buffer.
  One pass over the head after the terminator arrives. A small counter for the
  body. No byte-callback state machine. No generated parser.
- The cut list in §4 is settled. Do not implement a cut feature. Do not weaken
  a rejection into acceptance.

## 2. Read these files before you write code

The rule is in `AGENTS.md`: never guess a contract; read the body and its
callers. Read, in this order:

1. `src/core/frame.zig` — the house parser pattern: `initReserved` scratch,
   borrow-on-complete, resumable fill offsets, peer-fault-not-allocation.
2. `src/http/request.zig`, `src/http/response.zig`, `src/http/router.zig` —
   the handler surface the channel must feed: `Request`, `Response.send`,
   `Response.start`, `Response.startSse`, `Body.flush/finish/abort`,
   `Handler` union, `SlotTerminal`.
3. `src/core/fields.zig` — `validateRequestFields`. The h1 channel calls this
   same function. It does not reimplement header validation.
4. `src/core/limits.zig` — the declared-term convention and
   `resourceUpperBound`.
5. `src/edge/server.zig` — `EndpointConfig`, accept loop, shutdown, and
   `waitUntilListening`.
6. `src/edge/tls.zig` — the ALPN callback (`selectH2Only`, near line 205),
   `TlsPump` ownership, and the BIO pair.
7. `src/edge/connection.zig` — dispatch, slot release, and the reaper
   contract.
8. `tests/backend_parity.zig`, `tests/transport.zig`, `tests/deadlines.zig`,
   `tests/limits.zig` — the test idioms this brief's battery extends.

## 3. Scope

- **Parser** (`src/http1/parser.zig` or similar): head accumulation into a
  boot-reserved buffer of `Limits.h1_head_bytes`, one-pass parse, and a
  Content-Length body counter. Resumable across arbitrary chunk boundaries.
- **Channel** (`src/edge/h1.zig` or similar): connection loop, keep-alive,
  response framing (Content-Length, chunked, close-delimited), dispatch into
  the existing `Router`/`Response` surface.
- **Endpoints**: replace `EndpointConfig.tls_h2` with `tls`. The ALPN callback
  prefers `h2`, falls back to `http/1.1`, and selects `http/1.1` when the
  client sends no ALPN. Add `h1c` for cleartext HTTP/1.1 (the nginx-upstream
  and probe shape). `h2c_prior_knowledge` is unchanged. This breaks every
  `EndpointConfig` construction site, `tools/bench.zig`, and the smoke
  scripts. Update them in the same change.
- **Limits**: new declared terms `h1_head_bytes` (default 16 KiB) and any
  other h1 buffer, included in `resourceUpperBound`. Body accounting reuses
  `request_bytes_per_connection`.
- **Gate**: `tools/h1-smoke.sh` plus a `./zb build h1-smoke` step, added to
  `ci` (§10).

Out of scope: chunked request decoding, trailers, pipelined concurrency,
`Upgrade`, `CONNECT`, ranges, preface sniffing on one cleartext port, HTTP/1
client code.

## 4. The cut table — each row is a behavior with a test

Every row names the exact response and the connection outcome. "close" means
the server sends `Connection: close` when a response exists and closes after
it. The named test is the acceptance check for that row.

| Input | Response | Connection | Test |
|---|---|---|---|
| `Transfer-Encoding: chunked` request | `411` | close | `h1.reject.chunked_request` |
| `Transfer-Encoding` other than chunked | `501` | close | `h1.reject.te_gzip` |
| `Transfer-Encoding` + `Content-Length` | `400` | close | `h1.reject.te_cl_conflict` |
| Duplicate `Content-Length`, unequal | `400` | close | `h1.reject.dup_cl` |
| Non-numeric / signed / hex `Content-Length` | `400` | close | `h1.reject.cl_nonnumeric` |
| obs-fold (leading SP/HTAB on a header line) | `400` | close | `h1.reject.obs_fold` |
| Bare LF as a line terminator | `400` | close | `h1.reject.bare_lf` |
| Whitespace between header name and colon | `400` | close | `h1.reject.ws_before_colon` |
| Control byte in a header value | `400` | close | `h1.reject.ctl_in_value` |
| SP inside the request-target | `400` | close | `h1.reject.space_in_target` |
| HTTP/1.1 request without `Host` | `400` | close | `h1.reject.no_host_11` |
| Two `Host` headers | `400` | close | `h1.reject.dup_host` |
| `CONNECT` | `501` | close | `h1.reject.connect` |
| HTTP/0.9 (no version token) | none | close | `h1.reject.http09` |
| Version major != 1 (e.g. `HTTP/2.0` line) | `505` | close | `h1.reject.bad_version` |
| `Expect` other than `100-continue` | `417` | close | `h1.reject.expect_other` |
| Head exceeds `h1_head_bytes` | `431` | close | `h1.limits.head_over_bound` |
| Body exceeds request accounting | `413` | close | `h1.limits.body_over_bound` |
| `Upgrade: h2c` / `Upgrade: websocket` | ignore header, answer as h1 | keep | `h1.accept.upgrade_ignored` |
| `Range` | ignore header, `200` | keep | `h1.accept.range_ignored` |

Rationale for the closes: after a framing-ambiguous request, the byte stream
cannot be re-synchronized, so keep-alive after any 4xx framing rejection is a
smuggling primitive. Close on all of them.

## 5. Mechanical invariants

- **I1 — byte conservation.** For every accepted request,
  `bytes_consumed == head_len + content_length`. The next request begins at
  exactly that offset. No skip, no re-read, no peek beyond.
- **I2 — one framing per response.** Every response carries exactly one of:
  `Content-Length`, `Transfer-Encoding: chunked`, or close-delimiting with
  `Connection: close`. Never two. Asserted on captured wire bytes, not on
  server state.
- **I3 — parity with h2.** For every request in the parity table, dispatch
  the same request over h2 wire bytes and over h1 wire bytes. The
  handler-observed `Request` (method, path, query, authority, header set
  after `validateRequestFields`) is identical. This is the drift guard for
  the two-channels-one-contract risk.
- **I4 — bounded memory.** Steady-state h1 connection memory stays within the
  declared terms. A request whose head fits `h1_head_bytes` performs zero GPA
  allocations after connection setup, proven with a counting allocator.
- **I5 — oversize is a peer fault, not an allocation.** A head or body over
  its limit produces the table's status. It never grows a buffer.
- **I6 — flush reaches the wire.** After `Body.flush` returns on an SSE
  response, a raw client can read the event bytes without the response
  finishing. Buffer-until-finish is a failure.
- **I7 — shutdown holds.** SIGTERM with an active h1 SSE stream stops the
  server within the deadline. `shutdownHandlers` waits for slot release, not
  for `live_handlers == 0`.
- **I8 — scope is observable.** The battery runner prints the fixture count
  per category and exits non-zero when any category matches zero fixtures.

## 6. Paired axes — the laziest passing outputs, named

Each hardened axis creates a degenerate optimum. Each pairing below closes
one. All axes gate at once; no trade-offs between them.

1. **Reject-everything** passes every `h1.reject.*` case. Paired axis: the
   full `h1.accept.*` battery must pass on the same build.
2. **Close-after-every-response** makes I1 trivially true. Paired axis:
   `h1.keepalive.*` requires sequential requests on one connection.
3. **Buffer-the-whole-connection** passes all correctness cases. Paired axis:
   I4 zero-alloc and `h1.limits.*` bound the memory.
4. **Buffer-SSE-until-finish** passes wire-capture framing checks. Paired
   axis: I6 `h1.sse.flush_latency` with a raw reader.
5. **A private header validator** passes every h1 test while drifting from
   h2. Paired axis: I3 parity, plus a source check that the h1 path calls
   `fields.validateRequestFields`.
6. **A 100-continue that never reads the body** passes the status check.
   Paired axis: `h1.accept.expect_100` asserts the echoed body content.

## 7. Fixed I/O contract for the battery

One runner drives every implementation identically.

- Byte fixtures live in `testdata/h1/<category>/<name>.txn`. A fixture holds:
  the raw request bytes (with explicit `\r\n` escapes), the expected status,
  the expected connection outcome (`keep` | `close`), and optionally expected
  response-body bytes.
- The runner (`tests/h1_battery.zig`) connects over real TCP to a served
  endpoint, writes the fixture bytes with a controllable segmentation
  schedule, and asserts on the captured response bytes.
- Segmentation schedules: `whole`, `byte-at-a-time`, and `split-at(i)`. Every
  fixture runs under `whole` and `byte-at-a-time` at minimum.
- The runner prints `category=<name> fixtures=<n>` per category and fails on
  any zero (I8).

The escape-handling trap from the machine notes applies: write fixture files
with a script and verify the bytes with `python3 -c "print(repr(...))"`.
A fixture that fails is a finding. Do not edit the fixture to make it pass.
Fix the code, or report the disagreement with the RFC citation.

## 8. The test battery, by category

### h1.accept — must accept (pairs with the reject axis)

- `h1.accept.get_simple` — GET, no body, Content-Length response.
- `h1.accept.post_cl` — POST with a Content-Length body; handler sees the
  exact bytes.
- `h1.accept.get_with_body` — GET with a Content-Length body is legal;
  consumed per I1.
- `h1.accept.head_no_body` — HEAD: correct `Content-Length`, zero body bytes
  on the wire.
- `h1.accept.http10_probe` — `OPTIONS / HTTP/1.0` (the HAProxy check shape):
  answered, close-delimited, connection closes.
- `h1.accept.expect_100` — `Expect: 100-continue`: interim `100` first, then
  the final response; body content echoed.
- `h1.accept.absolute_form` — `GET http://host/path HTTP/1.1` reduces to
  path + authority (RFC 9112 MUST).
- `h1.accept.leading_crlf` — one blank line before the request line is
  ignored.
- `h1.accept.upgrade_ignored`, `h1.accept.range_ignored` — per the cut table.
- `h1.accept.query_preserved` — path with query; parity with h2 asserted
  in I3.
- `h1.accept.headers_at_bound` — head one byte under `h1_head_bytes`.

### h1.reject — must reject (exact status, exact close; see §4 table)

All twenty rows of the cut table, plus:

- `h1.reject.cl_values` — a parameterized sweep: `+5`, `-5`, `0x5`, `5 5`,
  `5,5`, `18446744073709551617` (u64 overflow), empty value.
- `h1.reject.smuggle_te_casing` — `Transfer-Encoding: chunKed` and
  ` chunked` (leading SP) still hit the 411/400 rows, never a pass-through.

### h1.frame — byte conservation (I1)

- `h1.frame.two_in_one_segment` — two complete requests in one TCP write;
  two correct responses in order.
- `h1.frame.trickle` — one request delivered one byte per write, head and
  body.
- `h1.frame.split_sweep` — a canonical POST split at every byte offset `i`;
  identical result for all `i`. This is the pico re-parse correctness sweep.
- `h1.frame.body_exact_boundary` — body ends exactly at a segment boundary;
  a second request follows and is answered.
- `h1.frame.body_larger_than_recv_buffer` — body larger than the channel's
  read chunk; asserts no head-slice reuse corruption.
- `h1.frame.early_second_request` — the second request's bytes arrive while
  the first response is still streaming; they are neither lost nor executed
  concurrently.

### h1.keepalive — pairs with the close-everything degenerate

- `h1.keepalive.sequential_100` — 100 requests on one connection; all
  answered; zero GPA allocations after the first (I4).
- `h1.keepalive.close_honored` — `Connection: close` on a request closes
  after the response.
- `h1.keepalive.close_after_reject` — any §4 close row actually closes
  (asserted by reading to EOF).
- `h1.keepalive.http10_forced_close` — HTTP/1.0 with
  `Connection: keep-alive` still closes (documented cut).
- `h1.keepalive.idle_reaped` — an idle connection is reaped by the deadline
  machinery and does not block shutdown.

### h1.resp — response framing (I2)

- `h1.resp.cl_xor_chunked` — wire capture over the whole battery asserts I2
  on every response.
- `h1.resp.status_line_exact` — the status line is byte-exact
  (`HTTP/1.1 200 OK\r\n` shape).
- `h1.resp.chunked_terminator` — a streamed response ends with `0\r\n\r\n`
  and keep-alive continues after it.
- `h1.resp.sse_http10` — SSE to an HTTP/1.0 client is close-delimited with
  `Connection: close`, never chunked.

### h1.sse — the Datastar path

- `h1.sse.flush_latency` — I6: a flushed event is readable before finish.
- `h1.sse.chunk_per_flush` — each flush produces at least one complete chunk
  on the wire.
- `h1.sse.occupies_connection` — an h1 connection carrying SSE serves no
  further requests; later bytes do not corrupt the stream.
- `h1.sse.client_abort` — the client closes mid-stream; the handler observes
  the terminal cause; the slot releases exactly once.

### h1.limits — bounds (I4, I5)

- `h1.limits.head_over_bound` — `431`, close, no buffer growth.
- `h1.limits.body_over_bound` — `413`, close.
- `h1.limits.zero_alloc_steady` — counting-allocator proof for I4.
- `h1.limits.resource_upper_bound` — the new terms appear in
  `resourceUpperBound` (extends `tests/limits.zig`).
- `h1.limits.slow_loris` — header bytes at one byte per second hit the read
  deadline; the connection is reaped.

### h1.parity — the drift guard (I3)

- `h1.parity.request_table` — each table request over h2 and h1; identical
  handler-observed `Request` (extends `tests/backend_parity.zig`).
- `h1.parity.validation_shared` — a header that h2 rejects via
  `validateRequestFields` is rejected on h1 with the mapped status.

### h1.alpn — channel selection

- `h1.alpn.both_offered` — a client offering `h2` and `http/1.1` gets h2.
- `h1.alpn.h1_only` — a client offering only `http/1.1` gets the h1 channel
  over TLS.
- `h1.alpn.none_offered` — no ALPN extension selects h1.
- `h1.alpn.h2c_unchanged` — the `h2c_prior_knowledge` endpoint still rejects
  an h1 request line.

### h1.shape — production-shape axes (each fixture sits PAST a boundary)

- `h1.shape.halfclose` — the client shuts its write side after the request;
  the full response is still delivered.
- `h1.shape.abort_mid_upload` — the client closes during a body upload; the
  handler errors; the slot releases once.
- `h1.shape.slow_handler` — the handler waits 2 s before responding; the
  client (and keep-alive) survive it.
- `h1.shape.tls_small_records` — h1 over TLS with the request split across
  many TLS records through `TlsPump`.

## 9. The mutation set — prove the battery bites

Run each mutation. Cite the specific failing test per mutation in the report.
A mutation that keeps the battery green is a hole in the battery; add the
fixture before you proceed.

- **M1** — consume one byte too few after the head terminator → must fail
  `h1.frame.two_in_one_segment`.
- **M2** — never emit the terminal chunk → must fail
  `h1.resp.chunked_terminator` and hang detection in `h1-smoke`.
- **M3** — ignore `Connection: close` → must fail
  `h1.keepalive.close_honored`.
- **M4** — keep the connection open after a 400 → must fail
  `h1.keepalive.close_after_reject`.
- **M5** — validate headers with a local copy instead of
  `validateRequestFields` and drop one check → must fail
  `h1.parity.validation_shared`.
- **M6** — buffer SSE until finish → must fail `h1.sse.flush_latency`.
- **M7** — grow the head buffer on overflow instead of 431 → must fail
  `h1.limits.head_over_bound`.
- **M8** — parse `Content-Length` with a lenient integer parse → must fail
  `h1.reject.cl_values`.

## 10. Gates and the smoke script

- Unit and battery: under `zig build test` via `tests/h1_battery.zig`.
- `tools/h1-smoke.sh` + `./zb build h1-smoke`, added to `ci`. The oracle is
  curl, because it shares no code with this stack:
  - `curl --http1.1` against the `tls` endpoint (forces the ALPN fallback).
  - `curl` against the `h1c` endpoint.
  - Two URLs in one invocation to prove keep-alive reuse.
  - `curl -N` on an SSE route; assert an event arrives within 2 s.
  - SIGTERM with the SSE stream open; assert the server process exits (the
    `tls-smoke` lesson: a stranded handler with perfect wire output).
  - The script ABORTS when curl is missing. A skipped gate reads as a pass.
- The suite cannot reach the TLS edge (`AGENTS.md`), so `h1-smoke` is the
  only coverage for ALPN fallback over real TLS. Do not fake it in-process.

## 11. Grading protocol

- The grader re-runs `zig build test`, the battery, the mutation set, and
  `h1-smoke` itself. It reads no number from your report that it did not
  recompute.
- Assertions run on what the consumer receives: captured wire bytes at the
  client socket, never server logs or internal counters.
- Cross-parser oracles beyond curl: a short Go `net/http` client script for
  keep-alive and 100-continue semantics, and the raw-socket runner for
  everything malformed. Go's parser shares no priors with this code.
- Held-out set: the grader keeps a private variant of every `h1.reject.*`
  fixture (same category, different bytes). The published battery is the
  floor, not the exam.
- Optional n≥3: fan the brief to cross-vendor builders via `cursor-agent`
  with `--worktree`, one grader over all diffs. Three identical failures
  mean this brief is wrong; report that instead of patching around it.

## 12. Evidence requirements for the completion report

- For every §4 row and every §5 invariant: the exact test name(s) and the
  command that runs them. "Covered by the battery" without a name is not
  evidence.
- For every mutation M1–M8: the failing test name and its failure output.
- The battery category counts (I8 output) pasted verbatim.
- Anything cut, deferred, or discovered gets filed with
  `~/.claude/scripts/taskprobe/task` at the moment it appears; list the
  receipts.
- A dependency API you did not read is an unfinished brief (`AGENTS.md`).
  Name the dependency files you read.

## 13. Review rounds

Budget two review rounds after green. Round 1 attacks the build; every
finding becomes a permanent fixture. Round 2 attacks round 1's fixes first;
fixes are the densest source of new defects. Green means no known path
fails. It does not mean correct.

## 14. Rejected alternatives, recorded so they can be overridden

- **llhttp-style callback state machine** — inverts control against the
  pump-feeds-chunks grain; a generated core violates the read-the-source
  rule.
- **Chunked request decoding now** — `411` is the sanctioned fallback
  (RFC 9112 §6.3); a ~60-line reversible addition if a real client hits it.
- **Preface sniffing on one cleartext port** — a second dispatch mechanism
  on the accept path for no named consumer; the `h1c` endpoint variant is
  explicit instead.
- **Keeping `tls_h2` beside `tls`** — a compatible shim beside the better
  design; the premises forbid it.
- **HTTP/1.0 keep-alive** — one consumer shape (ancient clients) against a
  real resync risk; forced close is one comparison.
