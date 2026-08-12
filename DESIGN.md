# starh2 design specification

Status: design of record for t-420  
Date: 2026-08-11  
Implementation status: not started

## 1. Decision

The standalone repository and Zig package are named **`starh2`**.

`starh2` is a server-side HTTP/2 stack shaped around Datastar:

- long-lived SSE streams;
- latency-sensitive one-shot Datastar morph, script, and signal responses;
- bounded parsing of Datastar signals from query strings and request bodies.

It is not a generic HTTP framework. The protocol implementation is pure Zig.
The edge uses zio for evented I/O and `tls.zig` for TLS 1.3 and ALPN. It does
not use Tina, dusty, nghttp2, OpenSSL, BoringSSL, or another HTTP server at
runtime.

RFC 9113 and RFC 7541 are normative. This document narrows optional behavior
but does not redefine legal HTTP/2 behavior.

## 2. Fixed toolchain and dependencies

The initial repository pins these exact revisions:

| Dependency | Revision | Reason |
|---|---|---|
| Zig | `0.16.0` | Product toolchain |
| `lalinsky/zio` | tag `v0.16.0`, commit `fd16258e8fadec5aa74f13302efecd070593cb06e` | Evented/coroutine runtime and socket I/O |
| `ianic/tls.zig` | base commit `5452bafc98d23e304209cb24d81fd2d19434e52d` plus the frozen `starh2-nonblock-v1` patchset below | Last verified Zig 0.16 revision with server ALPN |
| `zigster64/datastar-sdk.zig` | commit `0c1f91f3d82ecd1dad3c53b9450db12623fc3408` | Datastar wire formatting |

Current `tls.zig` main is not an allowed substitute: it requires Zig 0.17-dev.
The base revision's synchronous `Connection` is not safe to share between
concurrent reader and writer tasks. Its nonblocking error paths are also not
usable unchanged: ALPN failure can omit the required alert, malformed-record
handling can attempt to write through an unset Writer, and TLS 1.3 key updates
do not isolate directional key material.

The repository therefore carries a source-visible, Zig-0.16-compatible
`starh2-nonblock-v1` patchset over that exact base. The patchset is limited to:

1. making nonblocking handshake and decrypt calls return a tagged outcome that
   contains produced alert ciphertext together with the terminal TLS error;
2. emitting TLS alert 120 (`no_application_protocol`) when ALPN has no match;
3. eliminating every write through an unset Writer in nonblocking operation;
4. updating only decrypt keys on a received KeyUpdate and only encrypt keys
   when sending its response;
5. adding tests for fragmented handshakes, ALPN failure, malformed records,
   both KeyUpdate directions, and close-notify.

The patch exports this exact ABI:

```zig
pub const starh2_nonblock_abi: u32 = 1;

pub const DriveStatus = union(enum) {
    need_input,
    complete,
    peer_closed,
    tls_error: anyerror,
};

pub const HandshakeDriveResult = struct {
    consumed: usize,
    ciphertext_len: usize,
    status: DriveStatus,
};

pub const DecryptDriveResult = struct {
    consumed: usize,
    plaintext_len: usize,
    ciphertext_len: usize,
    status: DriveStatus,
};

pub const EncryptDriveResult = struct {
    consumed: usize,
    ciphertext_len: usize,
    status: DriveStatus,
};

pub fn serverDrive(
    server: *nonblock.Server,
    input: []const u8,
    ciphertext_out: []u8,
) HandshakeDriveResult;

pub fn serverAlpnProtocol(server: *const nonblock.Server) ?[]const u8;
pub fn serverTakeCipher(server: *nonblock.Server) error{HandshakeIncomplete}!Cipher;

pub fn connectionDecrypt(
    connection: *nonblock.Connection,
    input: []const u8,
    plaintext_out: []u8,
    ciphertext_out: []u8,
) DecryptDriveResult;

pub fn connectionEncrypt(
    connection: *nonblock.Connection,
    plaintext: []const u8,
    ciphertext_out: []u8,
) EncryptDriveResult;

pub fn connectionClose(
    connection: *nonblock.Connection,
    ciphertext_out: []u8,
) EncryptDriveResult;
```

All buffers are caller-owned. `consumed` refers to the supplied input
plaintext/ciphertext as appropriate. Even when `status` is `.tls_error`, any
required alert is present in `ciphertext_len` bytes and must be flushed before
close. No patched nonblocking function owns or invokes an `Io.Writer`.

The build consumes a fork commit or vendored package whose tree is exactly the
base plus that reviewed patch; the patch and upstream LICENSE remain visible
in `vendor/tls-zig/`. A compile-time patch ABI marker prevents accidentally
building against unpatched upstream. This is a minimal correctness backport,
not a new TLS implementation. Replacing it with a later upstream commit is a
separate reviewed change and must retain Zig 0.16 compatibility and the same
tests. The implementation TLS gate is not complete until the resulting fork
commit and SHA-256 of `tls-zig-nonblock-v1.patch` are recorded in
`build.zig.zon` and the repository's tool lock; deriving those identifiers
from the implemented patch is mechanical and does not reopen this ABI.

Every Zig API must be checked against the installed 0.16 standard library with
`zigstd`; remembered APIs from older Zig versions are not evidence. The
repository includes the `zb` shim used by Tina so agents and CI invoke
`./zb build ...`.

## 3. Repository and module layout

```text
starh2/
  AGENTS.md
  DESIGN.md                 # this document after repository creation
  build.zig
  build.zig.zon
  zb
  vendor/
    tls-zig/                 # pinned upstream base + nonblock-v1 patch
    tls-zig-nonblock-v1.patch
  src/
    root.zig                # stable public exports
    core/
      frame.zig             # frame header/payload parsing and serialization
      hpack.zig             # HPACK decoder/encoder
      fields.zig            # HTTP field and pseudo-field validation
      stream.zig            # RFC stream states and request assembly
      flow.zig              # connection/stream flow-control arithmetic
      session.zig           # deterministic connection state machine
      limits.zig            # limits, defaults, memory upper-bound calculation
    edge/
      server.zig            # listen, accept, global limits, graceful stop
      connection.zig        # connection actor and stream supervision
      wire_pump.zig         # sole raw socket reader and writer tasks
      tls.zig               # nonblocking tls.zig adapter and ALPN check
      h2c.zig               # prior-knowledge cleartext startup
    http/
      request.zig
      response.zig
      router.zig
    datastar.zig            # SDK re-exports and bounded signal helpers
  examples/
    hello.zig
    datastar_sse.zig
    conformance_server.zig
  fuzz/
    frame.zig
    hpack.zig
    session.zig
    corpus/
  tests/
    protocol.zig
    limits.zig
    transport.zig
    interop.zig
    multiplex.zig
  tools/
    h2spec/
    held-out/               # grader interface/docs, not secret fixture bytes
```

`src/core` imports only Zig standard-library facilities needed for data
structures and deterministic parsing. It does not import zio, TLS, sockets,
clocks, or the Datastar SDK. `src/edge` is the only zio/TLS boundary.
`src/http` and `src/datastar.zig` form the application-facing layer.

## 4. Architecture

### 4.1 Deterministic protocol core

`core.Session` is an actor-owned state machine. It accepts:

- arbitrary chunks of plaintext HTTP/2 bytes;
- application commands for a particular stream;
- handler-completion and cancellation events;
- explicit timer and shutdown events.

It produces:

- ordered outbound HTTP/2 frame intents;
- request-dispatch intents;
- stream cancellation/completion intents;
- a typed terminal connection outcome.

The state machine never performs I/O and never waits. A frame header, payload,
HPACK block, or application event presented in different chunk boundaries must
produce the same intents and final state.

The parser consumes incrementally. It first copies and validates the nine-byte
frame header, then consumes the payload in bounded pieces. It must not call a
Reader operation that assumes a peer-controlled payload fits in the Reader's
buffer. No core result retains a slice into an input chunk.

### 4.2 Connection actor

One connection actor exclusively owns:

- the `tls.nonblock.Server` during the handshake;
- the `tls.nonblock.Connection` after the handshake;
- both HPACK contexts;
- the frame parser and serializer;
- all stream and flow-control state;
- request assembly;
- the fair-write scheduler;
- graceful shutdown and the final socket-close decision.

TLS and HTTP/2 state are never concurrently accessed. The actor may migrate
between zio executors because ownership, rather than thread affinity, provides
serialization.

For both TLS and cleartext, two subordinate tasks own the raw socket
directions:

1. The read pump is the sole owner of the concrete zio stream Reader. It reads
   into bounded wire chunks and sends chunk handles to the connection actor.
2. The write pump is the sole owner of the concrete zio stream Writer. It
   receives ordered wire chunks from the actor, writes them, and flushes at
   actor-specified barriers.

The actor selects among inbound wire chunks, handler commands, writer
capacity/completions, stream completions, timers, and shutdown. It never waits
only for outbound capacity while inbound SETTINGS, PING, WINDOW_UPDATE, or
RST_STREAM frames could be pending.

Handlers never receive a connection Writer and never serialize a frame.
Response storage is reserved and copied before a command is enqueued, so no
command contains a handler borrow. A successful channel send transfers the
leased storage handle to the actor; a failed or canceled send leaves ownership
with the handler, which releases it. A separate acknowledgment carries only an
integer ticket.

### 4.3 TLS ownership

The TLS edge must not call `tls.serverFromStream`, return self-referential
Reader/Writer wrappers, or construct a `tls.Connection` shared by split pumps.

The actor feeds ciphertext chunks into the patched nonblocking server, queues
all returned handshake or alert bytes to the write pump, and retains
unconsumed ciphertext in a bounded record buffer. After the handshake:

1. `alpnProtocol()` must equal exactly `"h2"`;
2. the resulting cipher is moved into the patched nonblocking connection;
3. only the actor drives decrypt/encrypt outcomes and close-notify.

The ALPN protocol list and strings are static or server-owned for the entire
server lifetime. TLS options advertise only `"h2"`. TLS early data is disabled.
A client that does not offer `h2` receives a TLS `no_application_protocol`
failure and no HTTP response.

### 4.4 Task supervision and teardown

The accepted-connection task is the supervisor. It owns the actor state, raw
pump handles, and one bounded `zio.JoinHandle` slot for every live handler.
Each handler has a nonblocking cancellation token. Completion notifications
use a dedicated channel with one reserved slot per handler slot and never wait.

Before accepting connections, `Server.serve` starts a server-owned pool of 64
cancellation reaper tasks and a 4,096-job bounded queue. Shutdown closes that
queue and joins every reaper after all connections have relinquished their
handler slots. Reapers are part of the published task and stack budget; reset
storms cannot spawn unbounded tasks.

Each handler slot contains a generation and atomic completion owner:
`.live`, `.reaper_owned`, or `.reported`. Natural return CASes
`.live -> .reported` and posts one completion. The actor CASes
`.live -> .reaper_owned` before transferring the JoinHandle to the reaper
queue; if it observes `.reported`, no reaper job is needed. A reaper calls
`cancel()`, then changes `.reaper_owned -> .reported` and posts the one
completion. Thus natural and canceled completion cannot both report a slot.

At zio v0.16.0, `JoinHandle.cancel()` waits for task completion. The actor must
not call it because a handler can require actor progress while unwinding. For a
peer reset or shutdown, the actor first sets the stream cancellation token and
stops accepting its commands. It transfers the JoinHandle to a cancellation
reaper task; that task calls `cancel()` while the actor continues serving
queues, then posts the already-joined result to the reserved completion
channel. No later `join()` is required.

Connection failure or server shutdown applies this process to all handlers and
pumps, drains every queued owned handle or sweeps its pool after producers
have joined, then the supervisor closes the accepted socket exactly once. No
subordinate task closes the socket or cancels the whole connection group.

Handlers must propagate `error.Canceled`; a wait, sleep, queue operation, or
body write is a cancellation point. CPU-only handler loops must call
`zio.maybeYield`.

## 5. Memory and ownership contract

The edge owns every accepted stream, wire buffer, channel, TLS value, and
protocol connection. The protocol core borrows input only for the duration of
`ingest` and owns or copies anything retained.

Slices returned by Reader parsing methods are ephemeral. They may not enter an
HPACK table, request, queue, handler, or stream state without being copied into
bounded connection-owned or request-owned storage.

Request fields, path, body, and trailers live in a per-stream arena and remain
valid only until the handler returns and is joined. Datastar signal values
parsed into that arena have the same lifetime.

Response calls either:

- copy all retained arguments into bounded owned storage before returning; or
- remain suspended, cancellably, until the bytes have been copied or sent.

They never retain an undocumented caller borrow.

Default allocation limits are:

| Resource | Default |
|---|---:|
| Endpoints per server | 8 |
| Routes per server | 256 |
| Aggregate copied route-path bytes | 64 KiB |
| Active connections per server | 1,024 |
| Active streams per connection | 256 |
| Active streams per server | 4,096 |
| Inbound wire chunks per connection | 8 × `WIRE_CHUNK_SIZE` |
| Outbound bytes per stream | 64 KiB |
| Outbound bytes per connection | 4 MiB |
| Outbound bytes per server | 256 MiB |
| Request storage per connection | 8 MiB |
| Request storage per server | 256 MiB |
| Reserved control-frame storage | 64 KiB and 256 entries |
| Recent reset/closed-stream tombstones | 1,024 |
| Concurrent TLS handshakes per server | 128 |
| TLS certificate chain | 64 KiB encoded |
| TLS private key | 16 KiB encoded |
| TLS handshake-flight scratch | 256 KiB per active handshake |
| Cancellation reaper tasks | 64 per server |
| Cancellation reaper jobs | 4,096 per server |

The payload pools are allocated on demand but cannot grow past their limits.
Capacity is measured in bytes and entries. Control-frame capacity is reserved
and cannot be consumed by DATA.

`WIRE_CHUNK_SIZE` is computed at compile time as the maximum of the patched
TLS input-buffer requirement, TLS maximum ciphertext record, and one
16,384-byte HTTP/2 frame plus its nine-byte header. The TLS receive accumulator
holds at least one complete maximum ciphertext record. TLS plaintext encrypt
scratch is 16 KiB; ciphertext scratch uses the patched TLS maximum. Returned
TLS slices are copied into or produced directly inside leased wire chunks
before scratch reuse. Certificate/key loading computes a handshake-flight
upper bound and rejects configuration that cannot fit the published 256 KiB
scratch.

`Limits.resourceUpperBound()` reports starh2's maximum **incremental** demand
on its borrowed runtime in three independent ceilings:

- allocator-managed bytes, including channels, chunks, stream/request state,
  starh2 task records, certificates, routes, and TLS scratch;
- initially committed coroutine-stack bytes;
- reserved coroutine-stack virtual address space.

The stack ceilings include connection supervisors, both pumps, cancellation
reapers, all admitted handlers, and cached stacks. A counting allocator checks
only the first ceiling; OS mapping/RSS and zio task/stack accounting check the
other two. No allocator-only measurement is accepted as total memory evidence.
Pre-existing runtime tasks, caches, and allocations are explicitly outside the
incremental result. Resource acceptance creates a dedicated clean runtime with
the specified stack-pool configuration, takes a baseline before `Server.init`,
and compares the starh2 delta with these ceilings.
Allocation failure and pool exhaustion are observable outcomes; bytes are
never silently dropped.

The zio runtime uses all available executors and task migration. Its stack pool
is configured with a 1 MiB maximum stack, 64 KiB initially committed,
256 cached unused stacks, and a 30-second cache age. The server-wide stream,
connection, and cancellation-reaper limits bound total task count.

## 6. Transport negotiation

`Endpoint.mode` has exactly two v1 values:

- `.tls_h2`: TLS 1.3, ALPN list `{"h2"}`, then the HTTP/2 client preface;
- `.h2c_prior_knowledge`: cleartext TCP beginning directly with the HTTP/2
  client preface.

There is no automatic protocol sniffing and no HTTP/1 parser. HTTP/1.1
Upgrade is deprecated by RFC 9113 and is not implemented. A cleartext
connection whose first bytes do not match the HTTP/2 preface is closed without
an HTTP/1 response. A bad complete or impossible preface is a connection
`PROTOCOL_ERROR`; GOAWAY may be omitted as RFC 9113 permits.

Immediately after every accept and before reading TLS or cleartext bytes, the
edge calls `stream.socket.setNoDelay(true)`. Failure to set `TCP_NODELAY`
closes that accepted socket; it is not best-effort and setting it only on the
listener does not satisfy this contract.

The default preface/TLS-handshake timeout is five seconds.

## 7. HTTP/2 protocol profile

### 7.1 Initial settings

The server connection preface sends:

| Setting | Value |
|---|---:|
| `SETTINGS_HEADER_TABLE_SIZE` | 4,096 |
| `SETTINGS_ENABLE_PUSH` | 0 |
| `SETTINGS_MAX_CONCURRENT_STREAMS` | 256 |
| `SETTINGS_INITIAL_WINDOW_SIZE` | 65,535 |
| `SETTINGS_MAX_FRAME_SIZE` | 16,384 |
| `SETTINGS_MAX_HEADER_LIST_SIZE` | 32,768 |

The connection receive window is then raised from 65,535 to 4 MiB with a
connection WINDOW_UPDATE. Stream receive windows remain 65,535.

Peer settings govern server output. A peer reduction in
`SETTINGS_INITIAL_WINDOW_SIZE` is applied to every active outbound stream,
including a negative result; no DATA is sent until later WINDOW_UPDATE frames
make the relevant windows positive. Server output DATA frames are capped at
16 KiB even when the peer advertises a larger frame size.

### 7.2 Mandatory frame behavior

The receive implementation supports and validates:

- DATA, including padding and END_STREAM;
- HEADERS, including padding, the deprecated priority fields, END_HEADERS,
  END_STREAM, and following CONTINUATION frames;
- PRIORITY, whose Exclusive/Dependency/Weight fields are parsed and ignored;
  only its nonzero stream ID and exact five-byte length are enforced;
- RST_STREAM;
- SETTINGS and ACK;
- PING and ACK;
- GOAWAY;
- WINDOW_UPDATE;
- request trailers in a terminal HEADERS/CONTINUATION field block.

Unknown frame types and unknown settings are ignored after generic frame
validation, except that no frame may interleave a HEADERS/CONTINUATION field
block. PUSH_PROMISE from a client is a connection `PROTOCOL_ERROR`.

CONTINUATION is mandatory, not an optional edge case. A field block is
contiguous and connection-exclusive until END_HEADERS. Missing, interleaved,
or wrong-stream CONTINUATION produces connection `PROTOCOL_ERROR`.

The core tracks the highest peer-created odd stream ID. New stream IDs must be
odd and strictly increasing. Skipped lower odd IDs are implicitly closed.
Bounded tombstones retain reset/closed details needed for late-frame handling;
history does not grow with connection age.

### 7.3 HPACK

The decoder implements:

- the complete static table;
- indexed fields;
- literals with incremental indexing;
- literals without indexing;
- never-indexed literals;
- all integer prefix widths with overflow checks;
- RFC 7541 Huffman decoding, including EOS and padding validation;
- dynamic table insertion, eviction, and size updates.

The decoder dynamic table begins empty with a 4,096-byte maximum and never
allocates beyond that maximum. Invalid indices, integer encodings, Huffman
codes, EOS use, padding, table updates, or table-size transitions are
connection `COMPRESSION_ERROR`.

The encoder deliberately has no dynamic table and does not Huffman-encode in
v1. It uses static indices and literal representations. `authorization`,
`proxy-authorization`, `cookie`, and `set-cookie` are always encoded
never-indexed. All other literal fields are encoded without indexing. Because
the encoder does not mutate a dynamic table, cancellation cannot desynchronize
later responses.

Field limits are:

| Limit | Value |
|---|---:|
| Compressed field block | 64 KiB |
| CONTINUATION frames per field block | 16 |
| Decoded header-list size | 32 KiB using `name.len + value.len + 32` |
| Fields per section | 100 |
| Individual field name | 256 bytes |
| Individual regular field value | 8 KiB |
| `:path` wire value | 24 KiB |

Crossing the compressed-byte or continuation-count limit ends the connection
with `ENHANCE_YOUR_CALM`; the connection closes, so preserving the compression
context afterward is unnecessary. A syntactically valid field section that
crosses decoded list, field count, name, or regular-field value policy limits
is fully decompressed to keep the HPACK context synchronized, then receives a
431 response with END_STREAM. `:path` component or combined-size overflow is
instead governed by section 7.5 and returns 414. The application is not
dispatched.

### 7.4 HTTP request validation

Before dispatch, the server validates:

- pseudo-fields precede regular fields and are unique;
- required `:method`, `:scheme`, and `:path` fields are present for ordinary
  requests, with valid CONNECT rules even though CONNECT is not routed;
- field names are lowercase and contain only legal bytes;
- raw field values contain no NUL, CR, LF, prohibited bytes, or leading or
  trailing SP/HTAB;
- connection-specific fields are absent;
- `te`, when present, is one case-insensitive token equal to `trailers`; no OWS
  normalization is performed because leading/trailing whitespace was already
  rejected;
- `host`, when present with `:authority`, agrees after both are parsed to
  `(host, effective_port)`: reg-name comparison is ASCII case-insensitive,
  IPv6 literals compare by binary address, explicit ports compare numerically,
  and an omitted port equals 80 for `http` or 443 for `https`; userinfo is
  rejected;
- duplicate content-length values agree and parse without overflow;
- DATA payload length equals content-length;
- trailers contain no pseudo-fields and terminate the remote side.

A malformed HTTP request is a stream `PROTOCOL_ERROR` and is never dispatched.
The server sends RST_STREAM directly; it does not send an optional 400 first.
HPACK failures remain connection errors.

Request trailers are decoded, validated, retained in `Request.trailers`, and
counted against the same per-section limits. Response trailers are not exposed.

The handler is spawned only after the complete request, including body and
trailers, reaches END_STREAM. This keeps the v1 API bounded and intentionally
excludes streaming request bodies.

### 7.5 Body, path, and signal limits

The path component and encoded query component of `:path` are each limited to
16 KiB, and their combined wire value is limited to 24 KiB. Crossing any of
those limits yields 414 and no handler dispatch. This makes a 16 KiB Datastar
query reachable on the short fixed routes without allowing two maximum-size
components at once.

The decoded request body is limited to 256 KiB. If content-length proves the
overflow, the server sends 413 immediately; otherwise it sends 413 when byte
256 KiB + 1 arrives.

**Common early-rejection policy:** after a 431, 414, or 413 response with local
END_STREAM, the server continues receiving and decompressing field blocks,
discarding DATA and trailers through remote END_STREAM, restoring stream and
connection receive credit, enforcing content-length and trailer/message
semantics, and enforcing the request-body idle timeout. HPACK, frame,
connection-level, and stream-level errors still apply. A later malformed
trailer or content-length mismatch sends RST_STREAM `PROTOCOL_ERROR` but never
a second HTTP response. The application is never dispatched.

Aggregate request-storage exhaustion before dispatch resets the affected
stream with `REFUSED_STREAM`; no application side effect has occurred.

Datastar query parsing:

- recognizes one exact `datastar` key;
- limits the whole encoded query to 16 KiB (percent decoding cannot expand it);
- rejects duplicate `datastar` keys;
- percent-decodes once;
- treats malformed percent encoding or JSON as a typed parse error;
- limits JSON nesting to 64 and object fields to 1,024.

Datastar body parsing accepts at most 256 KiB and applies the same JSON depth
and field limits. Typed parsing ignores unknown fields, matching the Datastar
SDK convention. A parsed value is request-arena-owned.

The general helpers return typed errors. The fixed conformance routes map
missing, duplicate, malformed-percent, malformed-JSON, depth, field-count, and
wrong-type signal errors to 400; query component overflow maps to 414 and body
overflow maps to 413.

### 7.6 Flow control and scheduling

Inbound DATA, including pad length and padding, is debited from stream and
connection receive windows before application interpretation. Window credit is
returned only after bytes are copied into admitted request storage or
explicitly discarded. At half-window, WINDOW_UPDATE restores the stream to
65,535 and the connection to 4 MiB.

The complete DATA payload is charged to the connection window first. If that
underflows, the result is connection `FLOW_CONTROL_ERROR`. Otherwise a stream
window underflow is stream `FLOW_CONTROL_ERROR`. DATA on a stream that is later
rejected for stream state still affects the connection window. A zero-length
DATA frame with END_STREAM is legal when available credit is zero.

The writer schedules:

1. terminal GOAWAY/RST and required acknowledgments;
2. other control frames and response HEADERS;
3. DATA with 16 KiB deficit-round-robin quanta across flow-eligible streams.

After 64 nonterminal control frames, one eligible DATA quantum is allowed so a
control flood cannot permanently starve application progress. A stream with a
zero window is skipped without blocking other streams. A zero connection
window blocks all DATA, as required, but never blocks control frames.

Outbound handler writes wait cancellably for bounded capacity. They are never
dropped. A stream that has pending bytes but makes no stream-window or socket
progress for 30 seconds is reset with CANCEL and its handler observes
`error.SlowConsumer`.

The write pump flushes:

- before parking with buffered bytes;
- after response HEADERS and END_STREAM;
- after control/terminal batches;
- at an explicit application flush barrier;
- before graceful transport close.

An SSE write has an implicit flush barrier. A one-shot `send` returns only
after its END_STREAM batch has crossed a successful writer flush.

### 7.7 Work and time budgets

Defaults:

| Budget | Default action on excess |
|---|---|
| TLS handshake or client preface | 5 seconds, transport close without HTTP/2 error |
| Unfinished field block | 5 seconds, GOAWAY `ENHANCE_YOUR_CALM` |
| Request-body idle time | 15 seconds, RST_STREAM CANCEL |
| Graceful server drain | 30 seconds |
| Non-DATA frames | 10,000 per 10 seconds, GOAWAY `ENHANCE_YOUR_CALM` |
| RST_STREAM | 1,000 per 10 seconds, GOAWAY `ENHANCE_YOUR_CALM` |
| SETTINGS | 100 per 10 seconds, GOAWAY `ENHANCE_YOUR_CALM` |
| PING | 100 per 10 seconds, GOAWAY `ENHANCE_YOUR_CALM` |

Rate budgets are token buckets measured by the edge clock. A frame that causes
a required RFC connection error reports that protocol error rather than a
simultaneous rate error.

### 7.8 Error mapping

| Condition | Wire result | Application result |
|---|---|---|
| Invalid client preface | connection `PROTOCOL_ERROR`; GOAWAY optional | none |
| HPACK decode/table/Huffman failure | GOAWAY `COMPRESSION_ERROR` | all handlers connection closed |
| Interleaved/wrong CONTINUATION | GOAWAY `PROTOCOL_ERROR` | all handlers connection closed |
| Illegal new stream ID | GOAWAY `PROTOCOL_ERROR` | all handlers connection closed |
| Malformed HTTP fields or content-length | RST_STREAM `PROTOCOL_ERROR` | handler not started |
| Concurrent/global stream limit before dispatch | RST_STREAM `REFUSED_STREAM` | handler not started |
| DATA after remote END_STREAM | RST_STREAM `STREAM_CLOSED` | stream closed |
| Stream WINDOW_UPDATE zero | RST_STREAM `PROTOCOL_ERROR` | stream closed |
| Stream WINDOW_UPDATE causes overflow | RST_STREAM `FLOW_CONTROL_ERROR` | stream closed |
| Connection WINDOW_UPDATE zero | GOAWAY `PROTOCOL_ERROR` | connection closed |
| Connection window overflow | GOAWAY `FLOW_CONTROL_ERROR` | connection closed |
| SETTINGS initial-window change overflows any stream | GOAWAY `FLOW_CONTROL_ERROR` | connection closed |
| DATA exceeds connection receive credit | GOAWAY `FLOW_CONTROL_ERROR` | connection closed |
| DATA exceeds only stream receive credit | RST_STREAM `FLOW_CONTROL_ERROR` | stream closed |
| Applicable bad frame length | connection or stream `FRAME_SIZE_ERROR` exactly as RFC 9113 specifies | closed scope |
| Peer RST_STREAM | no answering RST | `error.PeerReset`; code in terminal cause |
| Slow outbound consumer | RST_STREAM CANCEL | `error.SlowConsumer` |
| Live handler application error before response | 500 one-shot response | original error logged |
| Handler cancellation/reset/shutdown/slow-consumer error | no new response | terminal cause retained |
| Handler error after response commitment | RST_STREAM `INTERNAL_ERROR` | original error logged |
| Connection-owned allocation failure | GOAWAY `INTERNAL_ERROR` best effort | connection closed |

After RST_STREAM, the connection still performs the minimum HPACK and
connection-flow accounting RFC 9113 requires. It never answers RST with RST.
`REFUSED_STREAM` is never used after a handler has started.

Transport EOF or read/write failure is not misreported as a peer protocol
error. The edge may inspect concrete zio diagnostics for cancellation, but the
core sees only a typed transport termination.

## 8. Application API

The server is allocator-injected and externally controllable:

```zig
pub const EndpointConfig = union(enum) {
    tls_h2: std.Io.net.IpAddress,
    h2c_prior_knowledge: std.Io.net.IpAddress,
};

pub const EndpointAddress = std.Io.net.IpAddress;

pub const TlsConfig = struct {
    certificate_chain_pem: []const u8,
    private_key_pem: []const u8,
};

pub const ResourceUpperBound = struct {
    allocator_bytes: usize,
    committed_stack_bytes: usize,
    virtual_stack_bytes: usize,
};

pub const Limits = struct {
    max_endpoints: usize = 8,
    max_routes: usize = 256,
    max_route_path_bytes: usize = 64 * 1024,
    max_connections: usize = 1_024,
    max_streams_per_connection: usize = 256,
    max_streams_per_server: usize = 4_096,
    inbound_wire_chunks_per_connection: usize = 8,
    outbound_bytes_per_stream: usize = 64 * 1024,
    outbound_bytes_per_connection: usize = 4 * 1024 * 1024,
    outbound_bytes_per_server: usize = 256 * 1024 * 1024,
    request_bytes_per_connection: usize = 8 * 1024 * 1024,
    request_bytes_per_server: usize = 256 * 1024 * 1024,
    control_bytes_per_connection: usize = 64 * 1024,
    control_entries_per_connection: usize = 256,
    stream_tombstones: usize = 1_024,
    concurrent_tls_handshakes: usize = 128,
    certificate_chain_bytes: usize = 64 * 1024,
    private_key_bytes: usize = 16 * 1024,
    tls_handshake_scratch_bytes: usize = 256 * 1024,
    cancellation_reaper_tasks: usize = 64,
    cancellation_reaper_jobs: usize = 4_096,
    decoded_header_bytes: usize = 32 * 1024,
    compressed_header_bytes: usize = 64 * 1024,
    header_fields: usize = 100,
    continuation_frames: usize = 16,
    regular_field_value_bytes: usize = 8 * 1024,
    path_wire_bytes: usize = 24 * 1024,
    path_component_bytes: usize = 16 * 1024,
    query_component_bytes: usize = 16 * 1024,
    request_body_bytes: usize = 256 * 1024,
    preface_timeout_ns: u64 = 5 * std.time.ns_per_s,
    field_block_timeout_ns: u64 = 5 * std.time.ns_per_s,
    request_body_idle_timeout_ns: u64 = 15 * std.time.ns_per_s,
    slow_consumer_timeout_ns: u64 = 30 * std.time.ns_per_s,
    graceful_drain_timeout_ns: u64 = 30 * std.time.ns_per_s,

    pub const defaults: Limits = .{};
    pub fn resourceUpperBound(self: Limits) error{InvalidConfig}!ResourceUpperBound;
};

pub const InitError = error{
    OutOfMemory,
    InvalidConfig,
    InvalidRoute,
    InvalidCertificate,
    CertificateTooLarge,
    PrivateKeyTooLarge,
    TlsPatchMismatch,
};

pub const ServeError = error{
    OutOfMemory,
    BindFailed,
    ListenFailed,
    RuntimeShutdown,
    TransportSetupFailed,
};

pub const ServerConfig = struct {
    endpoints: []const EndpointConfig,
    routes: []const Route,
    tls: ?TlsConfig,
    limits: Limits = .defaults,
};

pub const Server = struct {
    pub fn init(
        gpa: std.mem.Allocator,
        runtime: *zio.Runtime,
        config: ServerConfig,
    ) InitError!Server;

    pub fn serve(self: *Server, gpa: std.mem.Allocator) ServeError!void;
    pub fn requestShutdown(self: *Server) void;
    pub fn localAddress(self: *const Server, endpoint_index: usize) EndpointAddress;
    pub fn deinit(self: *Server, gpa: std.mem.Allocator) void;
};
```

`Server` borrows the runtime, whose lifetime encloses `deinit`. `serve` has the
caller precondition that it runs in a zio task spawned by that runtime; zio
v0.16.0 has no public current-runtime accessor, so this is not dynamically
checked. The conformance harness meets the precondition by invoking `serve`
inside `runtime.spawn` and joining that handle. `init`, `serve`, and `deinit`
must receive the same allocator identity; a mismatch is a caller-contract
assertion in Debug and ReleaseSafe.

`init` validates `Limits`, requires `tls` exactly when at least one TLS endpoint
exists, and copies
routes, endpoint configuration, and certificate/key material into bounded
server-owned storage. `Handler.ptr` remains caller-owned, must outlive the
server, and may be invoked concurrently on different executors. `serve` uses
only its passed allocator for later connection/request allocations, allowing a
grader to inject counting and failing allocators. `requestShutdown` is
thread-safe and idempotent. `localAddress` exposes kernel-selected ports.

The handler-facing shape is:

```zig
pub const Handler = struct {
    ptr: *anyopaque,
    runFn: *const fn (*anyopaque, *const Request, *Response) anyerror!void,
};

pub const Route = struct {
    method: Method,
    path: []const u8,
    handler: Handler,
};

pub const Request = struct {
    method: Method,
    scheme: []const u8,
    authority: []const u8,
    path: []const u8,
    query: []const u8,
    headers: []const Header,
    body: []const u8,
    trailers: []const Header,
};

pub const Response = struct {
    pub fn send(
        self: *Response,
        status: u16,
        headers: []const Header,
        body: []const u8,
    ) ResponseError!void;

    pub fn start(
        self: *Response,
        status: u16,
        headers: []const Header,
    ) ResponseError!Body;

    pub fn startSse(
        self: *Response,
        headers: []const Header,
    ) ResponseError!Body;
};

pub const Body = struct {
    pub fn writeAll(self: *Body, bytes: []const u8) ResponseError!void;
    pub fn flush(self: *Body) ResponseError!void;
    pub fn finish(self: *Body) ResponseError!void;
    pub fn abort(self: *Body) ResponseError!void;
    pub fn terminalCause(self: *const Body) ?TerminalCause;
};

pub const TerminalCause = union(enum) {
    peer_reset: H2ErrorCode,
    slow_consumer,
    connection_closed,
    server_shutdown,
    internal,
};

pub const ResponseError = error{
    Canceled,
    OutOfMemory,
    InvalidStatus,
    InvalidHeader,
    ResponseCommitted,
    BodyClosed,
    ConnectionClosed,
    PeerReset,
    SlowConsumer,
    WriteFailed,
};
```

The exact-route router strips the query before matching. It performs an exact
method and path comparison, has no parameters or wildcard syntax, returns 404
when no path exists, and 405 with `allow` when the path exists for another
method. Routes are fixed at server initialization.

`send` and `start`/`startSse` are mutually exclusive and can commit a response
only once. Zig cannot make `Body` affine, so copies alias one logical body.
Every copy contains a pointer, stream ID, and generation checked against
actor-owned state. Either alias can operate while the logical body is open;
the first terminal transition wins, and all aliases then return
`error.BodyClosed`. `Body` is not thread-safe, so simultaneous calls are a
caller contract violation even though terminal state is checked atomically.
`writeAll` never drops bytes. `finish` sends END_STREAM once. `abort` requests
RST_STREAM CANCEL and is idempotent but fallible when the connection is already
terminal.

`startSse` sends status 200 and these mandatory fields:

- `content-type: text/event-stream`;
- `cache-control: no-cache`;
- `x-accel-buffering: no`.

It never emits `connection`, `keep-alive`, or `transfer-encoding`. Every
`writeAll` on an SSE body includes an implicit flush barrier. The bytes are
already-formatted SSE event blocks, normally returned by the Datastar SDK.
`starh2` does not rewrite or coalesce event semantics.

Returning without committing a response produces a 500 one-shot response.
Returning after `start` without `finish` resets the stream with INTERNAL_ERROR.
`ResponseError` is a normal Zig error set and carries no payload. A peer reset
causes later body operations to return `error.PeerReset`; its HTTP/2 code is
available from `terminalCause()`. Connection shutdown returns
`error.ConnectionClosed`.

`starh2.datastar` re-exports `patchElements`, `patchElementsFmt`,
`patchSignals`, `executeScript`, their option types, and URL decoding from the
pinned SDK. It adds:

```zig
pub fn readSignalsFromQuery(comptime T: type, request: *const Request) SignalError!T;
pub fn readSignalsFromBody(comptime T: type, request: *const Request) SignalError!T;
```

The result is allocated in request-owned storage and is valid only during the
handler. The helpers enforce the limits in section 7.5 before materializing a
JSON tree.

## 9. Graceful shutdown

On requested server shutdown, each connection:

1. sends GOAWAY NO_ERROR with last-stream-ID `2^31-1`;
2. sends a PING and waits for its ACK or one second;
3. records the highest accepted stream and sends a second GOAWAY NO_ERROR with
   that ID;
4. refuses later streams, lets existing handlers drain for up to 30 seconds;
5. resets remaining streams with CANCEL;
6. emits TLS close-notify when applicable, flushes, joins pumps, and closes the
   socket once.

Protocol failure skips the two-phase grace and sends one best-effort GOAWAY
with the highest successfully processed stream ID and the selected error code.
Long-lived SSE does not make shutdown unbounded.

## 10. Explicit v1 non-goals

The grader must not require:

- HTTP/1.0 or HTTP/1.1;
- h2c HTTP/1.1 Upgrade;
- ALPN fallback to `http/1.1`;
- server push or sending PUSH_PROMISE;
- priority-tree or extensible-priority scheduling;
- CONNECT tunneling, RFC 8441 extended CONNECT, or WebSockets;
- response trailers or informational responses;
- HTTP/3 or QUIC;
- an HTTP client;
- streaming request bodies;
- dynamic-table or Huffman use by the encoder;
- response compression;
- middleware, sessions, CORS kits, route parameters, wildcard routing, static
  files, templates, or a compatibility layer for another Zig server.

Receiving CONTINUATION, HPACK dynamic/Huffman forms, padded frames, PRIORITY,
and request trailers is not a non-goal.

## 11. Fixed conformance-server contract

The repository builds `zig-out/bin/starh2-conformance-server`.

```text
starh2-conformance-server \
  --mode tls|h2c \
  --bind 127.0.0.1:0 \
  [--cert <pem> --key <pem>]
```

It writes logs only to stderr. After listen and before accept it writes exactly
one JSON line to stdout:

```json
{"ready":true,"mode":"tls","protocol":"h2","port":12345}
```

For h2c, `protocol` is `"h2c"`. SIGTERM runs the graceful-shutdown contract.
The process spawns no subprocess and makes no outbound network connection.

The fixed routes are:

| Route | Behavior |
|---|---|
| `GET /hello` | Returns `hello:<x-grader-nonce>` as `text/plain` |
| `GET /sse?datastar=...` | Parses query signals, opens SSE, immediately patches a nonce/sequence element, then emits a nonce-bearing event every 100 ms |
| `POST /morph` | Parses body signals and returns one nonce-bearing Datastar patch-elements event |
| `GET /signals?datastar=...` | Parses query signals and returns a patch-signals event |
| `POST /signals` | Parses body signals and returns a patch-signals event |

Expected values derive from a fresh grader nonce and parsed input, not from
published static fixtures.

## 12. Mechanical acceptance gates

All gates are conjunctive. No score or average allows one axis to compensate
for another.

### 12.1 Build and ownership

1. `./zb build test` passes under Debug with `std.testing.allocator`.
2. ReleaseSafe conformance server and examples build on supported macOS and
   Linux targets.
3. Allocator counting stays within the allocator component of
   `Limits.resourceUpperBound()`. Grader-owned zio task/stack accounting and OS
   mapping/RSS probes stay within its committed-stack and virtual-space
   components.
4. Through the public allocator-injected Server API, a grader iterates
   allocation failure across initialization, accept, request assembly,
   response, and teardown without leak, panic, or silent response loss.
5. A real accepted socket reports `TCP_NODELAY=1` through `getsockopt`.
   Linux acceptance also traces the running server and proves the setsockopt
   occurs on the accepted descriptor before its first read; a helper that is
   never called does not pass.
6. The server runs in an egress-disabled clean container with one listener
   process, no children, and no runtime HTTP/TLS proxy dependency.

### 12.2 Protocol identity and interop

The independent grader pins its own tools and rejects missing capabilities.

1. TLS curl uses `--http2`; captured verbose output must contain ALPN server
   acceptance of `h2`, and `%{http_version}` must equal `2`.
2. Cleartext curl uses `--http2-prior-knowledge`; `%{http_version}` must equal
   `2`.
3. `nghttp` verbose transcripts independently show SETTINGS and HTTP/2
   responses over TLS and cleartext prior knowledge.
4. A raw HTTP/1 request receives no valid HTTP/1 response.
5. A TLS client offering no `h2` receives no application response.
   Its wire capture must contain TLS alert 120. Malformed TLS record probes
   must produce the specified alert or clean close without panic, undefined
   access, or an HTTP/2 frame.
6. A real headless Chrome fetch over TLS reports protocol `h2`, receives the
   SSE event incrementally, and applies the expected Datastar DOM morph.
   Browser testing is not used for h2c because browsers generally do not
   support cleartext prior knowledge.
7. Pinned h2spec v2.6.0 runs its applicable strict suite over TLS and h2c with
   no expected-failure baseline. The repository publishes the stable IDs and
   RFC citations of inapplicable cases; exclusions are limited to RFC
   7540-only priority dependency semantics removed by RFC 9113 and optional
   features this server did not advertise. Any broader exclusion fails.
   h2spec remains supplementary to RFC 9113-specific held-out probes.

The grader captures these facts itself. Server logs or self-reported protocol
strings are not evidence.

### 12.3 Datastar multiplexing

A grader-owned Go client using `x/net/http2` supplies a dialer that counts
physical TCP connections and enables `StrictMaxConcurrentStreams`.

For TLS and h2c separately:

1. Exactly one TCP connection opens 100 SSE streams.
2. Every stream receives response fields and one valid nonce-bearing Datastar
   event within 10 seconds.
3. While all 100 remain open, 200 nonce-bearing morph requests are submitted.
   At most 156 are active at once, so SSE plus morph streams never exceed the
   advertised 256; the remainder queue client-side. All finish within 15
   seconds and no request, once admitted by the client transport, exceeds two
   seconds.
4. Every response maps to its own nonce; cross-stream mixing is a failure.
5. One SSE stream exhausts its stream window and stops reading. The other 99
   continue receiving events and all morphs still finish.
6. Each unaffected SSE stream receives an event before and after the morph
   burst, proving concurrent progress rather than sequential completion.
7. Resetting a subset of SSE streams cancels only those handlers; a new probe
   stream succeeds on the same connection.

The client controls and counts the socket. Opening one connection per stream or
buffering SSE until close cannot pass.

### 12.4 Limits and error scope

Held-out raw-frame and application probes exercise `limit - 1`, `limit`, and
`limit + 1` for every wire-visible limit: frame and compressed-field-block
bytes, continuation count, decoded header bytes/count/name/value, `:path`
components and combined value, body/query bytes, concurrent streams, and
stream/connection flow windows. They verify:

- the exact status, RST_STREAM, or GOAWAY result;
- the exact HTTP/2 error code;
- GOAWAY last-stream-ID;
- whether an unaffected stream must survive;
- that no handler ran when dispatch was forbidden;
- that connection/request allocation stayed capped.

Configuration and internal capacity limits (certificate/key/scratch size,
wire-chunk and control-pool counts, tombstones, server-wide memory, and stack
ceilings) are instead tested through their exact initialization, resource
accounting, refusal, and allocation-failure API outcomes in section 12.1; they
are not required to manufacture an HTTP wire error.

The negative corpus covers:

- frame lengths at zero, legal maximum, and over maximum;
- every TCP split of selected headers/frames and coalesced preface+SETTINGS;
- padded DATA and HEADERS;
- missing, interleaved, wrong-stream, excessive, and timed-out CONTINUATION;
- HPACK dynamic indexing/eviction/size updates and Huffman EOS/padding errors;
- invalid stream IDs and every stream state, including reset and GOAWAY races;
- zero/overflow/tiny WINDOW_UPDATE and peer window reductions;
- SETTINGS ACK/length/value errors, PING errors, and unknown extensions;
- uppercase/illegal fields, pseudo-field ordering/duplicates/omissions,
  authority mismatch, connection fields, TE variants, CR/LF/NUL, duplicate or
  mismatched content-length, and bad trailers;
- query/body Unicode, percent encoding, JSON escape expansion, nesting,
  duplicates, wrong types, fragmentation, and cap boundaries;
- rapid reset, SETTINGS/PING, empty-frame, and slow-consumer attacks;
- cancellation during queued HEADERS, DATA, TLS records, and handler waits.

After a stream-scoped failure, an unaffected probe stream must succeed whenever
RFC 9113 permits connection survival.

## 13. Fuzz contract

The build exposes three separately selectable `std.testing.fuzz` targets:

1. **frame** — arbitrary byte chunks into the preface and frame parser;
2. **hpack** — arbitrary field blocks into the HPACK decoder;
3. **session** — structured and raw sequences covering HEADERS/DATA,
   WINDOW_UPDATE, RST_STREAM, SETTINGS, PING, and GOAWAY.

Each target ships a nonempty corpus. Corpus regression tests prove:

- frame seeds reach every known frame type, partial header/payload, unknown
  type, malformed length, padding, and CONTINUATION path;
- HPACK seeds reach every representation, integer width, dynamic-table
  insertion/eviction/update, Huffman decode, and malformed path;
- session seeds cause open, both half-closed states, closed/reset, GOAWAY,
  blocked/unblocked flow control, and at least one stream and connection error.

Targets use `std.testing.Smith` or equivalent generated actions, not only a
loop over valid fixtures. A case is successful only if it reaches a parser or
state classification. Returning before input affects state is a harness
failure.

Invariants checked after every operation:

- input position is monotonic and never exceeds input length;
- each loop consumes input, emits an event, reaches `need_more`, or terminates;
- no loop exceeds `2 * input_bytes + 1,024` state steps;
- HPACK table accounting equals the sum of retained entries and is at most
  4,096 bytes;
- decoded/list/queue/pool accounting stays within configured limits;
- stream states and stream-ID high-water marks obey RFC transitions;
- flow windows do not overflow and DATA intents never exceed available credit;
- every outbound frame intent is legal for its connection and stream state;
- the session is waiting for a defined event or is in one defined terminal
  state;
- arbitrary input, allocator failure, and queue exhaustion never panic or hit
  an assertion.

Deterministic held-out grading generates at least 10,000 seeded cases per
target, limits each input to 64 KiB, each case to 100 ms, and each target to
60 seconds. Valid HPACK cases are compared with Go's independent HPACK decoder.
Frame/state outcomes are compared with a grader-owned model. Selected valid
transcripts are replayed at every possible split and must produce identical
application results.

The held-out suite includes mutation canaries for frame length, required
CONTINUATION, invalid HPACK index, Huffman padding, flow-window overflow, and
illegal stream transition. A fuzz target that does not reject each canary
fails before any soak is credited.

PR CI runs at least 100,000 generated cases per target. Nightly/release CI runs
each target for five minutes or one million cases, whichever is later. A crash,
panic, invariant failure, allocator leak, or timeout fails. Fuzz execution
counts come from the grader, not the implementation.

## 14. Independent oracle and review policy

The external grading stack is:

- curl built with HTTP/2;
- nghttp2 `nghttp`;
- h2spec v2.6.0 in strict mode;
- a grader-owned Go `x/net/http2`, Framer, and HPACK harness;
- headless Chrome controlled through CDP.

Tool versions and downloaded artifact hashes are pinned in the grader image.
Expected protocol version, ALPN, socket count, field/body meaning, DOM state,
error scope/code, and fuzz execution are recomputed from client observations.
Builder output is never an oracle input.

Exact held-out bytes and seeds remain outside the implementation repository,
while these categories remain public:

1. ALPN and HTTP/1 anti-fallback;
2. h2c preface fragmentation/coalescing;
3. frame and continuation boundaries;
4. HPACK dynamic/Huffman/eviction/bomb cases;
5. stream-state and connection-vs-stream errors;
6. flow-control and blocked-stream isolation;
7. signal encoding and cap boundaries;
8. SSE flush/order/reset behavior;
9. 100-stream multiplexing with morph traffic;
10. Chrome Datastar DOM round trips;
11. resource exhaustion and repeated teardown;
12. regression cases from every later review.

Every independent review finding becomes a permanent fixture. Fixes are
re-reviewed with emphasis on regressions introduced by the previous round.
Green means no known held-out case fails, not proof of universal correctness.

## 15. Implementation order

The implementation follow-up proceeds in these gates:

1. bounded frame parser/serializer and independent frame corpus;
2. HPACK decoder/encoder and RFC 7541 oracle tests;
3. deterministic Session, stream states, flow control, and error corpus;
4. cleartext actor and raw zio pumps;
5. actor-owned nonblocking TLS and ALPN;
6. request/response/router and Datastar helpers;
7. SSE fairness, shutdown, and resource budgets;
8. interop, h2spec, held-out grading, fuzz soak, and independent re-review.

No later gate weakens an earlier assertion to make integration pass. A valid
ported case that fails is a finding, not permission to change the expected
wire behavior.

