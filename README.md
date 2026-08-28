# starh2

A server-side HTTP/2 stack in Zig, shaped around [Datastar](https://data-star.dev).

HTTP/2 is the whole surface. Datastar's load is long-lived SSE multiplexed with
latency-sensitive one-shot responses on one connection, and HTTP/2 already gives
stream isolation, flow control, and fair multiplexing for that shape. Everything
below follows from serving that one shape well rather than serving everything.

**Status:** used in production by one consumer. The API is not yet stable.

## Non-goals

These are refused on purpose, not missing. Each one would pull in parsers or
lifecycle modes that the target workload does not use, and every one of them
would need the same conformance and security work as the paths that are used.

- HTTP/1.1 keep-alive, chunked encoding, h2c `Upgrade`, ALPN fallback, HTTP/3
  (a first-cut HTTP/1.1 oneshot sibling lives in `starh2.http1`: Content-Length
  framing and `Connection: close` that actually ends the connection)
- Server push, `CONNECT`, WebSockets, response trailers
- Streaming request bodies, response compression
- A client library
- Framework surface: middleware, sessions, CORS, route parameters, wildcards,
  static files, templates

## Requirements

- Zig 0.16.0 (pinned; the TLS dependency is not source-compatible with 0.17-dev)
- For TLS, a certificate and key in PEM form
- curl built with HTTP/2, to run the TLS gate
- With more than one zio executor, set `enable_task_migration = false`. The
  pinned zio can otherwise strand a migrated socket task under repeated TLS
  connection churn while readable and writable bytes remain queued in the
  kernel.

## Install

```sh
zig fetch --save git+https://github.com/dimenus/starh2
zig fetch --save git+https://github.com/lalinsky/zio
```

<!-- doctest: build -->
```zig
const starh2 = b.dependency("starh2", .{ .target = target, .optimize = optimize });
exe_mod.addImport("starh2", starh2.module("starh2"));

// `Server.init` takes a `std.Io`. zio provides one, and it is the
// implementation starh2 is built and tested against. Declare it yourself, so
// starh2's pin of zio is not forced on you.
const zio = b.dependency("zio", .{ .target = target, .optimize = optimize });
exe_mod.addImport("zio", zio.module("zio"));
```

## Use

A cleartext server, prior-knowledge HTTP/2. There is no `Upgrade` handshake: a
client speaks HTTP/2 immediately or it is refused.

<!-- doctest: program -->
```zig
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/plain" }}, "hello");
}

const ctx: u8 = 0;

fn run(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 8080);
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &.{.{
            .method = .GET,
            .path = "/hello",
            .handler = .{ .complete = .{ .ptr = @constCast(&ctx), .runFn = hello } },
        }},
        .tls = null,
    });
    defer server.deinit(gpa);
    try server.serve(gpa);
}
```

`serve` blocks until shutdown. It binds inside its own task, so a caller that
needs to know the listener is up calls `server.waitUntilListening(timeout_ns)`
rather than sleeping; it reports `BindFailed` instead of timing out when a bind
failed. `server.requestShutdown()` starts a graceful stop.

For TLS, swap the endpoint and supply PEM bytes:

<!-- doctest: config -->
```zig
.endpoints = &.{.{ .tls = addr }},
.tls = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem },
```

ALPN prefers `h2` and falls back to `http/1.1`. A client that sends no ALPN
is served HTTP/1.1. Cleartext HTTP/1.1 uses the `h1c` endpoint.

### Responses

A route is `.complete` or `.task`. Complete handlers receive `CompleteResponse`
and may run on the connection actor — they can only `send` a finished body, so
they cannot stream, sleep, or wait on that socket's ingest path. Bytes still
leave through the write pump. Task handlers receive `Response`, always run on
their own task, and may stream or wait.

<!-- doctest: handler -->
```zig
// Complete / one-shot: headers and a finished body.
try resp.send(200, &.{}, body);

// Task — streaming: headers now, body later.
var out = try resp.start(200, &.{});
try out.writeAll(chunk);
try out.finish();

// Task — server-sent events. Each write reaches the wire before it returns.
var events = try resp.startSse(&.{});
try events.writeAll("data: hello\n\n");
```

A task handler runs on its own task, so the stream can end underneath it. Every
call returns the exact reason rather than a generic failure — `error.PeerReset`,
`error.SlowConsumer`, `error.ConnectionClosed`, `error.WriteFailed` — because a
peer that reset a stream and a server that failed to write need different
reactions. `Body.terminalCause()` reports it without attempting a write.

### Routes

Exact paths are matched first, so adding a prefix route never changes what an
existing exact route answers. Among prefix routes the longest match wins, and
`Request.path_remainder` carries the bytes past the prefix.

<!-- doctest: route -->
```zig
.{ .method = .GET, .path = "/v1/tasks/", .prefix = true, .handler = h },
```

## Bounded by construction

Every per-connection buffer is reserved at connection boot, and the write path
does not allocate. A response larger than a stream's slab parks the handler
until the peer reads, rather than growing a buffer.

That makes the memory ceiling computable before the process serves anything:

<!-- doctest: snippet -->
```zig
const bound = try starh2.Limits.defaults.resourceUpperBound();
// bound.terms holds the per-term breakdown behind this total.
std.debug.print("upper bound: {d} bytes\n", .{bound.allocator_bytes});
```

`Server.init` calls it first and refuses to start on `error.InvalidConfig`, so
an inconsistent configuration fails at boot instead of under load. The terms are
built from real `@sizeOf` values, and a struct that grows without its recorded
size being updated fails the build.

`Limits` also carries the admission and timeout budgets — streams per
connection and per server, request and outbound byte ceilings, header and
CONTINUATION caps, and the idle deadlines that bound a peer that opens work and
then stops.

## Datastar

Signal reads live in their own module, `starh2_datastar`. Add it beside the core
module only if you want them:

<!-- doctest: build -->
```zig
exe_mod.addImport("starh2_datastar", starh2.module("starh2_datastar"));
```

<!-- doctest: handler -->
```zig
const ds = @import("starh2_datastar");

const Signals = struct { query: []const u8 = "" };
const signals = try ds.readSignalsFromQuery(Signals, req); // or ...FromBody
try resp.send(200, &.{}, signals.query);
```

Datastar sends client-side state as JSON, in the `datastar` query parameter for
a GET and in the body otherwise. This module is that convention and nothing
else. The generic parts, form decoding and parameter lookup, stay in
`starh2.http.form`, because they are ordinary HTTP.

Signal JSON is attacker-controlled, so size, nesting depth, and field count are
bounded before the typed parse runs. Values live in the request arena.

The module does **not** re-export the Datastar SDK, and nothing starh2 ships
depends on the SDK. The SDK emitters take an allocator and return bytes, so they
carry no transport coupling and a consumer calls them directly. A re-export
would oblige every user of an HTTP/2 server to compile a hypermedia SDK, and it
would force starh2's pin of the SDK onto that consumer. Declare the SDK
yourself, at the version you want. Inside this repo the SDK is a lazy
dependency, and `starh2-conformance-server` is the only target that imports it.

## Architecture

Three layers, one-way dependencies:

- `core` — the protocol as a deterministic state machine. No I/O, no clock, no
  lock. The same bytes produce the same intents on every run, which is why a
  conformance failure is a test case rather than a timing story.
- `edge` — sockets, TLS, tasks, timers, memory limits. It drives `core` and
  executes the intents `core` emits.
- `http` — the handler-facing surface.

`core` never depends on `edge`. Start reading at `src/root.zig` for the request
path, then `src/edge/connection.zig`, whose header carries the task topology,
the lock discipline, and the wake protocol — most of the subtle rules in this
stack follow from those three.

Decisions and their rationale live in the git log, not in this file.

## Testing

`AGENTS.md` carries the gates and how to run them, including the TLS gate and
the interop commands. Conformance runs against h2spec; the only accepted
failures are the two documented RFC 7540 priority exclusions in
`tools/h2spec/EXCLUSIONS.md`.

## License

MIT. See `LICENSE`.
