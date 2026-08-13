//! starh2 — server-side HTTP/2 stack shaped around Datastar.
//!
//! # How to read this tree
//!
//! Three layers, and the dependency direction is one-way:
//!
//! - `core` — the protocol, as a deterministic state machine. No I/O, no
//!   clock, no lock, no knowledge that an edge exists. Given the same bytes it
//!   produces the same intents on every run.
//! - `edge` — everything real: sockets, TLS, tasks, timers, memory limits. It
//!   drives `core` and executes the intents `core` produces.
//! - `http` — the handler-facing surface. `Request`, `Response`, `Router`.
//!
//! `edge` depends on `core`. `core` never depends on `edge` for behaviour. The
//! split is why a conformance failure is reproducible: the decision lives in a
//! module that has no timing in it.
//!
//! # The shape of a request
//!
//! socket -> `ReadPump` -> actor -> `Session.ingest` -> `Intent.dispatch_request`
//! -> `Router` -> handler task -> `Response` -> `Session` command -> `Intent`
//! -> `FairScheduler` -> `WritePump` -> socket.
//!
//! Start with `edge/connection.zig`. Its module header carries the task
//! topology, the lock discipline, and the wake protocol, and nearly every
//! subtle rule in this stack is a consequence of one of those three.
//!
//! # What this stack deliberately does not do
//!
//! HTTP/2 only: no HTTP/1.1, no h2c Upgrade, no ALPN fallback, no HTTP/3. Also
//! no server push, no CONNECT, no response trailers, and no streaming request
//! bodies. Datastar needs multiplexed long-lived SSE beside one-shot updates,
//! and every omission above removes a code path that would otherwise need the
//! same conformance and security work as the ones that are used.
const std = @import("std");

pub const core = struct {
    pub const frame = @import("core/frame.zig");
    pub const hpack = @import("core/hpack.zig");
    pub const fields = @import("core/fields.zig");
    pub const stream = @import("core/stream.zig");
    pub const flow = @import("core/flow.zig");
    pub const session = @import("core/session.zig");
    pub const limits = @import("core/limits.zig");
    pub const rates = @import("core/rates.zig");
    pub const bound_shapes = @import("core/bound_shapes.zig");
    pub const wire_const = @import("core/wire_const.zig");
};

pub const edge = struct {
    pub const server = @import("edge/server.zig");
    pub const connection = @import("edge/connection.zig");
    pub const wire_pump = @import("edge/wire_pump.zig");
    pub const tls_edge = @import("edge/tls.zig");
    pub const h2c = @import("edge/h2c.zig");
    pub const ticket_table = @import("edge/ticket_table.zig");
    pub const control_pool = @import("edge/control_pool.zig");
    pub const fair_scheduler = @import("edge/fair_scheduler.zig");
    pub const slab_pool = @import("edge/slab_pool.zig");
};

pub const http = struct {
    pub const request = @import("http/request.zig");
    pub const response = @import("http/response.zig");
    pub const router = @import("http/router.zig");
    /// Form-urlencoded decoding for query strings. Generic HTTP: nothing in
    /// here knows about Datastar, and `std` has no form decoder.
    pub const form = @import("http/form.zig");
};


pub const Limits = core.limits.Limits;
pub const ResourceUpperBound = core.limits.ResourceUpperBound;
pub const Server = edge.server.Server;
pub const ServerConfig = edge.server.ServerConfig;
pub const EndpointConfig = edge.server.EndpointConfig;
pub const EndpointAddress = edge.server.EndpointAddress;
pub const TlsConfig = edge.server.TlsConfig;
pub const InitError = edge.server.InitError;
pub const ServeError = edge.server.ServeError;
pub const BindState = edge.server.BindState;
pub const Handler = http.router.Handler;
pub const Route = http.router.Route;
pub const Request = http.request.Request;
pub const Response = http.response.Response;
pub const Body = http.response.Body;
pub const Header = http.request.Header;
pub const Method = http.request.Method;
pub const TerminalCause = http.response.TerminalCause;
pub const ResponseError = http.response.ResponseError;
pub const H2ErrorCode = core.frame.ErrorCode;
pub const Session = core.session.Session;

test {
    _ = core.frame;
    _ = core.hpack;
    _ = core.fields;
    _ = core.stream;
    _ = core.flow;
    _ = core.session;
    _ = core.limits;
    _ = core.rates;
    _ = edge.server;
    _ = edge.connection;
    _ = edge.wire_pump;
    _ = edge.tls_edge;
    _ = edge.h2c;
    _ = edge.ticket_table;
    _ = edge.control_pool;
    _ = edge.fair_scheduler;
    _ = edge.slab_pool;
    _ = core.bound_shapes;
    _ = http.request;
    _ = http.response;
    _ = http.router;
    _ = http.form;
}
