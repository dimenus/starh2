//! Listen, accept, global limits, graceful stop.
//!
//! `Server` owns everything that outlives a single connection: the listeners,
//! the routes, the TLS key material, the request pool, and the server-wide
//! counters in `GlobalAccounting`. A `Connection` borrows all of it and owns
//! only its own stream.
//!
//! # Task groups, and why there are two
//!
//! Shutdown has an ORDER, and one group cannot express it:
//! 1. Accept loops stop first, so no new connection can arrive during the
//!    drain.
//! 2. Connections drain next, so in-flight requests can finish.
//! 3. Handlers that outlived their connection end last; they are in no
//!    group, and `serve` waits for them without a limit, and reports what it
//!    waits for (`waitForHandlers`).
//! A single group would cancel the connections with the accept loops and
//! abort the very work the graceful path exists to preserve.
//!
//! # Two shutdown signals, not one
//!
//! `shutdown_flag` is what a loop polls; `shutdown_event` is what a parked task
//! waits on (a zio.Event: set once, never reset, so the actor's select
//! observes it race-free). Both are set together, because a connection actor
//! may be parked in a select where a flag alone would never be observed.
const std = @import("std");
const test_hook = @import("test_hook.zig");
const builtin = @import("builtin");
const zio = @import("zio");
const limits_mod = @import("../core/limits.zig");
const router_mod = @import("../http/router.zig");
const connection = @import("connection.zig");
const conn_slot = @import("conn_slot.zig");
const h1 = @import("h1.zig");
const slab_pool = @import("slab_pool.zig");
const request_pool = @import("request_pool.zig");
const brotli = @import("../http/brotli.zig");
const tls_edge = @import("tls.zig");
const accounts = @import("accounts.zig");
pub const proxy_protocol = @import("proxy_protocol.zig");
const request = @import("../http/request.zig");
pub const EndpointAddress = std.Io.net.IpAddress;

/// Whom an endpoint's connections are charged to (`accounts.zig`).
///
/// - `.peer`, the default on TCP endpoints: the socket peer. IPv4 per
///   address; IPv6 by its /64, plus an aggregate account on
///   `Limits.client_ipv6_aggregate_prefix_bits` (/48 by default). An
///   IPv4-mapped address is IPv4. A peer whose address cannot be read is the
///   one shared unknown identity.
/// - `.proxy_source`: the client a PROXY v2 header names, for a proxy that
///   keeps one upstream connection per client (an NLB, HAProxy or nginx
///   `stream`, Envoy TCP). Needs the endpoint's `proxy_protocol` on. A
///   `LOCAL` header (a health check) is charged to the endpoint's one
///   infrastructure account.
/// - `.none`, the default on Unix socket endpoints: no client account, only
///   the connection's and the server's. The only honest setting behind an L7
///   proxy that pools connections (an ALB, Cloudflare HTTP), with or without
///   PROXY: one pooled upstream connection carries many users.
///
/// Every endpoint's identity and framing are logged at boot. No
/// X-Forwarded-For: identity is needed at accept, before any HTTP.
///
/// The failure mode of the default: `.peer` left on behind an L7 proxy that
/// sends no PROXY header makes every user one identity, and nothing at boot
/// can see it. Behind one that does send it, `.peer` is a boot error
/// (`PeerIdentityBehindProxyProtocol`).
pub const ClientIdentity = enum { peer, proxy_source, none };

/// A Unix socket peer has no address: its identity is the PROXY source or
/// none.
pub const UnixClientIdentity = enum {
    none,
    proxy_source,

    fn general(self: UnixClientIdentity) ClientIdentity {
        return switch (self) {
            .none => .none,
            .proxy_source => .proxy_source,
        };
    }
};

/// PROXY protocol v2 framing on a TCP endpoint (`proxy_protocol.zig` has the
/// protocol and the order at accept): every connection starts with a header,
/// which the server reads and strips, whatever the identity. Only from a
/// non-empty list of trusted proxy sources (`Server.init` fails with
/// `ProxyProtocolUntrusted` otherwise); a connection from any other source
/// is closed at accept. The connections waiting for their header are bounded
/// by `Limits.proxy_header_waiters`, split into a floor per PROXY endpoint and
/// a shared pool (`Server.proxy_floor`), and each has
/// `Limits.proxy_header_timeout_ns` from its accept to deliver the header.
pub const ProxyProtocol = union(enum) {
    off,
    v2: struct {
        /// Proxy source ranges allowed to send a header. Copied at init.
        trusted: []const proxy_protocol.Cidr,
    },
};

/// PROXY v2 framing on a Unix socket endpoint: no trusted list, since only
/// the host reaches the socket.
pub const UnixProxyProtocol = enum { off, v2 };

pub const EndpointConfig = union(enum) {
    tls: Tcp,
    h2c_prior_knowledge: Tcp,
    h1c: Tcp,
    /// TLS or h2c on a Unix stream socket at this path: for a proxy on the
    /// same host, and the transport the tests drive, whose fixed kernel
    /// buffers hold a stall (TCP loopback's do not on macOS). The server
    /// copies the path, binds it, and deletes the file when its listeners
    /// close. A file already there is not removed: the bind fails. At most
    /// `std.Io.net.UnixAddress.max_len` bytes (104 on macOS).
    tls_unix: Unix,
    h2c_unix: Unix,

    pub const Tcp = struct {
        address: EndpointAddress,
        client_identity: ClientIdentity = .peer,
        proxy_protocol: ProxyProtocol = .off,
    };

    pub const Unix = struct {
        path: []const u8,
        client_identity: UnixClientIdentity = .none,
        proxy_protocol: UnixProxyProtocol = .off,
    };

    pub fn mode(ep: EndpointConfig) connection.Mode {
        return switch (ep) {
            .tls, .tls_unix => .tls,
            .h2c_prior_knowledge, .h2c_unix => .h2c,
            .h1c => .h1c,
        };
    }

    fn unixPath(ep: EndpointConfig) ?[]const u8 {
        return switch (ep) {
            .tls_unix, .h2c_unix => |u| u.path,
            else => null,
        };
    }

    fn clientIdentity(ep: EndpointConfig) ClientIdentity {
        return switch (ep) {
            .tls, .h2c_prior_knowledge, .h1c => |t| t.client_identity,
            .tls_unix, .h2c_unix => |u| u.client_identity.general(),
        };
    }

    /// PROXY v2 framing on: the header is read and stripped.
    fn proxied(ep: EndpointConfig) bool {
        return switch (ep) {
            .tls, .h2c_prior_knowledge, .h1c => |t| t.proxy_protocol == .v2,
            .tls_unix, .h2c_unix => |u| u.proxy_protocol == .v2,
        };
    }

    /// The sources allowed to send a PROXY header; empty when framing is off
    /// and on a Unix socket.
    fn trusted(ep: EndpointConfig) []const proxy_protocol.Cidr {
        return switch (ep) {
            .tls, .h2c_prior_knowledge, .h1c => |t| switch (t.proxy_protocol) {
                .off => &.{},
                .v2 => |v| v.trusted,
            },
            .tls_unix, .h2c_unix => &.{},
        };
    }
};

pub const TlsConfig = struct {
    certificate_chain_pem: []const u8,
    private_key_pem: []const u8,
};

/// Test-only: TLS handshakes that ended at their own timeout, as opposed to
/// a peer that broke the handshake. Counted in Debug and `-Dobserve=true`.
pub var test_handshake_timeouts: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};

/// Test-only: times an accept loop parked because the connection or TLS
/// handshake limit was full. Counted in Debug and `-Dobserve=true`.
/// Test-only: the accept loop of this endpoint index fails to spawn once
/// `test_accept_spawn_gate` opens. A spawn failure cannot be caused from
/// outside (zio refuses a spawn only for no memory or a runtime going down),
/// and the exit it takes must be tested with a handler already running on an
/// earlier endpoint.
pub var test_fail_accept_spawn: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(std.math.maxInt(usize)) else {};
pub var test_accept_spawn_gate: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};

fn testFailAcceptSpawn(i: usize) bool {
    if (test_fail_accept_spawn.load(.acquire) != i) return false;
    while (!test_accept_spawn_gate.load(.acquire)) zio.sleep(.fromMilliseconds(1)) catch {};
    return true;
}

pub var test_admission_waits: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};

/// Test-only: accepted connections whose `admitAccepted` found a slot gone
/// (another endpoint's accept loop took it between this loop's room check
/// and its admission), so the charge it had taken was undone. Counted in
/// Debug and `-Dobserve=true`, so a race test can prove it ran the undo.
pub var test_admit_refused: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};

pub const InitError = error{
    OutOfMemory,
    InvalidConfig,
    /// The process cannot have the descriptors `max_connections` needs
    /// (`checkFileLimit`).
    FileLimitTooLow,
    InvalidRoute,
    InvalidCertificate,
    CertificateTooLarge,
    PrivateKeyTooLarge,
    /// A TCP endpoint with `proxy_protocol = .v2` and no trusted sources: any
    /// client could then name any address.
    ProxyProtocolUntrusted,
    /// A trusted PROXY source range that holds every IPv4-mapped address
    /// (`::ffff:0:0/96`) without being a mapped range itself, such as
    /// `::/0`: it would trust IPv4 sources it does not name. The log line
    /// names the range; list the IPv4 range or the mapped range instead.
    ProxyTrustedRangeHoldsIpv4,
    /// `client_identity = .proxy_source` with `proxy_protocol` off: there is
    /// no header to name the source.
    ProxySourceWithoutProxyProtocol,
    /// `client_identity = .peer` with `proxy_protocol = .v2` on TCP: the
    /// socket peer is then the trusted proxy, so every client would be one
    /// identity. Use `.proxy_source`, or `.none` behind a pooling proxy.
    PeerIdentityBehindProxyProtocol,
    /// More PROXY endpoints than `Limits.proxy_header_waiters`: each needs a
    /// floor of at least one header waiter of its own.
    ProxyWaitersBelowEndpoints,
    /// A route's share override (`router.Route.share`) above the server's
    /// limit of that resource. The log line names the route.
    RouteShareAboveServerLimit,
    /// A share override on a complete route, which has no route account.
    RouteShareOnCompleteRoute,
    /// A route's share override of 0: it would refuse the route everything
    /// whenever the server is at its pressure mark. The log line names it.
    RouteShareZero,
};

/// Which PROXY header waiter slot a connection holds (`Server.proxy_floor`).
pub const ProxySlot = enum { floor, shared };

/// A PROXY v2 connection on its way to its header: its waiter slot, and when
/// it was accepted, on the awake clock (both its deadlines count from there).
const ProxyWait = struct {
    slot: ProxySlot,
    accepted_ns: u64,
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
    routes: []const router_mod.Route,
    tls: ?TlsConfig,
    limits: limits_mod.Limits = .defaults,
    /// Load-aware connection placement (pinned scheduling). The balancer
    /// belongs to the runtime this server runs on and may be shared by every
    /// server on it; it must outlive the server. See `balancer.zig`.
    balancer: ?*connection.Balancer = null,
};

/// Observable progress of `serve` startup.
///
/// `serve` binds INSIDE its own spawned task, so a caller holding only a join
/// handle cannot tell "listening" from "about to fail". Before this state
/// existed, every caller papered over the gap with a sleep, and
/// `localAddress` read uninitialized storage until the bind landed. An
/// embedder that switches routes on a successful start would then report
/// success while the listener was gone.
///
/// `.listening` is published only after EVERY endpoint is bound and its accept
/// loop is spawned, so a released waiter may connect immediately. An errdefer
/// publishes `.failed` on all three startup error paths, so a waiter learns of
/// a failure instead of waiting out its timeout.
pub const BindState = enum(u8) {
    pending,
    listening,
    failed,
};

pub const Server = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    limits: limits_mod.Limits,
    routes: []router_mod.Route,
    endpoints: []EndpointConfig,
    listeners: []std.Io.net.Server,
    local_addrs: []EndpointAddress,
    tls_acceptor: ?tls_edge.Acceptor = null,
    shutdown_flag: std.atomic.Value(bool) = .init(false),
    shutdown_event: zio.Event = .init,
    active_connections: std.atomic.Value(usize) = .init(0),
    accounting: connection.GlobalAccounting,
    /// The client accounts (`accounts.zig`): one record per client identity,
    /// and per IPv6 aggregate, that holds anything.
    clients: accounts.Table,
    /// Connections on PROXY v2 endpoints accepted and not yet past their
    /// header (and, on TLS, not yet holding a handshake slot). One ceiling
    /// for the server, `Limits.proxy_header_waiters` (W), split at boot over
    /// the N PROXY endpoints: each has a floor of its own, `proxy_floor` =
    /// max(1, W / 2N), and the rest, `proxy_shared`, is one pool any of them
    /// takes from once its floor is full. No endpoint takes another's floor.
    /// While an endpoint's floor and the pool are both full its accept loop
    /// parks (no close).
    ///
    /// Why floors: a TLS PROXY connection keeps its waiter slot until a
    /// handshake slot is free, so handshake load on one PROXY endpoint fills
    /// any one shared cap and parks every other PROXY endpoint. A floor
    /// bounds what one endpoint's stall takes from the others; the pool
    /// absorbs one endpoint's burst. A stalled trusted proxy can still fill
    /// its own floor and the pool; the log names it.
    proxy_floor: usize = 0,
    proxy_shared: usize = 0,
    /// Floor slots in use, by endpoint index (non-PROXY endpoints stay 0).
    proxy_floor_used: [accounts.Table.max_labels]std.atomic.Value(usize) = @splat(.init(0)),
    proxy_shared_used: std.atomic.Value(usize) = .init(0),
    /// Connections closed at accept because their source is not on the
    /// endpoint's trusted list.
    proxy_untrusted: std.atomic.Value(u64) = .init(0),
    /// PROXY headers that were malformed, over `Limits.proxy_header_bytes`,
    /// or not complete by the preface deadline; each connection was closed.
    proxy_header_failures: std.atomic.Value(u64) = .init(0),
    proxy_log: RateLimitedLog = .{},
    router: router_mod.Router = undefined,
    /// Server-wide outbound DATA slabs. Shared so an idle connection holds
    /// none, and capped so the ceiling stays computable.
    slabs: slab_pool.SlabPool,
    /// Server-wide request memory: every request's header list, body and
    /// handler allocations. See `request_pool.zig`. Connections hold a pointer
    /// to it, so the server must not move once `serve` runs.
    requests: request_pool.RequestPool,
    /// Server-wide brotli encoder contexts. Present only when
    /// `limits.response_compression` is on; handlers borrow through ConnConfig.
    compression_pool: ?brotli.Pool = null,
    balancer: ?*connection.Balancer = null,
    /// Connection slots: the boot memory h2c and TLS connections reuse. See
    /// `conn_slot.zig`.
    slots: conn_slot.SlotPool,
    listeners_bound: usize = 0,
    /// Accept calls that failed with something other than a cancel or a
    /// connection the peer aborted, over every endpoint (`acceptLoop`).
    accept_errors: std.atomic.Value(u64) = .init(0),
    /// Shutdown reports `serve` has written (`waitForHandlers`), and the task
    /// handlers the last one counted. A test reads them; an operator reads
    /// the log.
    shutdown_reports: std.atomic.Value(u32) = .init(0),
    /// True from the start of `serve` until it returns. `deinit` checks it
    /// as well as the counters: a serving server with nothing charged still
    /// has accept loops and listeners in use.
    serving: std.atomic.Value(bool) = .init(false),
    shutdown_report_handlers: std.atomic.Value(usize) = .init(0),
    /// Report 1's count of task handlers the connections asked to cancel.
    shutdown_report_asked: std.atomic.Value(usize) = .init(0),
    bind_state: std.atomic.Value(BindState) = .init(.pending),
    bind_event: zio.Event = .init,

    /// Validate everything, then own everything.
    ///
    /// The memory bound is computed FIRST, before any allocation. A
    /// configuration whose limits are inconsistent or would overflow must fail
    /// at boot, where an operator sees it, and not under load.
    ///
    /// Routes and endpoints are duplicated rather than borrowed. The server
    /// outlives its caller's configuration, and a route path that a handler
    /// matches against must not be able to disappear.
    pub fn init(gpa: std.mem.Allocator, io: std.Io, config: ServerConfig) InitError!Server {
        try checkFileLimit(config.limits.max_connections);
        _ = try config.limits.resourceUpperBound();
        if (config.endpoints.len == 0 or config.endpoints.len > config.limits.max_endpoints) return error.InvalidConfig;
        if (config.routes.len > config.limits.max_routes) return error.InvalidConfig;
        // `router.Found.route` is a u32.
        if (config.routes.len > std.math.maxInt(u32) - 1) return error.InvalidConfig;

        var need_tls = false;
        var need_h2c = false;
        for (config.endpoints) |ep| {
            if (ep.mode() == .tls) need_tls = true;
            if (ep.mode() == .h2c) need_h2c = true;
            if (ep.unixPath()) |path| _ = std.Io.net.UnixAddress.init(path) catch return error.InvalidConfig;
        }
        if (need_h2c) try config.limits.checkH2cWriteQueue();
        if (need_tls and config.tls == null) return error.InvalidConfig;

        // The PROXY header waiters: a floor per PROXY endpoint, and the rest
        // shared (`Server.proxy_floor`).
        var proxy_endpoints: usize = 0;
        for (config.endpoints) |ep| proxy_endpoints += @intFromBool(ep.proxied());
        if (proxy_endpoints > config.limits.proxy_header_waiters) return error.ProxyWaitersBelowEndpoints;
        const proxy_floor: usize = if (proxy_endpoints == 0) 0 else @max(1, config.limits.proxy_header_waiters / (2 * proxy_endpoints));
        const proxy_shared: usize = config.limits.proxy_header_waiters - proxy_endpoints * proxy_floor;

        for (config.endpoints) |ep| {
            const unix = ep.unixPath() != null;
            if (ep.proxied() and !unix and ep.trusted().len == 0) return error.ProxyProtocolUntrusted;
            switch (ep.clientIdentity()) {
                .proxy_source => if (!ep.proxied()) return error.ProxySourceWithoutProxyProtocol,
                .peer => if (ep.proxied()) return error.PeerIdentityBehindProxyProtocol,
                .none => {},
            }
        }

        var route_bytes: usize = 0;
        for (config.routes) |route| route_bytes += route.path.len;
        if (route_bytes > config.limits.max_route_path_bytes) return error.InvalidRoute;

        const routes = try gpa.alloc(router_mod.Route, config.routes.len);
        errdefer {
            for (routes) |route| {
                if (route.path.len != 0) gpa.free(route.path);
            }
            gpa.free(routes);
        }
        for (routes) |*route| route.* = .{ .method = .GET, .path = &.{}, .handler = undefined };
        for (config.routes, 0..) |route, i| {
            routes[i] = .{
                .method = route.method,
                .path = try gpa.dupe(u8, route.path),
                .prefix = route.prefix,
                .handler = route.handler,
                .max_request_body_bytes = route.max_request_body_bytes,
                .share = route.share,
            };
        }
        // The route accounts, from the same list the router matches against,
        // so a route index is always a slot of the table.
        var route_accounts = try accounts.RouteTable.init(gpa, routes, config.limits.routeLimits());
        errdefer route_accounts.deinit(gpa);

        const endpoints = try gpa.dupe(EndpointConfig, config.endpoints);
        for (endpoints) |*ep| switch (ep.*) {
            .tls_unix, .h2c_unix => |*u| u.path = &.{},
            .tls, .h2c_prior_knowledge, .h1c => |*t| switch (t.proxy_protocol) {
                .off => {},
                .v2 => |*v| v.trusted = &.{},
            },
        };
        errdefer freeEndpoints(gpa, endpoints);
        // The paths and the trusted lists are the caller's: own a copy.
        for (config.endpoints, endpoints, 0..) |src, *dst, i| switch (dst.*) {
            .tls_unix, .h2c_unix => |*d| d.path = try gpa.dupe(u8, src.unixPath().?),
            .tls, .h2c_prior_knowledge, .h1c => |*d| switch (d.proxy_protocol) {
                .off => {},
                .v2 => |*v| v.trusted = try ownTrusted(gpa, i, src.trusted()),
            },
        };
        for (endpoints, 0..) |ep, i| {
            const framing: []const u8 = if (!ep.proxied()) "off" else if (ep.unixPath() != null) "v2 (Unix socket, no trusted list)" else "v2";
            log.info("endpoint {d} ({s}): client_identity=.{s} proxy_protocol={s} trusted={d}", .{ i, @tagName(ep), @tagName(ep.clientIdentity()), framing, ep.trusted().len });
        }
        if (proxy_endpoints != 0) log.info("PROXY header waiters: {d}, a floor of {d} for each of {d} PROXY endpoints and {d} shared; header deadline {d} ms", .{ config.limits.proxy_header_waiters, proxy_floor, proxy_endpoints, proxy_shared, config.limits.proxy_header_timeout_ns / std.time.ns_per_ms });

        var tls_acceptor: ?tls_edge.Acceptor = null;
        errdefer if (tls_acceptor) |*auth| auth.deinit();
        if (config.tls) |tls_config| {
            if (tls_config.certificate_chain_pem.len > config.limits.certificate_chain_bytes) return error.CertificateTooLarge;
            if (tls_config.private_key_pem.len > config.limits.private_key_bytes) return error.PrivateKeyTooLarge;
            tls_acceptor = tls_edge.Acceptor.initFromPem(
                gpa,
                io,
                tls_config.certificate_chain_pem,
                tls_config.private_key_pem,
            ) catch return error.InvalidCertificate;
        }

        const listeners = try gpa.alloc(std.Io.net.Server, config.endpoints.len);
        errdefer gpa.free(listeners);
        const local_addrs = try gpa.alloc(EndpointAddress, config.endpoints.len);
        errdefer gpa.free(local_addrs);

        var slabs = slab_pool.SlabPool.init(
            gpa,
            io,
            config.limits.outbound_bytes_per_stream,
            config.limits.outbound_slabs_per_server,
        ) catch return error.OutOfMemory;
        errdefer slabs.deinit(gpa);

        // Reserves the pool's address space now, so a mapping the OS refuses
        // is a boot error here and never a request error later.
        var requests = request_pool.RequestPool.init(config.limits.requestPoolConfig()) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidConfig => return error.InvalidConfig,
        };
        errdefer requests.deinit();

        var compression_pool: ?brotli.Pool = null;
        if (config.limits.response_compression) {
            compression_pool = brotli.Pool.init(
                gpa,
                config.limits.compression_contexts_per_server,
                config.limits.compression_context_bytes,
                config.limits.compression_quality,
                config.limits.compression_window_bits,
            );
        }

        // One slot size for the whole server: the largest any endpoint needs.
        // A server with only h1c endpoints makes no slots.
        var slot_boot_bytes: usize = 0;
        for (config.endpoints) |ep| {
            slot_boot_bytes = @max(slot_boot_bytes, config.limits.slotBootBytes(ep.mode()) catch return error.InvalidConfig);
        }
        var slots = conn_slot.SlotPool.init(gpa, config.limits.max_connections, slot_boot_bytes) catch return error.OutOfMemory;
        errdefer slots.deinit();

        // Keyed at boot, so a client cannot aim keys at one probe chain.
        var seed: u64 = undefined;
        zio.random(std.mem.asBytes(&seed));
        var clients = try accounts.Table.init(gpa, config.limits.clientTableRecords() catch return error.InvalidConfig, seed, config.limits.accountRules());
        errdefer clients.deinit(gpa);
        if (endpoints.len > accounts.Table.max_labels) return error.InvalidConfig;
        for (endpoints, 0..) |ep, i| {
            clients.setEndpointLabel(i, "endpoint {d} ({s}) client_identity=.{s}", .{ i, @tagName(ep), @tagName(ep.clientIdentity()) });
        }

        var self: Server = .{
            .gpa = gpa,
            .io = io,
            .limits = config.limits,
            .routes = routes,
            .endpoints = endpoints,
            .listeners = listeners,
            .local_addrs = local_addrs,
            .tls_acceptor = tls_acceptor,
            .accounting = .{
                .max_streams = config.limits.max_streams_per_server,
                .task_handler_capacity = config.limits.max_task_handlers,
                .max_outbound_bytes = config.limits.outbound_bytes_per_server,
                .max_handshakes = config.limits.concurrent_tls_handshakes,
                .routes = route_accounts,
            },
            .clients = clients,
            .slabs = slabs,
            .requests = requests,
            .compression_pool = compression_pool,
            .balancer = config.balancer,
            .slots = slots,
            .proxy_floor = proxy_floor,
            .proxy_shared = proxy_shared,
        };
        self.router = .{ .routes = self.routes };
        return self;
    }

    fn closeListeners(self: *Server) void {
        var i: usize = 0;
        while (i < self.listeners_bound) : (i += 1) {
            self.listeners[i].socket.close(self.io);
            // The socket file this server bound.
            if (self.endpoints[i].unixPath()) |path| std.Io.Dir.cwd().deleteFile(self.io, path) catch {};
        }
        self.listeners_bound = 0;
    }

    pub fn serve(self: *Server, gpa: std.mem.Allocator) ServeError!void {
        std.debug.assert(gpa.vtable == self.gpa.vtable);
        self.serving.store(true, .release);
        // The last thing `serve` does, after every wait below.
        defer self.serving.store(false, .release);
        self.listeners_bound = 0;
        errdefer {
            self.closeListeners();
            self.bind_state.store(.failed, .release);
            self.bind_event.set();
        }

        for (self.endpoints, 0..) |endpoint, i| {
            self.listeners[i] = switch (endpoint) {
                .tls, .h2c_prior_knowledge, .h1c => |t| listenEndpoint(self.io, t.address, self.limits.listen_backlog),
                .tls_unix, .h2c_unix => |u| listenUnix(self.io, u.path, self.limits.listen_backlog),
            } catch return error.ListenFailed;
            self.local_addrs[i] = self.listeners[i].socket.address;
            self.listeners_bound = i + 1;
        }

        var accept_group: zio.Group = .init;
        var connection_group: zio.Group = .init;

        // Spawned now, while spawning works (`CancelSignal`). No connection
        // exists yet, so its own failure needs no wait.
        var cancel: CancelSignal = .{};
        var canceller = zio.spawn(cancelOnSignal, .{ &connection_group, &cancel }) catch return error.TransportSetupFailed;

        for (self.endpoints, 0..) |endpoint, i| {
            const spawned = if (comptime test_hook.on) testFailAcceptSpawn(i) else false;
            const failed = spawned or blk: {
                accept_group.spawn(acceptLoop, .{ self, i, endpoint.mode(), &connection_group }) catch break :blk true;
                break :blk false;
            };
            if (failed) {
                // An earlier endpoint may have taken connections, and their
                // handlers may run: this exit waits for them like every
                // other (`waitForHandlers`), so `serve` never returns under
                // a running handler.
                accept_group.cancel();
                zio.beginShield();
                defer zio.endShield();
                self.waitForHandlers(&connection_group, true, &cancel, &canceller);
                return error.TransportSetupFailed;
            }
        }

        self.bind_state.store(.listening, .release);
        self.bind_event.set();

        const canceled = blk: {
            self.shutdown_event.wait() catch break :blk true;
            break :blk false;
        };
        self.shutdown_flag.store(true, .release);
        self.shutdown_event.set();
        accept_group.cancel();
        self.closeListeners();

        // From here on `serve` waits for every connection and every handler,
        // and a cancel of its own task does not cut that short: it stays
        // pending, and `serve` reports it (RuntimeShutdown) once it returns.
        zio.beginShield();
        defer zio.endShield();
        self.waitForHandlers(&connection_group, canceled, &cancel, &canceller);
        if (canceled) return error.RuntimeShutdown;
    }

    /// The shutdown contract: `serve` never returns while a handler still
    /// runs, because a handler still reaches the request pool, the slabs,
    /// this struct's accounting and the balancer, which `deinit` frees.
    ///
    /// 1. The drain. Every connection saw the shutdown: HTTP/2 sends GOAWAY
    ///    and drains its streams (a first phase of up to 1 s, then
    ///    `graceful_drain_timeout_ns`), HTTP/1.1 cancels its handler at once.
    /// 2. The cancel. Each connection reaches its cancel step (HTTP/2 when its
    ///    drain ends, HTTP/1.1 at once) and counts itself, with the handlers
    ///    it asks to cancel (`GlobalAccounting.shutdown_cancelled`,
    ///    `shutdown_cancel_asked`). When every live connection has, report
    ///    right then: how many were asked, what still runs, how long the
    ///    drain ran and its deadline, because a supervisor's kill can come
    ///    before the next step. A connection that never reaches its step (an
    ///    actor stuck) does not hold the report back past the drain deadline
    ///    and a second; the report then says how many are missing. A
    ///    canceled `serve` (or a startup failure) skips the drain and cancels
    ///    the connections at once, from the task spawned at startup
    ///    (`CancelSignal`), so this keeps reporting while they end.
    /// 3. Wait `shutdown_cancel_grace_ns`. If anything still runs, report
    ///    again, then keep waiting and report every
    ///    `shutdown_report_interval_ns`. There is no limit.
    ///
    /// Every wait polls the counters, from 1 ms doubling to 50 ms: a
    /// handler's last act is its decrement (`dropLinkRef`), and a wake after
    /// it would touch this struct after `deinit` may have run. The polls run
    /// under the caller's shield, so a cancel of `serve` neither ends the wait
    /// nor makes it spin (a delivered error.Canceled would come back from
    /// every sleep; zio's `checkCancel` delivers none under a shield).
    ///
    /// Reports are `warn` lines (`log`), counted in `shutdown_reports`. The
    /// cancel reports name the task routes that still hold handlers, with
    /// their counts (`accounts.RouteTable.writeHeld`); a 404 or 405 handler
    /// has no route, so a total can be more than the routes' sum.
    ///
    /// With one executor and a handler that never yields, this task never
    /// runs again and nothing is reported; only a watchdog on an OS thread
    /// could (design: the executor watchdog).
    fn waitForHandlers(self: *Server, connection_group: *zio.Group, canceled: bool, cancel: *CancelSignal, canceller: *zio.JoinHandle(void)) void {
        const t0 = monoNs();
        // A canceled `serve` (or a startup failure) cancels the connections
        // at once, through the task spawned at startup, so this keeps
        // reporting while they end.
        if (canceled) cancel.fire(true);
        const drain_ns: u64 = if (canceled) 0 else std.time.ns_per_s +| self.limits.graceful_drain_timeout_ns;
        // Report 1 comes when the cancel does: every live connection has
        // reached its cancel step (`GlobalAccounting.shutdown_cancelled`), or
        // the cancel was the group's. The drain deadline is the backstop for
        // a connection that cannot reach it (its actor stuck).
        // The connections' own deadlines run from when each saw the shutdown,
        // a little after `t0`: the backstop waits a second more.
        const backstop_ns: u64 = if (canceled) 0 else drain_ns +| std.time.ns_per_s;
        const how = self.waitCancelTransition(t0 +| backstop_ns, canceled, canceller);
        if (how != .quiet) {
            self.reportShutdown(if (canceled) .cancel_group else if (how == .asked) .cancel else .cancel_backstop, t0, drain_ns);
            var deadline = monoNs() +| self.limits.shutdown_cancel_grace_ns;
            var phase: ReportPhase = .grace;
            while (!self.waitQuiet(deadline, canceled, canceller)) {
                self.reportShutdown(phase, t0, drain_ns);
                phase = .interval;
                deadline = monoNs() +| self.limits.shutdown_report_interval_ns;
            }
        }
        // Not canceled: the cancelling task has nothing to do.
        cancel.fire(false);
        canceller.join();
        // Every connection task has ended: this returns at once.
        connection_group.wait() catch {};
    }

    /// The cancelling task's signal. The task is started with `serve`, while
    /// a spawn cannot fail for the runtime going down (zio refuses every
    /// spawn once `shutting_down` is set: `runtime.zig` `getNextExecutor`),
    /// and is told at the end whether to cancel the connections. A cancel of
    /// a group waits for its tasks (`Group.cancel`), so it runs there and not
    /// on `serve`, which keeps reporting meanwhile.
    const CancelSignal = struct {
        event: zio.Event = .init,
        cancel: std.atomic.Value(bool) = .init(false),
        fired: bool = false,

        fn fire(self: *CancelSignal, cancel_group: bool) void {
            if (self.fired) return;
            self.fired = true;
            self.cancel.store(cancel_group, .release);
            self.event.set();
        }
    };

    fn cancelOnSignal(group: *zio.Group, signal: *CancelSignal) void {
        zio.beginShield();
        defer zio.endShield();
        signal.event.wait() catch {};
        if (signal.cancel.load(.acquire)) group.cancel();
    }

    const Transition = enum { quiet, asked, backstop };

    /// Wait until nothing is left, or every handler still running has been
    /// asked to cancel (every live connection is past its cancel step, or
    /// only orphans are left, or the group cancel was requested), or
    /// `backstop_ns`.
    fn waitCancelTransition(self: *Server, backstop_ns: u64, canceled: bool, canceller: *zio.JoinHandle(void)) Transition {
        var pause_ms: u64 = 1;
        while (true) {
            // Checked before `quiet`: handlers that were asked to cancel and
            // ended at once are still the cancel this report is about.
            const asked = self.accounting.shutdown_cancel_asked.load(.acquire);
            const handlers = self.accounting.task_handlers.load(.acquire) + self.accounting.orphaned_handlers.load(.acquire);
            if (asked != 0 or handlers != 0) {
                if (canceled) return .asked;
                const live = self.active_connections.load(.acquire);
                if (self.accounting.shutdown_cancelled.load(.acquire) >= live) return .asked;
            }
            if (self.quiet() and (!canceled or canceller.hasResult())) return .quiet;
            const now = monoNs();
            if (now >= backstop_ns) return .backstop;
            zio.sleep(.fromNanoseconds(@min(backstop_ns - now, pause_ms * std.time.ns_per_ms))) catch {};
            pause_ms = @min(pause_ms * 2, 50);
        }
    }

    /// No connection, no task handler (either protocol, orphans included),
    /// and a group cancel, if one was asked, done; or
    /// `deadline_ns`. True when quiet. `orphaned_handlers` is the last thing
    /// an orphan releases, so reading it zero means that orphan is done with
    /// this struct.
    fn waitQuiet(self: *Server, deadline_ns: u64, canceled: bool, canceller: *zio.JoinHandle(void)) bool {
        var pause_ms: u64 = 1;
        while (true) {
            if (self.quiet() and (!canceled or canceller.hasResult())) return true;
            const now = monoNs();
            if (now >= deadline_ns) return false;
            zio.sleep(.fromNanoseconds(@min(deadline_ns - now, pause_ms * std.time.ns_per_ms))) catch {};
            pause_ms = @min(pause_ms * 2, 50);
        }
    }

    fn quiet(self: *Server) bool {
        return self.active_connections.load(.acquire) == 0 and
            self.accounting.orphaned_handlers.load(.acquire) == 0 and
            self.accounting.task_handlers.load(.acquire) == 0;
    }

    const ReportPhase = enum { cancel, cancel_group, cancel_backstop, grace, interval };

    fn reportShutdown(self: *Server, phase: ReportPhase, t0: u64, drain_ns: u64) void {
        const n = self.shutdown_reports.fetchAdd(1, .acq_rel) + 1;
        const handlers = self.accounting.task_handlers.load(.acquire);
        const orphans = self.accounting.orphaned_handlers.load(.acquire);
        const conns = self.active_connections.load(.acquire);
        self.shutdown_report_handlers.store(handlers, .release);
        const asked = self.accounting.shutdown_cancel_asked.load(.acquire);
        if (n == 1) self.shutdown_report_asked.store(asked, .release);
        const since_ms = (monoNs() -| t0) / std.time.ns_per_ms;
        var route_buf: [512]u8 = undefined;
        var rw: std.Io.Writer = .fixed(&route_buf);
        self.accounting.routes.writeHeld(&rw);
        const per_route = rw.buffered();
        switch (phase) {
            .cancel => log.warn("shutdown report {d}: after {d} ms (drain deadline {d} ms) every connection has asked its handlers to cancel, {d} task handlers in all; still running: {d} ({d} of them handlers whose connection ended), {d} connections; per task route: {s}", .{ n, since_ms, drain_ns / std.time.ns_per_ms, asked, handlers, orphans, conns, per_route }),
            .cancel_group => log.warn("shutdown report {d}: serve was canceled or failed to start, and every connection is being cancelled now; still running: {d} task handlers ({d} of them handlers whose connection ended), {d} connections; per task route: {s}", .{ n, handlers, orphans, conns, per_route }),
            .cancel_backstop => log.warn("shutdown report {d}: the drain deadline passed ({d} ms) with {d} of {d} connections not yet at their cancel step, so not every handler has been asked to cancel; still running: {d} task handlers ({d} of them handlers whose connection ended); per task route: {s}", .{ n, since_ms, conns -| self.accounting.shutdown_cancelled.load(.acquire), conns, handlers, orphans, per_route }),
            .grace => log.warn("shutdown report {d}: {d} task handlers ({d} orphaned), {d} connections still running {d} ms after shutdown, past the cancel grace of {d} ms; serve keeps waiting and reports every {d} ms", .{ n, handlers, orphans, conns, since_ms, self.limits.shutdown_cancel_grace_ns / std.time.ns_per_ms, self.limits.shutdown_report_interval_ns / std.time.ns_per_ms }),
            .interval => log.warn("shutdown report {d}: still waiting {d} ms after shutdown for {d} task handlers ({d} orphaned), {d} connections", .{ n, since_ms, handlers, orphans, conns }),
        }
    }

    /// A connection start for the client table's buckets (`accounts.Start`):
    /// now, and the server's connections and (TLS) handshakes in progress
    /// before this connection. `own` is how many of the connection count are
    /// this connection's already (a PROXY connection is counted at accept).
    fn startNow(self: *Server, mode: connection.Mode, own: usize) accounts.Start {
        return .{
            .now_ns = monoNs(),
            .connections = self.active_connections.load(.seq_cst) -| own,
            .handshakes = if (mode == .tls) self.accounting.active_handshakes.load(.seq_cst) else null,
        };
    }

    /// The connection's charge: its client accounts (`c`), then the server.
    /// `.client_refused` when the client is over its share under pressure
    /// (`accounts.Charge.add`): the caller closes the connection.
    fn tryAdmitConnection(self: *Server, c: accounts.Charge) connection.Admit {
        if (c.add(.connections, 1, self.active_connections.load(.seq_cst)) != .took) return .client_refused;
        while (true) {
            const current = self.active_connections.load(.seq_cst);
            if (current >= self.limits.max_connections) {
                c.sub(.connections, 1);
                return .server_full;
            }
            if (self.active_connections.cmpxchgWeak(current, current + 1, .seq_cst, .seq_cst) == null) return .ok;
        }
    }

    fn releaseConnection(self: *Server, c: accounts.Charge) void {
        self.giveBackConnection(c);
        self.accounting.wakeAdmission();
    }

    /// `releaseConnection` without the wake, for an undo made while holding
    /// `admission_mu` (`admitAccepted` inside `awaitAdmission`'s loop):
    /// `wakeAdmission` takes that mutex, which is not recursive, so the
    /// accept loop would wait on itself for ever, uncancelably.
    ///
    /// The client's count goes back before the server's, the reverse of the
    /// other releases. An accept loop admits only once it sees the server's
    /// slot free (`admissionRoom`), so by then the client's count must
    /// already be down: the other way round, a client that reconnects at
    /// once is refused by its share for the connection it just ended.
    fn giveBackConnection(self: *Server, c: accounts.Charge) void {
        c.sub(.connections, 1);
        self.accounting.saturatingRelease(&self.active_connections, 1, "active_connections", .seq_cst);
    }

    /// A header waiter slot for a connection on PROXY endpoint `i`: its own
    /// floor first, then the shared pool. Null when both are full.
    fn tryAdmitProxyWaiter(self: *Server, i: usize) ?ProxySlot {
        if (boundedIncrement(&self.proxy_floor_used[i], self.proxy_floor)) return .floor;
        if (boundedIncrement(&self.proxy_shared_used, self.proxy_shared)) return .shared;
        return null;
    }

    fn releaseProxyWaiter(self: *Server, i: usize, slot: ProxySlot) void {
        switch (slot) {
            .floor => self.accounting.saturatingRelease(&self.proxy_floor_used[i], 1, "proxy_floor_used", .seq_cst),
            .shared => self.accounting.saturatingRelease(&self.proxy_shared_used, 1, "proxy_shared_used", .seq_cst),
        }
        self.accounting.wakeAdmission();
    }

    /// Every PROXY header waiter slot in use, floors and pool.
    pub fn proxyWaiters(self: *const Server) usize {
        var n = self.proxy_shared_used.load(.acquire);
        for (&self.proxy_floor_used) |*f| n += f.load(.acquire);
        return n;
    }

    pub fn proxyFloorUsed(self: *const Server, i: usize) usize {
        return self.proxy_floor_used[i].load(.acquire);
    }

    /// Give back what `admitAccepted` took before it was refused. Outside
    /// `admission_mu` it wakes the parked loops: one may have checked while
    /// the slot was taken for a moment, and parked. Under the mutex it must
    /// not (see `giveBackConnection`), and need not: every parked loop
    /// re-checks under the mutex before it waits, so none can have checked
    /// between this take and this undo.
    fn undoConnection(self: *Server, c: accounts.Charge, locked: bool) void {
        if (locked) self.giveBackConnection(c) else self.releaseConnection(c);
    }

    /// What one accept must take: the connection slot, and the handshake slot
    /// (TLS) or the PROXY waiter slot (a PROXY v2 endpoint, whose
    /// TLS handshake slot comes after its header), charged to `charge`.
    const Admission = struct {
        mode: connection.Mode,
        proxied: bool,
        charge: accounts.Charge,
        endpoint: usize,
        /// Where `admitAccepted` says which PROXY waiter slot it took; null
        /// for the other predicates.
        proxy_slot: ?*ProxySlot,
    };

    /// Every limit an accept on this endpoint must pass has room. Checked
    /// before `accept`, so a full server leaves new connections in the
    /// kernel's listen queue. Never `.client_refused`: no client is known
    /// before the accept.
    fn admissionRoom(self: *Server, a: Admission, locked: bool) connection.Admit {
        _ = locked;
        if (self.active_connections.load(.seq_cst) >= self.limits.max_connections) return .server_full;
        if (a.proxied) {
            const room = self.proxy_floor_used[a.endpoint].load(.seq_cst) < self.proxy_floor or
                self.proxy_shared_used.load(.seq_cst) < self.proxy_shared;
            return if (room) .ok else .server_full;
        }
        if (a.mode == .tls and self.accounting.active_handshakes.load(.seq_cst) >= self.accounting.max_handshakes) return .server_full;
        return .ok;
    }

    /// Take the connection slot and the second slot `admissionRoom` names:
    /// all or none. A full server parks the loop; a client over its share
    /// under pressure, on either slot, is refused (the caller closes it).
    fn admitAccepted(self: *Server, a: Admission, locked: bool) connection.Admit {
        const got: connection.Admit = admit: {
            const conn = self.tryAdmitConnection(a.charge);
            if (conn != .ok) break :admit conn;
            if (a.proxied) {
                if (self.tryAdmitProxyWaiter(a.endpoint)) |ps| {
                    a.proxy_slot.?.* = ps;
                    break :admit .ok;
                }
                self.undoConnection(a.charge, locked);
                break :admit .server_full;
            }
            if (a.mode == .tls) {
                const hs = self.accounting.tryAdmitHandshake(a.charge);
                if (hs != .ok) {
                    self.undoConnection(a.charge, locked);
                    break :admit hs;
                }
            }
            break :admit .ok;
        };
        if (got == .server_full and comptime connection.test_observe) _ = test_admit_refused.fetchAdd(1, .monotonic);
        return got;
    }

    /// A proxied TLS connection past its header: its handshake slot.
    fn admitHandshake(self: *Server, a: Admission, locked: bool) connection.Admit {
        _ = locked;
        return self.accounting.tryAdmitHandshake(a.charge);
    }

    fn awaitHandshakeSlot(self: *Server, a: Admission) std.Io.Cancelable!bool {
        return self.awaitAdmission(a, admitHandshake);
    }

    /// Park while `ready` finds the server full. A release of any admission
    /// slot wakes it (`GlobalAccounting.wakeAdmission`); the ordering
    /// argument is on `GlobalAccounting.admission_waiters`. Server stop
    /// cancels the accept group, which ends the wait with `error.Canceled`.
    /// `ready` is told whether it runs under `admission_mu`, so an undo
    /// inside it does not take the mutex again. True when admitted; false
    /// when `ready` refused the client (over its share under pressure), which
    /// never waits: one identity must not park the loop for everyone.
    fn awaitAdmission(
        self: *Server,
        a: Admission,
        comptime ready: fn (*Server, Admission, bool) connection.Admit,
    ) std.Io.Cancelable!bool {
        switch (ready(self, a, false)) {
            .ok => return true,
            .client_refused => return false,
            .server_full => {},
        }
        const acct = &self.accounting;
        try acct.admission_mu.lock();
        defer acct.admission_mu.unlock();
        _ = acct.admission_waiters.fetchAdd(1, .seq_cst);
        defer _ = acct.admission_waiters.fetchSub(1, .seq_cst);
        if (comptime connection.test_observe) _ = test_admission_waits.fetchAdd(1, .monotonic);
        while (true) {
            switch (ready(self, a, true)) {
                .ok => return true,
                .client_refused => return false,
                .server_full => try acct.admission_cond.wait(&acct.admission_mu),
            }
        }
    }

    /// `slot` is null exactly for h1c, which keeps its own buffers.
    ///
    /// The defers run bottom to top, and the order is the point: the slot is
    /// reset and pushed first, the balancer's count next, and the connection's
    /// admission last. Admission released first would let the accept loop see
    /// room, find the free list empty, and make a second slot. A safe build
    /// checks the order at the moment admission is released, whatever the
    /// scheduling: `slot_back` is set only after `recycle` has put the slot on
    /// the free list or in the quarantine.
    ///
    /// The client accounts go last of all: the connection's charge to them
    /// (`releaseConnection`), then its references (`accounts.Table.release`).
    /// Everything else the connection charged to them (streams, outbound
    /// bytes, request leases, a handshake) was given back before its task
    /// returned; what its handlers still hold is charged through their links'
    /// own references.
    ///
    /// `proxied`: a PROXY v2 endpoint. Its header is read here first
    /// (`takeProxyHeader`), and only then is the connection keyed and charged
    /// to its client (`.proxy_source`), or to none (`.none`).
    fn connEntry(self: *Server, stream: std.Io.net.Stream, config: connection.ConnConfig, slot: ?*conn_slot.ConnSlot, endpoint_index: usize, proxy: ?ProxyWait) std.Io.Cancelable!void {
        var cfg = config;
        defer self.clients.release(cfg.charge);
        var slot_back = slot == null;
        defer {
            if (std.debug.runtime_safety and !slot_back) {
                @panic("connection admission released before its slot was returned");
            }
            self.releaseConnection(cfg.charge);
        }
        defer if (config.balancer) |b| b.releaseConn(config.exec_index.?);
        defer if (slot) |sl| {
            _ = self.slots.recycle(sl);
            slot_back = true;
        };
        connection.probeNote(.conn_entry);
        if (proxy) |pw| {
            self.takeProxyHeader(stream, &cfg, endpoint_index, pw) catch |err| {
                stream.close(cfg.io);
                if (err == error.Canceled) return error.Canceled;
                return;
            };
        }
        if (cfg.mode == .h1c) return h1.serve(stream, cfg, null, &.{});
        if (cfg.mode == .tls) return serveTlsThenBranch(stream, cfg, slot.?);
        return connection.serveAccepted(stream, cfg, slot.?);
    }

    const ProxyError = error{ Canceled, Failed };

    /// A PROXY v2 connection, on its own task: read its header by its own
    /// deadline (`Limits.proxy_header_timeout_ns` from the accept); with
    /// `.proxy_source`, key it to the client the header names and charge the
    /// connection to that client (with `.none`, charge no client); and on TLS
    /// take its handshake slot by the preface deadline, also from the accept.
    /// The PROXY waiter slot is held until then. `Request.peer` is the
    /// header's source either way. On any failure the caller closes the
    /// connection; nothing is charged to a client then.
    fn takeProxyHeader(self: *Server, stream: std.Io.net.Stream, cfg: *connection.ConnConfig, endpoint_index: usize, pw: ProxyWait) ProxyError!void {
        defer self.releaseProxyWaiter(endpoint_index, pw.slot);
        const header_deadline_ns = pw.accepted_ns +| self.limits.proxy_header_timeout_ns;
        const slot_deadline_ns = pw.accepted_ns +| self.limits.preface_timeout_ns;
        const src = readProxyHeader(stream, self.limits.proxy_header_bytes, header_deadline_ns) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            _ = self.proxy_header_failures.fetchAdd(1, .monotonic);
            self.proxy_log.note("PROXY header from {f} failed ({s}); connection closed", .{ fmtPeer(cfg.peer), @errorName(err) });
            return error.Failed;
        };
        var keys_buf: [accounts.max_refs]accounts.Key = undefined;
        const keys: []const accounts.Key = switch (src) {
            .local => blk: {
                keys_buf[0] = accounts.infrastructureKey(@intCast(endpoint_index));
                break :blk keys_buf[0..1];
            },
            .peer => |p| blk: {
                cfg.peer = p;
                break :blk accounts.keysFor(p, self.limits.client_ipv6_aggregate_prefix_bits, &keys_buf);
            },
        };
        // The server's connection slot was taken at accept; the client's
        // charge is taken now that the client is known, and given back with
        // the server's in `connEntry`. `.none` (behind a pooling proxy) keys
        // nothing: the header only strips and names.
        const charge: accounts.Charge = switch (self.endpoints[endpoint_index].clientIdentity()) {
            // The server's count already holds this connection (taken at
            // accept): the start reads it without it.
            .proxy_source => switch (self.clients.acquire(keys, @intCast(endpoint_index), self.startNow(cfg.mode, 1)) catch return error.Canceled) {
                .charge => |c| c,
                // Counted (and a rate refusal logged) by the table: closed.
                .table_full, .rate_limited => return error.Failed,
            },
            .none => .none,
            // A boot error (`PeerIdentityBehindProxyProtocol`).
            .peer => unreachable,
        };
        // The server's count already holds this connection (taken at
        // accept), so its use before this connection is one less: the same
        // reading the plain path makes before its server charge.
        if (charge.add(.connections, 1, self.active_connections.load(.seq_cst) -| 1) != .took) {
            // Over its share under pressure: closed after the header. The
            // server's slot goes back in `connEntry` (its charge is still
            // `.none`, so no client count is given back there).
            self.clients.release(charge);
            return error.Failed;
        }
        cfg.charge = charge;
        if (cfg.mode != .tls) return;
        const a: Admission = .{ .mode = .tls, .proxied = true, .charge = charge, .endpoint = endpoint_index, .proxy_slot = null };
        const admitted = zio.withTimeout(.{ .deadline = .fromNanoseconds(slot_deadline_ns) }, awaitHandshakeSlot, .{ self, a }) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            _ = self.proxy_header_failures.fetchAdd(1, .monotonic);
            self.proxy_log.note("connection from {f} (PROXY) found no TLS handshake slot by the preface deadline; closed", .{fmtPeer(cfg.peer)});
            return error.Failed;
        };
        // Over its handshake share under pressure: closed (`Charge.add`
        // counted and logged it).
        if (!admitted) return error.Failed;
        cfg.handshake_held = true;
    }

    fn serveTlsThenBranch(stream: std.Io.net.Stream, config: connection.ConnConfig, slot: *conn_slot.ConnSlot) std.Io.Cancelable!void {
        const acceptor = config.tls_acceptor orelse {
            stream.close(config.io);
            return;
        };
        const tls_conn = slot.createTlsConn() catch {
            if (config.handshake_held) {
                if (config.accounting) |a| a.releaseHandshake(config.charge);
            }
            stream.close(config.io);
            return;
        };
        tls_conn.initTcp(stream);
        tls_conn.setupAccept(acceptor);

        handshakeWithTimeout(tls_conn, config) catch |err| {
            tls_conn.deinit();
            if (config.handshake_held) {
                if (config.accounting) |a| a.releaseHandshake(config.charge);
            }
            stream.close(config.io);
            if (err == error.Canceled) return error.Canceled;
            return;
        };
        if (config.handshake_held) {
            if (config.accounting) |a| a.releaseHandshake(config.charge);
        }

        var leftover_buf: [16 * 1024]u8 = undefined;
        const leftover_n = tls_conn.drainLeftoverPlain(&leftover_buf);
        const leftover = leftover_buf[0..leftover_n];

        if (tls_edge.isHttp2Alpn(tls_conn.selectedAlpn())) {
            // `leftover_buf` is on this frame, and `serveAccepted` runs the
            // whole connection inside it, so the bytes outlive their only
            // reader. That reader is the actor's first ingest
            // (`Connection.runBody`), and `Session.ingest` keeps nothing it
            // was given: a complete frame is read in place and a partial one
            // is copied into the parser's own buffer (`Parser.ingestOne`).
            // So no copy is made here.
            var cfg = config;
            cfg.handshake_held = false;
            cfg.tls_ready = tls_conn;
            cfg.tls_leftover = leftover;
            return connection.serveAccepted(stream, cfg, slot);
        }

        const owned = if (leftover_n == 0) &.{} else config.gpa.dupe(u8, leftover) catch {
            tls_conn.deinit();
            stream.close(config.io);
            return;
        };
        defer if (owned.len != 0) config.gpa.free(owned);
        var cfg = config;
        cfg.handshake_held = false;
        return h1.serve(stream, cfg, tls_conn, owned);
    }

    /// The handshake runs on this task, the connection's own, under a
    /// timeout. zio's socket registry makes the loop that FIRST parks on a
    /// socket direction its owner for the fd's life (`ev/sockreg.zig`), so a
    /// handshake in a subtask on another executor would leave that executor
    /// servicing every later read of this connection.
    ///
    /// On timeout the canceled read loses no bytes that matter: the caller
    /// closes the connection.
    fn handshakeWithTimeout(tls_conn: *tls_edge.Conn, config: connection.ConnConfig) error{ Canceled, TlsHandshakeFailed, TlsHandshakeTimeout }!void {
        const timeout: zio.Timeout = .fromNanoseconds(config.limits.preface_timeout_ns);
        zio.withTimeout(timeout, tls_edge.Conn.handshake, .{ tls_conn, config.io }) catch |err| switch (err) {
            error.Timeout => {
                if (comptime connection.test_observe) _ = test_handshake_timeouts.fetchAdd(1, .monotonic);
                return error.TlsHandshakeTimeout;
            },
            error.Canceled => return error.Canceled,
            error.TlsHandshakeFailed => return error.TlsHandshakeFailed,
        };
    }

    /// Accept until shutdown. One loop per endpoint.
    ///
    /// Admission happens HERE, before any connection state is built. While
    /// the connection limit, or for TLS the handshake limit, is full, the loop
    /// does not call accept at all: it parks until a slot is released, and new
    /// connections wait in the kernel's listen queue (`Limits.listen_backlog`)
    /// instead of being accepted and closed. A burst larger than the limits
    /// is then metered through them rather than refused, the way nginx and
    /// Go's net/http behave.
    ///
    /// Room is checked before accept and the slots are taken after it. A
    /// concurrent loop on another endpoint can take the room in between; this
    /// loop then waits again holding the one accepted stream. Slots are never
    /// held across an accept with nothing to show for it, so the counters stay
    /// exact for the drain checks and for other endpoints.
    ///
    /// What bounds the wait: a handshake slot is held at most
    /// `preface_timeout_ns` (the handshake timeout), so stalled TLS clients
    /// delay admission by at most that long. A connection slot is held for
    /// the connection's life; a server full of idle connections stops
    /// accepting until one closes, where it used to accept and close at once.
    /// The listen queue, and the client's own connect timeout, bound what
    /// waits behind it.
    ///
    /// The TLS handshake slot is separate from the connection slot on purpose.
    /// A handshake costs asymmetric CPU — the client sends a few bytes, the
    /// server performs public-key work — so the concurrent handshake count must
    /// be bounded independently of the connection count.
    ///
    /// `ConnectionAborted` continues the loop. A peer that disappears between
    /// the SYN and the accept is normal and must not stop the endpoint.
    ///
    /// No other error ends the loop either (`AcceptBackoff`): only a cancel
    /// does, which is how `serve` stops it.
    fn acceptLoop(
        self: *Server,
        endpoint_index: usize,
        mode: connection.Mode,
        connection_group: *zio.Group,
    ) std.Io.Cancelable!void {
        const listener = &self.listeners[endpoint_index];
        const endpoint = self.endpoints[endpoint_index];
        const identity = endpoint.clientIdentity();
        const proxied = endpoint.proxied();
        const unix = endpoint.unixPath() != null;
        var backoff: AcceptBackoff = .{};
        while (!self.shutdown_flag.load(.acquire)) {
            _ = try self.awaitAdmission(.{ .mode = mode, .proxied = proxied, .charge = .none, .endpoint = endpoint_index, .proxy_slot = null }, admissionRoom);
            connection.probeNote(.accept);
            const stream = listener.accept(self.io) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                error.ConnectionAborted => continue,
                else => {
                    _ = self.accept_errors.fetchAdd(1, .monotonic);
                    try backoff.fail(endpoint_index, err);
                    continue;
                },
            };
            backoff.ok();
            // A PROXY connection's deadlines count from here.
            const accepted_ns = awakeNs();
            // HTTP/2 writes small frames. Linux leaves Nagle on, so a frame can
            // sit until the peer's delayed ACK (~40 ms). That was nachos h2c at
            // 21k req/s with p99 glued to 42.01 ms while TLS (larger records)
            // was fine. Darwin's default is less punishing; the option is still
            // correct on every accepted socket.
            setTcpNoDelay(stream);
            // The socket peer, read once. It is the client for `.peer`, the
            // proxy on a PROXY endpoint (whose header then names the
            // client), and what the handler sees for `.none` without PROXY.
            const peer = connection.peerFromHandle(stream.socket.handle);
            // The trusted check first, before any slot is taken or byte
            // read: an untrusted source is closed, never keyed to its own
            // address.
            if (proxied and !unix and !proxy_protocol.trusted(endpoint.trusted(), peer)) {
                _ = self.proxy_untrusted.fetchAdd(1, .monotonic);
                self.proxy_log.note("connection from {f} to PROXY endpoint {d}: not a trusted source, closed", .{ fmtPeer(peer), endpoint_index });
                stream.close(self.io);
                continue;
            }
            var charge: accounts.Charge = .none;
            switch (identity) {
                // Keyed after the header (`takeProxyHeader`), or never.
                .none, .proxy_source => {},
                .peer => {
                    var keys_buf: [accounts.max_refs]accounts.Key = undefined;
                    const keys = accounts.keysFor(peer, self.limits.client_ipv6_aggregate_prefix_bits, &keys_buf);
                    switch (self.clients.acquire(keys, @intCast(endpoint_index), self.startNow(mode, 0)) catch |err| {
                        stream.close(self.io);
                        return err;
                    }) {
                        .charge => |c| charge = c,
                        // The table is full, or a start bucket is empty
                        // under pressure: refused and counted
                        // (`accounts.Table.insert_failures`,
                        // `rate_refusals`), never admitted uncounted.
                        .table_full, .rate_limited => {
                            stream.close(self.io);
                            continue;
                        },
                    }
                },
            }
            var proxy_slot: ProxySlot = undefined;
            const admission: Admission = .{ .mode = mode, .proxied = proxied, .charge = charge, .endpoint = endpoint_index, .proxy_slot = &proxy_slot };
            const admitted = self.awaitAdmission(admission, admitAccepted) catch |err| {
                self.clients.release(charge);
                stream.close(self.io);
                return err;
            };
            if (!admitted) {
                // Over a share under pressure (connections, or the
                // handshakes in progress): closed at accept, counted and
                // logged by `Charge.add`; nothing is held.
                self.clients.release(charge);
                stream.close(self.io);
                continue;
            }
            // A proxied TLS connection takes its handshake slot after its
            // header (`takeProxyHeader`).
            const handshake_held = mode == .tls and !proxied;
            // h1c keeps its own buffers and takes no slot. No slot is either
            // out of memory or, once `max_connections` slots exist and the
            // clean check holds some back, the cap: the connection is closed,
            // as on a spawn failure (`undoAccept`).
            const slot: ?*conn_slot.ConnSlot = if (mode == .h1c) null else self.slots.take() catch {
                self.undoAccept(stream, admission, null);
                continue;
            };
            const proxy_wait: ?ProxyWait = if (proxied) .{ .slot = proxy_slot, .accepted_ns = accepted_ns } else null;
            var config: connection.ConnConfig = .{
                .io = self.io,
                .mode = mode,
                .limits = self.limits,
                .router = self.router,
                .tls_acceptor = if (self.tls_acceptor) |*auth| auth else null,
                .gpa = self.gpa,
                .shutdown_flag = &self.shutdown_flag,
                .shutdown_event = &self.shutdown_event,
                .accounting = &self.accounting,
                .charge = charge,
                .peer = peer,
                .slab_pool = &self.slabs,
                .request_pool = &self.requests,
                .compression_pool = if (self.compression_pool) |*pool| pool else null,
                .handshake_held = handshake_held,
            };
            // A placed connection joins connection_group like any other, so
            // serve's drain and cancel cover it. Any spawn error (a placement
            // the runtime refuses, a runtime shutting down) falls through to
            // the plain group spawn below: a placement must never be the
            // reason an accepted connection is closed.
            if (self.balancer) |b| {
                if (b.reserve()) |ei| {
                    config.balancer = b;
                    config.exec_index = ei;
                    if (connection_group.spawnInto(.{ .executor = ei }, connEntry, .{ self, stream, config, slot, endpoint_index, proxy_wait })) |_| {
                        b.notePlaced(ei, stream.socket.address.getPort());
                        continue;
                    } else |_| {
                        b.releaseConn(ei);
                        config.balancer = null;
                        config.exec_index = null;
                    }
                }
            }
            // Placement probe only (observe, pinned builds): place the
            // connection on a chosen executor instead of zio's `.auto`, in
            // the same group and with the same fallback.
            if (comptime connection.placement_check) {
                if (connection.probeConnPlacement()) |placement| {
                    if (connection_group.spawnInto(placement, connEntry, .{ self, stream, config, slot, endpoint_index, proxy_wait })) |_| {
                        continue;
                    } else |_| {}
                }
            }
            connection_group.spawn(connEntry, .{ self, stream, config, slot, endpoint_index, proxy_wait }) catch {
                self.undoAccept(stream, admission, slot);
                continue;
            };
        }
    }

    /// An accepted connection that will not run: give back what
    /// `admitAccepted` took, in reverse, and close it. The slot goes back
    /// before admission is released, for the reason `connEntry` gives; the
    /// client's references go last.
    fn undoAccept(self: *Server, stream: std.Io.net.Stream, a: Admission, slot: ?*conn_slot.ConnSlot) void {
        if (a.proxied) {
            self.releaseProxyWaiter(a.endpoint, a.proxy_slot.?.*);
        } else if (a.mode == .tls) {
            self.accounting.releaseHandshake(a.charge);
        }
        if (slot) |sl| _ = self.slots.recycle(sl);
        self.releaseConnection(a.charge);
        self.clients.release(a.charge);
        stream.close(self.io);
    }

    pub fn requestShutdown(self: *Server) void {
        self.shutdown_flag.store(true, .release);
        self.shutdown_event.set();
    }

    pub const WaitListeningError = error{
        BindFailed,
        Timeout,
        Canceled,
    };

    pub fn waitUntilListening(self: *Server, timeout_ns: u64) WaitListeningError!void {
        switch (self.bind_state.load(.acquire)) {
            .listening => return,
            .failed => return error.BindFailed,
            .pending => {},
        }
        self.bind_event.waitTimeout(.fromNanoseconds(timeout_ns)) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Timeout => return switch (self.bind_state.load(.acquire)) {
                .listening => {},
                .failed => error.BindFailed,
                .pending => error.Timeout,
            },
        };
        return switch (self.bind_state.load(.acquire)) {
            .listening => {},
            .failed => error.BindFailed,
            .pending => error.Timeout,
        };
    }

    pub fn localAddress(self: *const Server, endpoint_index: usize) EndpointAddress {
        return self.local_addrs[endpoint_index];
    }

    /// Every counter must be zero once `serve` has returned, so a release
    /// that some path forgot is reported here instead of drifting in
    /// production, where it would look like a slow capacity leak under load.
    ///
    /// The checks run in every build and log (`err`) instead of asserting: a
    /// release build must not free the request pool under a live handler.
    /// `serve` still running (`serving`), or a connection, a stream, a task
    /// handler (orphans included), outbound bytes, a handshake or a
    /// compression context still counted, means `deinit` was called while
    /// `serve` had not returned, a contract violation by the caller. Then `deinit` frees nothing at all and leaks
    /// the whole server, since anything here may still be reached by what is
    /// running; the caller must not reuse the `Server`'s own memory either.
    pub fn deinit(self: *Server, gpa: std.mem.Allocator) void {
        std.debug.assert(gpa.vtable == self.gpa.vtable);
        var live = false;
        if (self.serving.load(.acquire)) {
            log.err("Server.deinit: serve has not returned", .{});
            live = true;
        }
        live = checkZero("active_connections", self.active_connections.load(.acquire)) or live;
        live = checkZero("active_streams", self.accounting.active_streams.load(.acquire)) or live;
        live = checkZero("task handlers (task_handlers)", self.accounting.task_handlers.load(.acquire)) or live;
        live = checkZero("orphaned_handlers", self.accounting.orphaned_handlers.load(.acquire)) or live;
        live = checkZero("outbound_bytes", self.accounting.outbound_bytes.load(.acquire)) or live;
        live = checkZero("active_handshakes", self.accounting.active_handshakes.load(.acquire)) or live;
        if (self.compression_pool) |*pool| {
            live = checkZero("compression contexts", pool.liveCount()) or live;
        }
        live = checkZero("PROXY header waiters", self.proxyWaiters()) or live;
        // Conservation: every client account back to zero and every record
        // free. With `serve` returned and the counters above at zero, a
        // record left is a release some path forgot.
        if (!self.clients.checkEmpty()) live = true;
        // The same for the task routes' accounts: an orphan that returned
        // gave its route back too.
        if (!self.accounting.routes.checkEmpty()) live = true;
        if (live) {
            log.err("Server.deinit with work still counted (serve has not returned?): freeing nothing, leaking the server", .{});
            return;
        }
        for (self.routes) |route| gpa.free(route.path);
        gpa.free(self.routes);
        freeEndpoints(gpa, self.endpoints);
        gpa.free(self.listeners);
        gpa.free(self.local_addrs);
        // Asserts every slab came home; a connection teardown that missed one
        // fails here rather than silently shrinking the pool.
        self.slabs.deinit(gpa);
        // Same for request blocks: one still out is a request that never gave
        // its memory back.
        self.requests.deinit();
        // Every slot must be back (or leaked on purpose): the connections are
        // gone, so one still out would be a connection that never gave its
        // slot back.
        self.slots.deinit();
        self.clients.deinit(gpa);
        self.accounting.routes.deinit(gpa);
        if (self.tls_acceptor) |*auth| auth.deinit();
        self.* = undefined;
    }
};

/// What an accept loop does when `accept` fails: wait and try again, never
/// give up.
///
/// Every error is retried, after 10 ms the first time and twice as long each
/// time after, up to 1 s; a successful accept resets it. The errors fall in
/// three groups, and none is fatal to the listener:
///
/// - Out of a resource: EMFILE (`ProcessFdQuotaExceeded`), ENFILE
///   (`SystemFdQuotaExceeded`), ENOBUFS and ENOMEM (`SystemResources`; zio's
///   io_uring backend reports EMFILE this way too). They clear when a
///   connection ends or memory is freed, which is what the wait is for: an
///   immediate retry would fail again at once and spin.
/// - About the one connection being taken: EPROTO (`ProtocolFailure`), EPERM
///   (`BlockedByFirewall`), ENETDOWN and the like (Linux's accept(2) says to
///   retry these as EAGAIN). The next connection is fine; the wait costs at
///   most 10 ms when they are isolated.
/// - About the listener itself: EINVAL (`SocketNotListening`) and whatever
///   zio maps to `Unexpected` (ENOTSOCK, EOPNOTSUPP, EBADF). On this server
///   they mean the listener was closed, which happens only after `serve`
///   cancelled this loop, so the cancel ends it first. Exiting instead would
///   leave the endpoint dead with nothing that could restart it; retrying at
///   1 s costs one system call a second and keeps the log line coming.
///
/// It logs the first failure at once and then at most once a minute, with
/// the count since the last line, so a flood of EMFILE is visible without
/// becoming a flood of log lines. `Server.accept_errors` counts every one.
const AcceptBackoff = struct {
    delay_ms: u64 = 0,
    failures_since_log: u64 = 0,
    last_log_ns: ?u64 = null,

    const first_ms = 10;
    const max_ms = 1000;
    const log_interval_ns = 60 * std.time.ns_per_s;

    fn fail(self: *AcceptBackoff, endpoint_index: usize, err: anyerror) std.Io.Cancelable!void {
        self.delay_ms = if (self.delay_ms == 0) first_ms else @min(self.delay_ms * 2, max_ms);
        self.failures_since_log += 1;
        const now = monoNs();
        if (self.last_log_ns == null or now -% self.last_log_ns.? >= log_interval_ns) {
            log.warn("accept on endpoint {d} failed: {s} ({d} failures since the last report); retrying in {d} ms", .{
                endpoint_index, @errorName(err), self.failures_since_log, self.delay_ms,
            });
            self.last_log_ns = now;
            self.failures_since_log = 0;
        }
        zio.sleep(.fromMilliseconds(self.delay_ms)) catch return error.Canceled;
    }

    fn ok(self: *AcceptBackoff) void {
        self.delay_ms = 0;
    }
};

const log = std.log.scoped(.starh2);

/// Descriptors the process needs beside one per connection: stdin, stdout
/// and stderr; one listener per endpoint (`Limits.max_endpoints` is 8 by
/// default); one socket per endpoint that an accept loop holds before
/// admission is taken; the runtime's own (a kqueue or epoll or io_uring
/// descriptor and a wakeup descriptor per executor); and room for a TLS
/// certificate read and a few that an embedder or a handler opens. 64 covers
/// those with the default endpoints and up to about 16 executors. What
/// handlers open beyond that is the application's to budget: a server whose
/// handlers open files needs a larger limit than this check asks for.
pub const reserved_fds: usize = 64;

/// Read RLIMIT_NOFILE, raise the soft limit as far as the process may (the
/// hard limit; on macOS no further than `kern.maxfilesperproc`, since
/// setrlimit above it fails, and the hard limit can read RLIM_INFINITY), and
/// refuse to start when even that is below `max_connections` plus
/// `reserved_fds`.
///
/// Without it a soft limit of 1,024, the usual default on Linux, below a
/// `max_connections` of 1,024, lets the clients use up the descriptors: every
/// accept then fails with EMFILE. The accept loop survives that
/// (`AcceptBackoff`), but the server cannot serve what it was configured
/// for, and that is an error to report at boot, where an operator sees it,
/// not under load.
fn checkFileLimit(max_connections: usize) InitError!void {
    if (builtin.os.tag == .windows) return;
    const need = std.math.add(usize, max_connections, reserved_fds) catch return error.FileLimitTooLow;
    var lim = std.posix.getrlimit(.NOFILE) catch return error.FileLimitTooLow;
    var ceiling: u64 = lim.max;
    if (builtin.os.tag.isDarwin()) {
        var per_proc: c_int = 0;
        var len: usize = @sizeOf(c_int);
        if (std.c.sysctlbyname("kern.maxfilesperproc", &per_proc, &len, null, 0) == 0 and per_proc > 0) {
            ceiling = @min(ceiling, @as(u64, @intCast(per_proc)));
        }
    }
    if (lim.cur < ceiling) {
        lim.cur = @intCast(ceiling);
        // A refusal leaves the soft limit where it was, which the check
        // below then judges.
        std.posix.setrlimit(.NOFILE, lim) catch {};
        lim = std.posix.getrlimit(.NOFILE) catch return error.FileLimitTooLow;
    }
    // macOS lets the soft limit stand above `kern.maxfilesperproc` (a shell
    // can show 1,048,576), but no descriptor past that is ever given out.
    const usable = @min(lim.cur, ceiling);
    if (usable < need) {
        log.warn("RLIMIT_NOFILE allows {d} descriptors at most (soft {d}, ceiling {d}), below max_connections ({d}) plus {d} reserved: lower max_connections or raise the hard limit", .{ usable, lim.cur, ceiling, max_connections, reserved_fds });
        return error.FileLimitTooLow;
    }
}

/// Log a counter `deinit` expects at zero. True when it is not.
fn checkZero(comptime name: []const u8, value: usize) bool {
    if (value == 0) return false;
    log.err("Server.deinit: " ++ name ++ " is {d}, expected 0", .{value});
    return true;
}

/// Add one to `counter` unless it is at `cap`. True when it did.
fn boundedIncrement(counter: *std.atomic.Value(usize), cap: usize) bool {
    var cur = counter.load(.seq_cst);
    while (cur < cap) {
        cur = counter.cmpxchgWeak(cur, cur + 1, .seq_cst, .seq_cst) orelse return true;
    }
    return false;
}

fn monoNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

/// The clock zio measures a `.deadline` against (`AutoCancel.set`,
/// `timedWaitForIo`: `.awake`). An absolute deadline from a fresh clock, not
/// a `.duration`, which zio arms from its loop's cached clock (t-3653).
fn awakeNs() u64 {
    return zio.Timestamp.now(.awake).toNanoseconds();
}

const HeaderError = error{
    Canceled,
    Timeout,
    /// The peer closed or the read failed before the header was complete.
    Closed,
    BadSignature,
    BadVersion,
    BadCommand,
    TooLong,
    BadFamily,
    ShortAddress,
};

/// Read one PROXY v2 header from `stream`, exactly: no byte past it is
/// consumed, since what follows belongs to TLS, HTTP/2 or HTTP/1.1. By
/// `deadline_ns` (the awake clock), and no more than `cap` bytes in all; the
/// TLVs are read and dropped through a small buffer, so the cap does not size
/// any buffer here.
fn readProxyHeader(stream: std.Io.net.Stream, cap: usize, deadline_ns: u64) HeaderError!proxy_protocol.Source {
    const timeout: zio.Timeout = .{ .deadline = .fromNanoseconds(deadline_ns) };
    var fixed: [proxy_protocol.fixed_bytes]u8 = undefined;
    try readExact(stream, &fixed, timeout);
    const f = try proxy_protocol.parseFixed(&fixed, cap);
    // The address block is at most 216 bytes (UNIX); what follows is TLVs.
    var body: [216]u8 = undefined;
    const keep = @min(@as(usize, f.rest), body.len);
    try readExact(stream, body[0..keep], timeout);
    var left: usize = f.rest - keep;
    var drop: [256]u8 = undefined;
    while (left > 0) {
        const n = @min(left, drop.len);
        try readExact(stream, drop[0..n], timeout);
        left -= n;
    }
    // `parseBody` takes the whole rest; only the address block matters, and
    // TLVs are ignored, so the kept prefix stands for it with its length.
    var short = f;
    short.rest = @intCast(keep);
    return proxy_protocol.parseBody(short, body[0..keep]);
}

fn readExact(stream: std.Io.net.Stream, buf: []u8, timeout: zio.Timeout) HeaderError!void {
    const NetHandle = zio.ev.Backend.NetHandle;
    const h = stream.socket.handle;
    const handle: NetHandle = if (@typeInfo(NetHandle) == .pointer) @ptrCast(h) else h;
    var got: usize = 0;
    while (got < buf.len) {
        var iov: [1]zio.os.iovec = undefined;
        const n = zio.net.readBuf(handle, .fromSlice(buf[got..], &iov), timeout) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Timeout => return error.Timeout,
            else => return error.Closed,
        };
        if (n == 0) return error.Closed;
        got += n;
    }
}

/// One warn line at most a minute, with the count of the ones held back:
/// the PROXY failures a client can cause must not become a flood of lines.
const RateLimitedLog = struct {
    last_ns: std.atomic.Value(u64) = .init(0),
    held: std.atomic.Value(u64) = .init(0),

    const interval_ns = 60 * std.time.ns_per_s;

    fn note(self: *RateLimitedLog, comptime fmt: []const u8, args: anytype) void {
        const now = monoNs();
        const last = self.last_ns.load(.monotonic);
        if (last != 0 and now -% last < interval_ns) {
            _ = self.held.fetchAdd(1, .monotonic);
            return;
        }
        if (self.last_ns.cmpxchgStrong(last, now, .monotonic, .monotonic) != null) {
            _ = self.held.fetchAdd(1, .monotonic);
            return;
        }
        const held = self.held.swap(0, .monotonic);
        log.warn(fmt ++ " ({d} more since the last report)", args ++ .{held});
    }
};

fn fmtPeer(p: request.Peer) std.fmt.Alt(request.Peer, formatPeer) {
    return .{ .data = p };
}

fn formatPeer(p: request.Peer, w: *std.Io.Writer) std.Io.Writer.Error!void {
    switch (p) {
        .ip4 => |a| try w.print("{d}.{d}.{d}.{d}", .{ a[0], a[1], a[2], a[3] }),
        .ip6 => |a| {
            for (0..8) |g| {
                if (g != 0) try w.writeByte(':');
                try w.print("{x}", .{std.mem.readInt(u16, a[2 * g ..][0..2], .big)});
            }
        },
        .unknown => try w.writeAll("(unknown peer)"),
    }
}

/// A trusted PROXY source range as the accept loop compares it
/// (`Cidr.checked`), or the boot error, with a log line that names the range.
fn checkTrusted(endpoint_index: usize, c: proxy_protocol.Cidr) InitError!proxy_protocol.Cidr {
    return c.checked() catch |err| switch (err) {
        error.InvalidCidr => error.InvalidConfig,
        error.HoldsMappedBlock => {
            log.warn("endpoint {d}: the trusted PROXY source range {f} holds every IPv4-mapped address (::ffff:0:0/96) without being one of them, so it would trust IPv4 sources it does not name; list the IPv4 range (for example 192.0.2.0/24) or the mapped range (::ffff:192.0.2.0/120) explicitly", .{ endpoint_index, c });
            return error.ProxyTrustedRangeHoldsIpv4;
        },
    };
}

/// The server's own copy of a trusted list, each range normalised, or the
/// boot error (`checkTrusted`). The only place a trusted range is checked.
fn ownTrusted(gpa: std.mem.Allocator, endpoint_index: usize, list: []const proxy_protocol.Cidr) InitError![]proxy_protocol.Cidr {
    const copy = try gpa.alloc(proxy_protocol.Cidr, list.len);
    errdefer gpa.free(copy);
    for (copy, list) |*dst, src| dst.* = try checkTrusted(endpoint_index, src);
    return copy;
}

/// Bind and listen one endpoint with an explicit accept-queue length.
///
/// std.Io's `ListenOptions.kernel_backlog` defaults to 128, and zio's
/// `netListenIp` passes it straight to listen(2), so a server that leaves it
/// alone gets a 128-entry queue whatever the kernel allows. `backlog` comes
/// from `Limits.listen_backlog`; the kernel caps it at somaxconn.
fn listenEndpoint(io: std.Io, address: EndpointAddress, backlog: u31) std.Io.net.IpAddress.ListenError!std.Io.net.Server {
    return address.listen(io, .{ .reuse_address = true, .kernel_backlog = backlog });
}

/// A Unix stream listener at `path`. Init checked the path's length.
fn listenUnix(io: std.Io, path: []const u8, backlog: u31) std.Io.net.UnixAddress.ListenError!std.Io.net.Server {
    const ua = std.Io.net.UnixAddress.init(path) catch unreachable;
    return ua.listen(io, .{ .kernel_backlog = backlog });
}

fn freeEndpoints(gpa: std.mem.Allocator, endpoints: []EndpointConfig) void {
    for (endpoints) |ep| {
        if (ep.unixPath()) |p| {
            if (p.len != 0) gpa.free(p);
        }
        const t = ep.trusted();
        if (t.len != 0) gpa.free(t);
    }
    gpa.free(endpoints);
}

/// Turn off Nagle on an accepted TCP socket.
///
/// `posix.setsockopt` treats `.INVAL` as unreachable (Zig 0.16
/// `lib/std/posix.zig:1081`). Darwin returns EINVAL when the peer RST'd
/// between accept and this call, which aborted the process before the
/// accept-loop catch could run. `system.setsockopt` reports the errno;
/// any failure is non-fatal — the connection is live, the next write pays
/// Nagle.
///
/// `posix.setsockopt` is also a Windows compile error (`use std.Io instead`);
/// this stack's gates are Darwin and Linux, and `builtin.os.tag` is comptime
/// so the Windows branch is not analyzed there.
fn setTcpNoDelay(stream: std.Io.net.Stream) void {
    if (builtin.os.tag == .windows) return;
    const on: c_int = 1;
    const opt = std.mem.asBytes(&on);
    _ = std.posix.errno(std.posix.system.setsockopt(
        stream.socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        opt.ptr,
        @intCast(opt.len),
    ));
}

test "accepted TCP sockets take TCP_NODELAY" {
    if (builtin.os.tag == .windows) return;
    const io = std.testing.io;
    const bind = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try bind.listen(io, .{ .reuse_address = true });
    defer listener.socket.close(io);
    const dest = listener.socket.address;
    const client = try dest.connect(io, .{ .mode = .stream });
    defer client.close(io);
    const accepted = try listener.accept(io);
    defer accepted.close(io);
    setTcpNoDelay(accepted);
    var val: c_int = 0;
    var len: std.posix.socklen_t = @sizeOf(c_int);
    switch (std.posix.errno(std.posix.system.getsockopt(
        accepted.socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        @ptrCast(&val),
        &len,
    ))) {
        .SUCCESS => {},
        else => return error.GetSockOptFailed,
    }
    // Darwin's getsockopt(TCP_NODELAY) returns 4 when the option is on, not 1
    // (verified against CPython on this host). Linux returns 1. Both are "Nagle
    // is off"; the kernel boolean is nonzero.
    try std.testing.expect(val != 0);
}

test "TCP_NODELAY after peer RST does not abort" {
    if (builtin.os.tag == .windows) return;
    const io = std.testing.io;
    const bind = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try bind.listen(io, .{ .reuse_address = true });
    defer listener.socket.close(io);
    const dest = listener.socket.address;
    const client = try dest.connect(io, .{ .mode = .stream });
    const accepted = try listener.accept(io);
    defer accepted.close(io);

    // SO_LINGER 0 + close is RST, not FIN. posix.setsockopt cannot be used
    // here either: the same `.INVAL => unreachable` would abort if the
    // option itself failed.
    const linger_val = std.posix.linger{ .onoff = 1, .linger = 0 };
    const linger_bytes = std.mem.asBytes(&linger_val);
    switch (std.posix.errno(std.posix.system.setsockopt(
        client.socket.handle,
        std.posix.SOL.SOCKET,
        std.posix.SO.LINGER,
        linger_bytes.ptr,
        @intCast(linger_bytes.len),
    ))) {
        .SUCCESS => {},
        else => {
            client.close(io);
            return error.LingerSetFailed;
        },
    }
    client.close(io);

    // Recv so Darwin processes the RST before TCP_NODELAY. Without this,
    // setsockopt still succeeds (the option race has not landed yet).
    var scratch: [1]u8 = undefined;
    var dest_bufs: [1][]u8 = .{&scratch};
    var reader = accepted.reader(io, &.{});
    _ = reader.interface.readVec(&dest_bufs) catch {};

    const on: c_int = 1;
    const opt = std.mem.asBytes(&on);
    const nodelay_errno = std.posix.errno(std.posix.system.setsockopt(
        accepted.socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        opt.ptr,
        @intCast(opt.len),
    ));
    if (builtin.os.tag.isDarwin()) {
        // This is the errno posix.setsockopt used to treat as unreachable.
        try std.testing.expectEqual(std.posix.E.INVAL, nodelay_errno);
    }

    setTcpNoDelay(accepted);
}

/// The accept-queue length the kernel actually granted a listening socket.
/// Linux reports it as `tcpi_sacked` in TCP_INFO for a socket in LISTEN
/// (`sk_max_ack_backlog`). Darwin has no getsockopt for it (only `netstat
/// -L` shows it), so the test below runs on Linux only.
fn linuxGrantedBacklog(listener: std.Io.net.Server) !u32 {
    var info: [104]u8 align(4) = undefined;
    var len: std.posix.socklen_t = info.len;
    switch (std.posix.errno(std.posix.system.getsockopt(
        listener.socket.handle,
        std.posix.IPPROTO.TCP,
        std.os.linux.TCP.INFO,
        &info,
        &len,
    ))) {
        .SUCCESS => {},
        else => return error.GetSockOptFailed,
    }
    // struct tcp_info: 8 bytes of u8 fields, then u32 rto, ato, snd_mss,
    // rcv_mss, unacked, sacked.
    if (len < 32) return error.ShortTcpInfo;
    return std.mem.readInt(u32, info[28..32], .little);
}

fn linuxSomaxconn(io: std.Io) !u32 {
    var buf: [32]u8 = undefined;
    const text = try std.Io.Dir.cwd().readFile(io, "/proc/sys/net/core/somaxconn", &buf);
    return std.fmt.parseInt(u32, std.mem.trim(u8, text, " \n"), 10);
}

test "endpoints listen with Limits.listen_backlog, not the 128 default" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const io = std.testing.io;
    const cap = try linuxSomaxconn(io);
    const bind = try std.Io.net.IpAddress.parse("127.0.0.1", 0);

    // Control: the std.Io default the server used before. If this ever reads
    // more than 128, the default moved and the knob below proves less.
    var plain = try bind.listen(io, .{ .reuse_address = true });
    defer plain.socket.close(io);
    try std.testing.expectEqual(@min(cap, 128), try linuxGrantedBacklog(plain));

    const want = limits_mod.Limits.defaults.listen_backlog;
    try std.testing.expect(want > 128);
    var server_listener = try listenEndpoint(io, bind, want);
    defer server_listener.socket.close(io);
    try std.testing.expectEqual(@min(cap, want), try linuxGrantedBacklog(server_listener));

    // A small explicit value is honoured too, so the knob is not a floor.
    var small = try listenEndpoint(io, bind, 7);
    defer small.socket.close(io);
    try std.testing.expectEqual(@as(u32, 7), try linuxGrantedBacklog(small));
}
