//! Listen, accept, global limits, graceful stop.
const std = @import("std");
const tls = @import("tls");
const limits_mod = @import("../core/limits.zig");
const router_mod = @import("../http/router.zig");
const connection = @import("connection.zig");

pub const EndpointAddress = std.Io.net.IpAddress;

pub const EndpointConfig = union(enum) {
    tls_h2: EndpointAddress,
    h2c_prior_knowledge: EndpointAddress,
};

pub const TlsConfig = struct {
    certificate_chain_pem: []const u8,
    private_key_pem: []const u8,
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
    routes: []const router_mod.Route,
    tls: ?TlsConfig,
    limits: limits_mod.Limits = .defaults,
};

/// Observable progress of `serve` startup.
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
    tls_auth: ?tls.config.CertKeyPair = null,
    shutdown_flag: std.atomic.Value(bool) = .init(false),
    shutdown_event: std.Io.Event = .unset,
    active_connections: std.atomic.Value(usize) = .init(0),
    accounting: connection.GlobalAccounting,
    router: router_mod.Router = undefined,
    reaper: ?connection.ReaperPool = null,
    listeners_bound: usize = 0,
    bind_state: std.atomic.Value(BindState) = .init(.pending),
    bind_event: std.Io.Event = .unset,

    pub fn init(gpa: std.mem.Allocator, io: std.Io, config: ServerConfig) InitError!Server {
        _ = try config.limits.resourceUpperBound();
        if (config.endpoints.len == 0 or config.endpoints.len > config.limits.max_endpoints) return error.InvalidConfig;
        if (config.routes.len > config.limits.max_routes) return error.InvalidConfig;
        if (config.limits.cancellation_reaper_jobs < config.limits.max_streams_per_server) return error.InvalidConfig;
        if (config.limits.cancellation_reaper_tasks == 0) return error.InvalidConfig;

        var need_tls = false;
        for (config.endpoints) |ep| {
            if (ep == .tls_h2) need_tls = true;
        }
        if (need_tls and config.tls == null) return error.InvalidConfig;

        if (@hasDecl(@import("tls"), "nonblock")) {
            if (tls.nonblock.starh2_nonblock_abi != 1) return error.TlsPatchMismatch;
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
            };
        }

        const endpoints = try gpa.dupe(EndpointConfig, config.endpoints);
        errdefer gpa.free(endpoints);

        var tls_auth: ?tls.config.CertKeyPair = null;
        errdefer if (tls_auth) |*auth| {
            if (@hasDecl(@TypeOf(auth.*), "deinit")) auth.deinit(gpa);
        };
        if (config.tls) |tls_config| {
            if (tls_config.certificate_chain_pem.len > config.limits.certificate_chain_bytes) return error.CertificateTooLarge;
            if (tls_config.private_key_pem.len > config.limits.private_key_bytes) return error.PrivateKeyTooLarge;
            tls_auth = loadCertKey(gpa, io, tls_config) catch return error.InvalidCertificate;
        }

        const listeners = try gpa.alloc(std.Io.net.Server, config.endpoints.len);
        errdefer gpa.free(listeners);
        const local_addrs = try gpa.alloc(EndpointAddress, config.endpoints.len);
        errdefer gpa.free(local_addrs);

        var reaper = connection.ReaperPool.init(gpa, io, config.limits.cancellation_reaper_jobs) catch return error.OutOfMemory;
        errdefer reaper.deinit();

        var self: Server = .{
            .gpa = gpa,
            .io = io,
            .limits = config.limits,
            .routes = routes,
            .endpoints = endpoints,
            .listeners = listeners,
            .local_addrs = local_addrs,
            .tls_auth = tls_auth,
            .accounting = .{
                .max_streams = config.limits.max_streams_per_server,
                .reaper_capacity = config.limits.cancellation_reaper_jobs,
                .max_outbound_bytes = config.limits.outbound_bytes_per_server,
                .max_request_bytes = config.limits.request_bytes_per_server,
                .max_handshakes = config.limits.concurrent_tls_handshakes,
            },
            .reaper = reaper,
        };
        self.router = .{ .routes = self.routes };
        return self;
    }

    fn loadCertKey(gpa: std.mem.Allocator, io: std.Io, config: TlsConfig) !tls.config.CertKeyPair {
        return tls.config.CertKeyPair.fromSlice(gpa, io, config.certificate_chain_pem, config.private_key_pem);
    }

    fn closeListeners(self: *Server) void {
        var i: usize = 0;
        while (i < self.listeners_bound) : (i += 1) self.listeners[i].socket.close(self.io);
        self.listeners_bound = 0;
    }

    fn stopReapers(self: *Server, group: *std.Io.Group) void {
        if (self.reaper) |*pool| pool.jobs.close(self.io);
        group.await(self.io) catch {
            // A canceled serve task still must wait until every queued handler join is consumed.
            group.cancel(self.io);
        };
    }

    pub fn serve(self: *Server, gpa: std.mem.Allocator) ServeError!void {
        std.debug.assert(gpa.vtable == self.gpa.vtable);
        self.listeners_bound = 0;
        errdefer {
            self.closeListeners();
            self.bind_state.store(.failed, .release);
            self.bind_event.set(self.io);
        }

        for (self.endpoints, 0..) |endpoint, i| {
            const address = switch (endpoint) {
                .tls_h2 => |value| value,
                .h2c_prior_knowledge => |value| value,
            };
            self.listeners[i] = address.listen(self.io, .{ .reuse_address = true }) catch return error.ListenFailed;
            self.local_addrs[i] = self.listeners[i].socket.address;
            self.listeners_bound = i + 1;
        }

        var reaper_group: std.Io.Group = .init;
        var accept_group: std.Io.Group = .init;
        var connection_group: std.Io.Group = .init;
        var reapers_live = false;
        var accepts_live = false;
        var connections_live = false;

        if (self.reaper) |*pool| {
            var i: usize = 0;
            while (i < self.limits.cancellation_reaper_tasks) : (i += 1) {
                reaper_group.concurrent(self.io, connection.ReaperPool.worker, .{pool}) catch {
                    reaper_group.cancel(self.io);
                    return error.TransportSetupFailed;
                };
                reapers_live = true;
            }
        }
        errdefer if (reapers_live) self.stopReapers(&reaper_group);

        for (self.endpoints, 0..) |endpoint, i| {
            const mode: connection.Mode = switch (endpoint) {
                .tls_h2 => .tls_h2,
                .h2c_prior_knowledge => .h2c,
            };
            accept_group.concurrent(self.io, acceptLoop, .{ self, i, mode, &connection_group }) catch {
                accept_group.cancel(self.io);
                connection_group.cancel(self.io);
                return error.TransportSetupFailed;
            };
            accepts_live = true;
            connections_live = true;
        }
        errdefer {
            if (accepts_live) accept_group.cancel(self.io);
            if (connections_live) connection_group.cancel(self.io);
        }

        self.bind_state.store(.listening, .release);
        self.bind_event.set(self.io);

        const canceled = blk: {
            self.shutdown_event.wait(self.io) catch break :blk true;
            break :blk false;
        };
        self.shutdown_flag.store(true, .release);
        self.shutdown_event.set(self.io);
        accept_group.cancel(self.io);
        accepts_live = false;
        self.closeListeners();

        if (canceled) {
            connection_group.cancel(self.io);
        } else {
            connection_group.await(self.io) catch {
                // If the serve task is canceled during graceful drain, force task cleanup.
                connection_group.cancel(self.io);
            };
        }
        connections_live = false;
        self.stopReapers(&reaper_group);
        reapers_live = false;
        if (canceled) return error.RuntimeShutdown;
    }

    fn tryAdmitConnection(self: *Server) bool {
        while (true) {
            const current = self.active_connections.load(.acquire);
            if (current >= self.limits.max_connections) return false;
            if (self.active_connections.cmpxchgWeak(current, current + 1, .acq_rel, .acquire) == null) return true;
        }
    }

    fn connEntry(self: *Server, stream: std.Io.net.Stream, config: connection.ConnConfig) std.Io.Cancelable!void {
        defer _ = self.active_connections.fetchSub(1, .acq_rel);
        return connection.serveAccepted(stream, config);
    }

    fn acceptLoop(
        self: *Server,
        endpoint_index: usize,
        mode: connection.Mode,
        connection_group: *std.Io.Group,
    ) std.Io.Cancelable!void {
        const listener = &self.listeners[endpoint_index];
        while (!self.shutdown_flag.load(.acquire)) {
            const stream = listener.accept(self.io) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                error.ConnectionAborted => continue,
                else => {
                    // Listener failure is endpoint-local; serve teardown owns the socket.
                    return;
                },
            };
            if (!self.tryAdmitConnection()) {
                stream.close(self.io);
                continue;
            }

            var handshake_held = false;
            if (mode == .tls_h2) {
                if (!self.accounting.tryAdmitHandshake()) {
                    _ = self.active_connections.fetchSub(1, .acq_rel);
                    stream.close(self.io);
                    continue;
                }
                handshake_held = true;
            }
            const config: connection.ConnConfig = .{
                .io = self.io,
                .mode = mode,
                .limits = self.limits,
                .router = self.router,
                .tls_auth = if (self.tls_auth) |*auth| auth else null,
                .gpa = self.gpa,
                .shutdown_flag = &self.shutdown_flag,
                .shutdown_event = &self.shutdown_event,
                .reaper = if (self.reaper) |*pool| pool else null,
                .accounting = &self.accounting,
                .handshake_held = handshake_held,
            };
            connection_group.concurrent(self.io, connEntry, .{ self, stream, config }) catch {
                if (handshake_held) self.accounting.releaseHandshake();
                _ = self.active_connections.fetchSub(1, .acq_rel);
                stream.close(self.io);
                continue;
            };
        }
    }

    pub fn requestShutdown(self: *Server) void {
        self.shutdown_flag.store(true, .release);
        self.shutdown_event.set(self.io);
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
        const timeout: std.Io.Timeout = .{ .duration = .{
            .raw = .fromNanoseconds(@intCast(timeout_ns)),
            .clock = .awake,
        } };
        self.bind_event.waitTimeout(self.io, timeout) catch |err| switch (err) {
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

    pub fn deinit(self: *Server, gpa: std.mem.Allocator) void {
        std.debug.assert(gpa.vtable == self.gpa.vtable);
        std.debug.assert(self.active_connections.load(.acquire) == 0);
        std.debug.assert(self.accounting.active_streams.load(.acquire) == 0);
        std.debug.assert(self.accounting.reaper_reserved.load(.acquire) == 0);
        std.debug.assert(self.accounting.outbound_bytes.load(.acquire) == 0);
        std.debug.assert(self.accounting.request_bytes.load(.acquire) == 0);
        std.debug.assert(self.accounting.active_handshakes.load(.acquire) == 0);
        for (self.routes) |route| gpa.free(route.path);
        gpa.free(self.routes);
        gpa.free(self.endpoints);
        gpa.free(self.listeners);
        gpa.free(self.local_addrs);
        if (self.reaper) |*pool| pool.deinit();
        if (self.tls_auth) |*auth| {
            if (@hasDecl(@TypeOf(auth.*), "deinit")) auth.deinit(gpa);
        }
        self.* = undefined;
    }
};
