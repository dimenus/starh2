//! Listen, accept, global limits, graceful stop.
const std = @import("std");
const zio = @import("zio");
const tls = @import("tls");
const limits_mod = @import("../core/limits.zig");
const router_mod = @import("../http/router.zig");
const connection = @import("connection.zig");

pub const EndpointAddress = zio.net.IpAddress;

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

const ConnSlot = struct {
    handle: ?zio.JoinHandle(void) = null,
    occupied: bool = false,
};

/// Observable progress of `serve`'s startup. `serve` binds inside the spawned
/// task, so a caller that only holds the JoinHandle cannot tell "listening" from
/// "about to fail BindFailed" without racing on a sleep. Embedders gate their
/// own cutover on this (qmdsync disables its HTTP/1.1 UI only once the h2
/// listener is up), so the answer has to be authoritative, not timed.
pub const BindState = enum(u8) {
    /// serve() has not finished binding and spawning accept loops yet.
    pending,
    /// Every endpoint is bound and its accept loop is running.
    listening,
    /// serve() gave up during startup and is returning an error.
    failed,
};

pub const Server = struct {
    gpa: std.mem.Allocator,
    runtime: *zio.Runtime,
    limits: limits_mod.Limits,
    routes: []router_mod.Route,
    endpoints: []EndpointConfig,
    listeners: []zio.net.Server,
    local_addrs: []EndpointAddress,
    tls_auth: ?tls.config.CertKeyPair = null,
    shutdown_flag: std.atomic.Value(bool) = .init(false),
    active_connections: std.atomic.Value(usize) = .init(0),
    accounting: connection.GlobalAccounting,
    router: router_mod.Router = undefined,
    reaper: ?connection.ReaperPool = null,
    reaper_handles: []?zio.JoinHandle(void) = &.{},
    reaper_started: usize = 0,
    conn_slots: []ConnSlot = &.{},
    conn_slots_mu: zio.Mutex = .init,
    listeners_bound: usize = 0,
    bind_state: std.atomic.Value(BindState) = .init(.pending),

    pub fn init(gpa: std.mem.Allocator, runtime: *zio.Runtime, config: ServerConfig) InitError!Server {
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
        for (config.routes) |r| route_bytes += r.path.len;
        if (route_bytes > config.limits.max_route_path_bytes) return error.InvalidRoute;

        const routes = try gpa.alloc(router_mod.Route, config.routes.len);
        errdefer {
            for (routes) |r| {
                if (r.path.len != 0) gpa.free(r.path);
            }
            gpa.free(routes);
        }
        for (routes) |*r| r.* = .{ .method = .GET, .path = &.{}, .handler = undefined };
        for (config.routes, 0..) |r, i| {
            routes[i] = .{
                .method = r.method,
                .path = try gpa.dupe(u8, r.path),
                .prefix = r.prefix,
                .handler = r.handler,
            };
        }

        const endpoints = try gpa.dupe(EndpointConfig, config.endpoints);
        errdefer gpa.free(endpoints);

        var tls_auth: ?tls.config.CertKeyPair = null;
        errdefer if (tls_auth) |*a| {
            if (@hasDecl(@TypeOf(a.*), "deinit")) a.deinit(gpa);
        };
        if (config.tls) |t| {
            if (t.certificate_chain_pem.len > config.limits.certificate_chain_bytes) return error.CertificateTooLarge;
            if (t.private_key_pem.len > config.limits.private_key_bytes) return error.PrivateKeyTooLarge;
            tls_auth = loadCertKey(gpa, t) catch return error.InvalidCertificate;
        }

        const listeners = try gpa.alloc(zio.net.Server, config.endpoints.len);
        errdefer gpa.free(listeners);
        const local_addrs = try gpa.alloc(EndpointAddress, config.endpoints.len);
        errdefer gpa.free(local_addrs);

        var reaper = connection.ReaperPool.init(gpa, config.limits.cancellation_reaper_jobs) catch return error.OutOfMemory;
        errdefer reaper.deinit();

        const reaper_handles = try gpa.alloc(?zio.JoinHandle(void), config.limits.cancellation_reaper_tasks);
        errdefer gpa.free(reaper_handles);
        @memset(reaper_handles, null);

        const conn_slots = try gpa.alloc(ConnSlot, config.limits.max_connections);
        errdefer gpa.free(conn_slots);
        @memset(conn_slots, .{});

        var self: Server = .{
            .gpa = gpa,
            .runtime = runtime,
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
            .reaper_handles = reaper_handles,
            .conn_slots = conn_slots,
        };
        self.router = .{ .routes = self.routes };
        return self;
    }

    fn loadCertKey(gpa: std.mem.Allocator, t: TlsConfig) !tls.config.CertKeyPair {
        var threaded = std.Io.Threaded.init(gpa, .{});
        defer threaded.deinit();
        const io = threaded.io();
        return tls.config.CertKeyPair.fromSlice(gpa, io, t.certificate_chain_pem, t.private_key_pem);
    }

    pub fn serve(self: *Server, gpa: std.mem.Allocator) ServeError!void {
        std.debug.assert(gpa.vtable == self.gpa.vtable);
        self.listeners_bound = 0;
        self.bind_state.store(.pending, .release);
        errdefer {
            var i: usize = 0;
            while (i < self.listeners_bound) : (i += 1) self.listeners[i].close();
            self.listeners_bound = 0;
        }
        // Fires on every error return below (bind, reaper spawn, accept spawn),
        // so a waiter is released with .failed instead of waiting out its timeout.
        // Nothing after the .listening store can return an error.
        errdefer self.bind_state.store(.failed, .release);
        for (self.endpoints, 0..) |ep, i| {
            const addr = switch (ep) {
                .tls_h2 => |a| a,
                .h2c_prior_knowledge => |a| a,
            };
            const listener = addr.listen(.{ .reuse_address = true }) catch return error.ListenFailed;
            self.listeners[i] = listener;
            self.local_addrs[i] = listener.socket.address.ip;
            self.listeners_bound = i + 1;
        }

        var group: zio.Group = .init;
        var accepts_started: usize = 0;

        // Reapers must outlive connections. On any failure after accepts can admit
        // work: shutdown → cancel accepts → close listeners → join connections → then reapers.
        const teardown = struct {
            fn go(srv: *Server, g: *zio.Group, started: *usize) void {
                srv.shutdown_flag.store(true, .release);
                g.cancel();
                var li: usize = 0;
                while (li < srv.listeners_bound) : (li += 1) srv.listeners[li].close();
                srv.listeners_bound = 0;
                if (started.* > 0) srv.joinAllConnections();
                srv.shutdownReapers();
            }
        }.go;

        if (self.reaper) |*pool| {
            for (self.reaper_handles) |*slot| {
                slot.* = zio.spawn(connection.ReaperPool.worker, .{pool}) catch {
                    teardown(self, &group, &accepts_started);
                    return error.OutOfMemory;
                };
                self.reaper_started += 1;
            }
        }

        for (self.endpoints, 0..) |ep, i| {
            const mode: connection.Mode = switch (ep) {
                .tls_h2 => .tls_h2,
                .h2c_prior_knowledge => .h2c,
            };
            group.spawn(acceptLoop, .{ self, i, mode }) catch {
                teardown(self, &group, &accepts_started);
                return error.OutOfMemory;
            };
            accepts_started += 1;
        }

        // Published only once every endpoint is bound AND its accept loop is
        // live, so a released waiter can immediately connect.
        self.bind_state.store(.listening, .release);

        while (!self.shutdown_flag.load(.acquire)) {
            zio.sleep(.fromMilliseconds(50)) catch break;
        }

        teardown(self, &group, &accepts_started);
    }

    fn shutdownReapers(self: *Server) void {
        if (self.reaper) |*pool| {
            pool.shut.store(true, .release);
            // Close graceful: workers drain every queued JoinHandle before exiting.
            // Do not cancel workers — that would abandon unconsumed cancel jobs.
            pool.jobs.close(.graceful);
        }
        for (self.reaper_handles[0..self.reaper_started]) |*slot| {
            if (slot.*) |*h| {
                h.join();
                slot.* = null;
            }
        }
        self.reaper_started = 0;
    }

    fn joinAllConnections(self: *Server) void {
        while (true) {
            var taken: ?zio.JoinHandle(void) = null;
            self.conn_slots_mu.lockUncancelable();
            var pending = false;
            for (self.conn_slots) |*slot| {
                if (slot.handle) |h| {
                    taken = h;
                    slot.handle = null;
                    slot.occupied = false;
                    break;
                } else if (slot.occupied) {
                    pending = true;
                }
            }
            self.conn_slots_mu.unlock();
            if (taken) |*h| {
                h.join();
                continue;
            }
            if (!pending and self.active_connections.load(.acquire) == 0) break;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
    }

    fn reapFinishedConnections(self: *Server) void {
        self.conn_slots_mu.lockUncancelable();
        defer self.conn_slots_mu.unlock();
        for (self.conn_slots) |*slot| {
            if (slot.handle) |*h| {
                if (h.hasResult()) {
                    h.join();
                    slot.handle = null;
                    slot.occupied = false;
                }
            }
        }
    }

    fn reserveConnSlot(self: *Server) ?usize {
        self.reapFinishedConnections();
        self.conn_slots_mu.lockUncancelable();
        defer self.conn_slots_mu.unlock();
        for (self.conn_slots, 0..) |*slot, i| {
            if (!slot.occupied) {
                slot.occupied = true;
                slot.handle = null;
                return i;
            }
        }
        return null;
    }

    fn storeConnHandle(self: *Server, index: usize, handle: zio.JoinHandle(void)) void {
        self.conn_slots_mu.lockUncancelable();
        defer self.conn_slots_mu.unlock();
        self.conn_slots[index].handle = handle;
        self.conn_slots[index].occupied = true;
    }

    fn abortReserve(self: *Server, index: usize) void {
        self.conn_slots_mu.lockUncancelable();
        defer self.conn_slots_mu.unlock();
        self.conn_slots[index].handle = null;
        self.conn_slots[index].occupied = false;
    }

    fn connEntry(self: *Server, slot_i: usize, stream: zio.net.Stream, cfg: connection.ConnConfig) void {
        defer {
            _ = self.active_connections.fetchSub(1, .acq_rel);
        }
        _ = slot_i; // slot identity retained by server JoinHandle table
        connection.serveAccepted(stream, cfg);
    }

    fn acceptLoop(self: *Server, endpoint_index: usize, mode: connection.Mode) void {
        const listener = self.listeners[endpoint_index];
        while (!self.shutdown_flag.load(.acquire)) {
            const stream = listener.accept(.{}) catch continue;
            const slot_i = self.reserveConnSlot() orelse {
                stream.close();
                continue;
            };
            const cur = self.active_connections.load(.acquire);
            if (cur >= self.limits.max_connections) {
                self.abortReserve(slot_i);
                stream.close();
                continue;
            }
            var handshake_held = false;
            if (mode == .tls_h2) {
                if (!self.accounting.tryAdmitHandshake()) {
                    self.abortReserve(slot_i);
                    stream.close();
                    continue;
                }
                handshake_held = true;
            }
            _ = self.active_connections.fetchAdd(1, .acq_rel);
            const cfg = connection.ConnConfig{
                .mode = mode,
                .limits = self.limits,
                .router = self.router,
                .tls_auth = if (self.tls_auth) |*a| a else null,
                .gpa = self.gpa,
                .shutdown_flag = &self.shutdown_flag,
                .reaper = if (self.reaper) |*p| p else null,
                .accounting = &self.accounting,
                .handshake_held = handshake_held,
            };
            const handle = zio.spawn(connEntry, .{ self, slot_i, stream, cfg }) catch {
                if (handshake_held) self.accounting.releaseHandshake();
                _ = self.active_connections.fetchSub(1, .acq_rel);
                self.abortReserve(slot_i);
                stream.close();
                continue;
            };
            self.storeConnHandle(slot_i, handle);
        }
    }

    pub fn requestShutdown(self: *Server) void {
        self.shutdown_flag.store(true, .release);
    }

    pub const WaitListeningError = error{
        /// serve() failed during startup; join its handle for the specific error.
        BindFailed,
        /// Still pending when the budget ran out — serve() may not have been spawned.
        Timeout,
        Canceled,
    };

    /// Block the calling task until `serve` is accepting on every endpoint.
    ///
    /// Callers spawn `serve` and then need to know whether the socket is actually
    /// up before they act on it — `localAddress` reads uninitialized storage until
    /// this returns, and an embedder that tears down its fallback path on the
    /// strength of a spawn alone loses the listener silently when the bind fails.
    pub fn waitUntilListening(self: *Server, timeout_ns: u64) WaitListeningError!void {
        const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% timeout_ns;
        while (true) {
            switch (self.bind_state.load(.acquire)) {
                .listening => return,
                .failed => return error.BindFailed,
                .pending => {},
            }
            if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) {
                // Re-check: a transition can land between the load and the deadline.
                return switch (self.bind_state.load(.acquire)) {
                    .listening => {},
                    .failed => error.BindFailed,
                    .pending => error.Timeout,
                };
            }
            zio.sleep(.fromMilliseconds(1)) catch return error.Canceled;
        }
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
        for (self.routes) |r| gpa.free(r.path);
        gpa.free(self.routes);
        gpa.free(self.endpoints);
        gpa.free(self.listeners);
        gpa.free(self.local_addrs);
        if (self.reaper_handles.len != 0) gpa.free(self.reaper_handles);
        if (self.conn_slots.len != 0) gpa.free(self.conn_slots);
        if (self.reaper) |*p| p.deinit();
        if (self.tls_auth) |*a| {
            if (@hasDecl(@TypeOf(a.*), "deinit")) a.deinit(gpa);
        }
        self.* = undefined;
    }
};
