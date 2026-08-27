//! One-shot HTTP/1.1 over `std.Io`.
//!
//! A connection helper beside the HTTP/2 server, not inside it. The h2
//! session, preface matcher, and frame parser are not involved: this module
//! reads and writes HTTP/1.1 through a buffered `std.Io` stream and an
//! explicit `flush`.
//!
//! One request, one response, then the connection ends. After the response
//! is flushed the write side is shut down (`shutdown(.send)`), which is the
//! TCP half-close `Connection: close` promised. Keep-alive waits.
//!
//! The client returns when `Content-Length` bytes have arrived. It does not
//! wait for peer close. A final response without a length is an error, not
//! an invitation to read until EOF.
const std = @import("std");
const builtin = @import("builtin");
const request_mod = @import("../http/request.zig");
const codec = @import("codec.zig");

pub const Header = codec.Header;
pub const Limits = codec.Limits;
pub const Request = request_mod.Request;

pub const Handler = struct {
    ptr: *anyopaque,
    runFn: *const fn (*anyopaque, *const Request, *Reply) anyerror!void,
};

/// Complete one-shot reply. `start` / streaming are not on this type.
pub const Reply = struct {
    status: u16 = 200,
    headers: []const Header = &.{},
    body: []const u8 = &.{},
    sent: bool = false,

    pub fn send(self: *Reply, status: u16, headers: []const Header, body: []const u8) error{ ResponseCommitted, InvalidStatus }!void {
        if (self.sent) return error.ResponseCommitted;
        if (status < 100 or status > 599) return error.InvalidStatus;
        self.status = status;
        self.headers = headers;
        self.body = body;
        self.sent = true;
    }
};

pub const Response = struct {
    status: u16,
    reason: []const u8,
    headers: []const Header,
    body: []const u8,
    connection_close: bool,
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *Response) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const BindState = enum(u8) {
    pending,
    listening,
    failed,
};

pub const InitError = error{InvalidConfig};
pub const ServeError = error{
    ListenFailed,
    TransportSetupFailed,
    RuntimeShutdown,
};

pub const ServerConfig = struct {
    address: std.Io.net.IpAddress,
    handler: Handler,
    limits: Limits = .{},
    max_connections: usize = 64,
};

pub const Server = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
    handler: Handler,
    limits: Limits,
    max_connections: usize,
    listener: ?std.Io.net.Server = null,
    local_addr: std.Io.net.IpAddress = undefined,
    shutdown_flag: std.atomic.Value(bool) = .init(false),
    shutdown_event: std.Io.Event = .unset,
    bind_state: std.atomic.Value(BindState) = .init(.pending),
    bind_event: std.Io.Event = .unset,
    active_connections: std.atomic.Value(usize) = .init(0),

    pub fn init(gpa: std.mem.Allocator, io: std.Io, config: ServerConfig) InitError!Server {
        if (config.max_connections == 0) return error.InvalidConfig;
        if (config.limits.header_bytes == 0 or config.limits.header_fields == 0) return error.InvalidConfig;
        return .{
            .gpa = gpa,
            .io = io,
            .address = config.address,
            .handler = config.handler,
            .limits = config.limits,
            .max_connections = config.max_connections,
        };
    }

    pub fn serve(self: *Server) ServeError!void {
        errdefer {
            self.closeListener();
            self.bind_state.store(.failed, .release);
            self.bind_event.set(self.io);
        }

        self.listener = self.address.listen(self.io, .{ .reuse_address = true }) catch return error.ListenFailed;
        self.local_addr = self.listener.?.socket.address;

        var accept_group: std.Io.Group = .init;
        var connection_group: std.Io.Group = .init;
        accept_group.concurrent(self.io, acceptLoop, .{ self, &connection_group }) catch {
            accept_group.cancel(self.io);
            connection_group.cancel(self.io);
            return error.TransportSetupFailed;
        };

        self.bind_state.store(.listening, .release);
        self.bind_event.set(self.io);

        const canceled = blk: {
            self.shutdown_event.wait(self.io) catch break :blk true;
            break :blk false;
        };
        self.shutdown_flag.store(true, .release);
        self.shutdown_event.set(self.io);
        accept_group.cancel(self.io);
        self.closeListener();

        if (canceled) {
            connection_group.cancel(self.io);
        } else {
            connection_group.await(self.io) catch {
                connection_group.cancel(self.io);
            };
        }
        if (canceled) return error.RuntimeShutdown;
    }

    pub fn requestShutdown(self: *Server) void {
        self.shutdown_flag.store(true, .release);
        self.shutdown_event.set(self.io);
    }

    pub const WaitListeningError = error{ BindFailed, Timeout, Canceled };

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

    pub fn localAddress(self: *const Server) std.Io.net.IpAddress {
        return self.local_addr;
    }

    pub fn deinit(self: *Server, gpa: std.mem.Allocator) void {
        std.debug.assert(gpa.vtable == self.gpa.vtable);
        std.debug.assert(self.active_connections.load(.acquire) == 0);
        self.closeListener();
        self.* = undefined;
    }

    fn closeListener(self: *Server) void {
        if (self.listener) |*listener| {
            listener.socket.close(self.io);
            self.listener = null;
        }
    }

    fn tryAdmit(self: *Server) bool {
        while (true) {
            const current = self.active_connections.load(.acquire);
            if (current >= self.max_connections) return false;
            if (self.active_connections.cmpxchgWeak(current, current + 1, .acq_rel, .acquire) == null) return true;
        }
    }

    fn acceptLoop(self: *Server, connection_group: *std.Io.Group) std.Io.Cancelable!void {
        while (!self.shutdown_flag.load(.acquire)) {
            const listener = if (self.listener) |*l| l else return;
            const stream = listener.accept(self.io) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                error.ConnectionAborted, error.SocketNotListening => {
                    if (self.shutdown_flag.load(.acquire)) return;
                    continue;
                },
                else => return,
            };
            setTcpNoDelay(stream) catch {};
            if (!self.tryAdmit()) {
                stream.close(self.io);
                continue;
            }
            connection_group.concurrent(self.io, connEntry, .{ self, stream }) catch {
                _ = self.active_connections.fetchSub(1, .acq_rel);
                stream.close(self.io);
                continue;
            };
        }
    }

    fn connEntry(self: *Server, stream: std.Io.net.Stream) std.Io.Cancelable!void {
        defer _ = self.active_connections.fetchSub(1, .acq_rel);
        serveConn(self.io, stream, self.gpa, self.limits, self.handler);
    }
};

/// One accepted stream: read one request, write one response, half-close, close.
pub fn serveConn(
    io: std.Io,
    stream: std.Io.net.Stream,
    gpa: std.mem.Allocator,
    limits: Limits,
    handler: Handler,
) void {
    defer stream.close(io);
    var read_buf: [8 * 1024]u8 = undefined;
    var write_buf: [8 * 1024]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);
    serveOne(&reader.interface, &writer.interface, gpa, limits, handler) catch {};
    stream.shutdown(io, .send) catch {};
}

pub fn get(
    io: std.Io,
    gpa: std.mem.Allocator,
    address: std.Io.net.IpAddress,
    host: []const u8,
    path: []const u8,
) !Response {
    return exchange(io, gpa, address, "GET", path, host, &.{}, "", .{});
}

pub fn exchange(
    io: std.Io,
    gpa: std.mem.Allocator,
    address: std.Io.net.IpAddress,
    method: []const u8,
    path: []const u8,
    host: []const u8,
    headers: []const Header,
    body: []const u8,
    limits: Limits,
) !Response {
    const stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var read_buf: [8 * 1024]u8 = undefined;
    var write_buf: [8 * 1024]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);
    try codec.writeRequest(&writer.interface, method, path, host, headers, body);
    try writer.interface.flush();
    // We advertised Connection: close; stop sending so the peer can finish.
    stream.shutdown(io, .send) catch {};
    return readResponse(&reader.interface, gpa, limits);
}

pub fn readResponse(reader: *std.Io.Reader, gpa: std.mem.Allocator, limits: Limits) !Response {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    errdefer arena_state.deinit();
    const arena = arena_state.allocator();

    const head = try readHead(reader, arena, limits.header_bytes);
    const storage = try arena.alloc(Header, limits.header_fields);
    const parsed = try codec.parseResponseHead(head, storage, limits);
    const body = try readBody(reader, arena, parsed.content_length, limits);
    return .{
        .status = parsed.status,
        .reason = parsed.reason,
        .headers = parsed.headers,
        .body = body,
        .connection_close = parsed.connection_close,
        .arena = arena_state,
    };
}

fn serveOne(
    reader: *std.Io.Reader,
    writer: *std.Io.Writer,
    gpa: std.mem.Allocator,
    limits: Limits,
    handler: Handler,
) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const head = readHead(reader, arena, limits.header_bytes) catch |err| {
        try writeStatus(writer, statusFor(err));
        return;
    };
    const storage = try arena.alloc(Header, limits.header_fields);
    const parsed = codec.parseRequestHead(head, storage, limits) catch |err| {
        try writeStatus(writer, statusFor(err));
        return;
    };
    if (codec.methodRequiresLength(parsed.method_raw) and parsed.content_length == null) {
        try writeStatus(writer, 411);
        return;
    }
    const body = readBody(reader, arena, parsed.content_length, limits) catch |err| {
        try writeStatus(writer, statusFor(err));
        return;
    };

    const req: Request = .{
        .method = request_mod.Method.parse(parsed.method_raw),
        .scheme = "http",
        .authority = parsed.host,
        .path = parsed.path,
        .query = parsed.query,
        .headers = parsed.headers,
        .body = body,
        .trailers = &.{},
        .arena = arena,
    };
    var reply: Reply = .{};
    handler.runFn(handler.ptr, &req, &reply) catch {
        if (!reply.sent) {
            try writeStatus(writer, 500);
            return;
        }
    };
    if (!reply.sent) {
        try writeStatus(writer, 500);
        return;
    }
    try codec.writeResponse(writer, reply.status, reply.headers, reply.body);
    try writer.flush();
}

fn writeStatus(writer: *std.Io.Writer, status: u16) !void {
    try codec.writeResponse(writer, status, &.{}, "");
    try writer.flush();
}

fn statusFor(err: anyerror) u16 {
    return switch (err) {
        error.HeaderTooLarge, error.TooManyHeaders => 431,
        error.MissingContentLength => 411,
        error.BodyTooLarge => 413,
        else => 400,
    };
}

fn readHead(reader: *std.Io.Reader, gpa: std.mem.Allocator, max: usize) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(gpa);
    var end: codec.HeadEnd = .{};
    while (!end.done()) {
        if (buf.items.len >= max) return error.HeaderTooLarge;
        const avail = reader.peekGreedy(1) catch |err| switch (err) {
            error.EndOfStream => return error.UnexpectedEof,
            else => return err,
        };
        const room = max - buf.items.len;
        const slice = avail[0..@min(avail.len, room)];
        const n = end.feed(slice);
        try buf.appendSlice(gpa, slice[0..n]);
        reader.toss(n);
        if (!end.done() and n == room) return error.HeaderTooLarge;
    }
    return buf.toOwnedSlice(gpa);
}

fn readBody(reader: *std.Io.Reader, gpa: std.mem.Allocator, content_length: ?u64, limits: Limits) ![]u8 {
    const n = content_length orelse return "";
    if (n > limits.body_bytes) return error.BodyTooLarge;
    const len: usize = std.math.cast(usize, n) orelse return error.BodyTooLarge;
    if (len == 0) return "";
    const body = try gpa.alloc(u8, len);
    errdefer gpa.free(body);
    reader.readSliceAll(body) catch |err| switch (err) {
        error.EndOfStream => return error.UnexpectedEof,
        else => return err,
    };
    return body;
}

fn setTcpNoDelay(stream: std.Io.net.Stream) std.posix.SetSockOptError!void {
    if (builtin.os.tag == .windows) return;
    const on: c_int = 1;
    try std.posix.setsockopt(
        stream.socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        std.mem.asBytes(&on),
    );
}
