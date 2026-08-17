//! Route table lookup. A linear scan on purpose: the table is bounded by
//! `Limits.max_routes`, it is built once at server start, and it is read by
//! every request without a lock. A tree or a map would add build-time state and
//! an allocation to save a scan over a list that is typically tens of entries.
//!
//! # Match order, and why it is this way
//!
//! 1. EXACT paths, always first. Adding a prefix route must never change what
//!    an existing exact route answers.
//! 2. PREFIX paths, longest wins. Where `/v1/tasks/` and `/v1/tasks/special/`
//!    both match, the more specific one is the one the author meant.
//!
//! The 404-versus-405 distinction is kept at each tier. A path that exists but
//! carries the wrong method returns `method_not_allowed` with the methods that
//! do exist, so a client learns the difference between "no such resource" and
//! "wrong verb".
const std = @import("std");
const request = @import("request.zig");
const response = @import("response.zig");

/// A handler that may block, stream, or sleep. Always runs on its own task so
/// a stuck SSE or a database wait cannot stop ingest on that connection.
pub const TaskHandler = struct {
    ptr: *anyopaque,
    runFn: *const fn (*anyopaque, *const request.Request, *response.Response) anyerror!void,
};

/// A handler that only `CompleteResponse.send`s. May run on the connection
/// actor: no spawn, still FairScheduler → WritePump. A body larger than
/// `outbound_bytes_per_stream` cannot wait for slab space on the actor — use
/// a task handler for large or streaming replies.
pub const CompleteHandler = struct {
    ptr: *anyopaque,
    runFn: *const fn (*anyopaque, *const request.Request, *response.CompleteResponse) anyerror!void,
};

pub const Handler = union(enum) {
    complete: CompleteHandler,
    task: TaskHandler,
};

pub const Route = struct {
    method: request.Method,
    path: []const u8,
    /// When true, `path` is a prefix; exact routes are still tried first.
    prefix: bool = false,
    handler: Handler,
};

pub const Found = struct {
    handler: Handler,
    /// Bytes of the request path after a matched prefix; empty for exact matches.
    path_remainder: []const u8 = "",
};

pub const Match = union(enum) {
    found: Found,
    not_found,
    method_not_allowed: []const request.Method,
};

pub const Router = struct {
    routes: []const Route,

    pub fn match(self: Router, method: request.Method, path: []const u8) Match {
        // Exact routes first — 404/405 semantics for those paths stay unchanged.
        var path_exists = false;
        var allowed: [8]request.Method = undefined;
        var allowed_len: usize = 0;
        for (self.routes) |r| {
            if (r.prefix) continue;
            if (std.mem.eql(u8, r.path, path)) {
                path_exists = true;
                if (allowed_len < allowed.len) {
                    allowed[allowed_len] = r.method;
                    allowed_len += 1;
                }
                if (r.method == method) return .{ .found = .{ .handler = r.handler } };
            }
        }
        if (path_exists) return .{ .method_not_allowed = allowed[0..allowed_len] };

        // Prefix routes: longest prefix wins among method matches; 405 if only method differs.
        var best_len: usize = 0;
        var best: ?Found = null;
        var prefix_path_exists = false;
        allowed_len = 0;
        for (self.routes) |r| {
            if (!r.prefix) continue;
            if (!std.mem.startsWith(u8, path, r.path)) continue;
            prefix_path_exists = true;
            if (allowed_len < allowed.len) {
                // Dedup methods for 405 Allow list on this path family.
                var seen = false;
                for (allowed[0..allowed_len]) |m| {
                    if (m == r.method) {
                        seen = true;
                        break;
                    }
                }
                if (!seen) {
                    allowed[allowed_len] = r.method;
                    allowed_len += 1;
                }
            }
            if (r.method != method) continue;
            if (r.path.len >= best_len) {
                best_len = r.path.len;
                best = .{ .handler = r.handler, .path_remainder = path[r.path.len..] };
            }
        }
        if (best) |f| return .{ .found = f };
        if (prefix_path_exists) return .{ .method_not_allowed = allowed[0..allowed_len] };
        return .not_found;
    }
};

test "exact match unchanged" {
    const dummy: u8 = 0;
    const h: Handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = struct {
        fn f(_: *anyopaque, _: *const request.Request, _: *response.Response) anyerror!void {}
    }.f } };
    const routes = [_]Route{
        .{ .method = .GET, .path = "/hello", .handler = h },
        .{ .method = .POST, .path = "/hello", .handler = h },
    };
    const r: Router = .{ .routes = &routes };
    switch (r.match(.GET, "/hello")) {
        .found => |f| try std.testing.expectEqual(@as(usize, 0), f.path_remainder.len),
        else => return error.ExpectedFound,
    }
    switch (r.match(.PUT, "/hello")) {
        .method_not_allowed => |m| try std.testing.expectEqual(@as(usize, 2), m.len),
        else => return error.Expected405,
    }
    try std.testing.expect(r.match(.GET, "/missing") == .not_found);
}

test "prefix match yields remainder; exact wins over prefix" {
    const dummy: u8 = 0;
    const h: Handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = struct {
        fn f(_: *anyopaque, _: *const request.Request, _: *response.Response) anyerror!void {}
    }.f } };
    const routes = [_]Route{
        .{ .method = .GET, .path = "/v1/tasks/special", .handler = h },
        .{ .method = .GET, .path = "/v1/tasks/", .prefix = true, .handler = h },
        .{ .method = .POST, .path = "/v1/tasks/", .prefix = true, .handler = h },
    };
    const r: Router = .{ .routes = &routes };
    switch (r.match(.GET, "/v1/tasks/special")) {
        .found => |f| try std.testing.expectEqual(@as(usize, 0), f.path_remainder.len),
        else => return error.ExpectedExact,
    }
    switch (r.match(.GET, "/v1/tasks/abc")) {
        .found => |f| try std.testing.expectEqualStrings("abc", f.path_remainder),
        else => return error.ExpectedPrefix,
    }
    switch (r.match(.DELETE, "/v1/tasks/abc")) {
        .method_not_allowed => |m| try std.testing.expectEqual(@as(usize, 2), m.len),
        else => return error.Expected405,
    }
    try std.testing.expect(r.match(.GET, "/other") == .not_found);
}

test "complete and task handlers both match" {
    const dummy: u8 = 0;
    const complete: Handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = struct {
        fn f(_: *anyopaque, _: *const request.Request, _: *response.CompleteResponse) anyerror!void {}
    }.f } };
    const task: Handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = struct {
        fn f(_: *anyopaque, _: *const request.Request, _: *response.Response) anyerror!void {}
    }.f } };
    const routes = [_]Route{
        .{ .method = .GET, .path = "/", .handler = complete },
        .{ .method = .GET, .path = "/sse", .handler = task },
    };
    const r: Router = .{ .routes = &routes };
    switch (r.match(.GET, "/")) {
        .found => |f| try std.testing.expect(f.handler == .complete),
        else => return error.ExpectedComplete,
    }
    switch (r.match(.GET, "/sse")) {
        .found => |f| try std.testing.expect(f.handler == .task),
        else => return error.ExpectedTask,
    }
}
