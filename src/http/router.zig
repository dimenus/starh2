const std = @import("std");
const request = @import("request.zig");
const response = @import("response.zig");

pub const Handler = struct {
    ptr: *anyopaque,
    runFn: *const fn (*anyopaque, *const request.Request, *response.Response) anyerror!void,
};

pub const Route = struct {
    method: request.Method,
    path: []const u8,
    handler: Handler,
};

pub const Match = union(enum) {
    found: Handler,
    not_found,
    method_not_allowed: []const request.Method,
};

pub const Router = struct {
    routes: []const Route,

    pub fn match(self: Router, method: request.Method, path: []const u8) Match {
        var path_exists = false;
        var allowed: [8]request.Method = undefined;
        var allowed_len: usize = 0;
        for (self.routes) |r| {
            if (std.mem.eql(u8, r.path, path)) {
                path_exists = true;
                if (allowed_len < allowed.len) {
                    allowed[allowed_len] = r.method;
                    allowed_len += 1;
                }
                if (r.method == method) return .{ .found = r.handler };
            }
        }
        if (!path_exists) return .not_found;
        return .{ .method_not_allowed = allowed[0..allowed_len] };
    }
};
