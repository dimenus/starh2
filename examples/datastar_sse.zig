const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

fn sse(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    const msg = "event: datastar-patch-elements\ndata: <div id=\"x\">hi</div>\n\n";
    try body.writeAll(msg);
    try body.finish();
}

const dummy: u8 = 0;

fn run(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 8080);
    var limits = starh2.Limits.defaults;
    limits.response_compression = true;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &.{.{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sse } } }},
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    try server.serve(gpa);
}

pub fn main() !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();
    const rt = try zio.Runtime.init(gpa, .{});
    defer rt.deinit();
    var h = try rt.spawn(run, .{ rt, gpa });
    h.join() catch {};
}
