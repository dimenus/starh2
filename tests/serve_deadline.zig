//! A deadline on `Server.serve` for tests.
//!
//! `serve` never returns while a handler still runs (`Server.waitForHandlers`),
//! so a test that leaks a charge, or leaves a handler stuck, does not fail: it
//! hangs. `join` turns that hang into a loud failure that names what `serve`
//! is waiting for. Use it in place of `handle.join()` after
//! `server.requestShutdown()`.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

/// How long `serve` may take to return after shutdown was requested. Longer
/// than any drain a test configures on purpose (the default drain is 30 s
/// plus a 1 s first phase, and a test that keeps the default and leaves a
/// handler running for it waits that long before the cancel).
pub const default_timeout_ns: u64 = 45 * std.time.ns_per_s;

/// Wait for `handle` (a `zio.JoinHandle` of `Server.serve`) to finish, up to
/// `default_timeout_ns`. Past it, panic with the counts `serve` is waiting
/// for: a test cannot go on, since `deinit` must not run under a live serve.
/// `server` is a `*Server` or a `**Server`.
pub fn join(server: anytype, handle: anytype) void {
    joinWithin(server, handle, default_timeout_ns);
}

pub fn joinWithin(server: anytype, handle: anytype, timeout_ns: u64) void {
    const s: *starh2.Server = switch (@TypeOf(server)) {
        *starh2.Server => server,
        **starh2.Server => server.*,
        *const *starh2.Server => server.*,
        else => @compileError("serve_deadline.join wants a *Server"),
    };
    const start = now();
    var pause_ms: u64 = 1;
    while (!handle.hasResult()) {
        if (now() - start >= timeout_ns) {
            std.debug.panic("serve did not return {d} ms after shutdown: {d} task handlers ({d} orphaned), {d} connections, {d} streams still counted; {d} shutdown reports written", .{
                timeout_ns / std.time.ns_per_ms,
                s.accounting.task_handlers.load(.acquire),
                s.accounting.orphaned_handlers.load(.acquire),
                s.active_connections.load(.acquire),
                s.accounting.active_streams.load(.acquire),
                s.shutdown_reports.load(.acquire),
            });
        }
        zio.sleep(.fromMilliseconds(pause_ms)) catch {};
        pause_ms = @min(pause_ms * 2, 20);
    }
    handle.join() catch {};
}

fn now() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}
