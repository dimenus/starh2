//! Load-aware connection placement for pinned scheduling.
//!
//! Under `-Dzio-scheduling=pinned` a connection and every task it spawns stay
//! on the executor its `connEntry` lands on, and zio's `.auto` is a
//! round-robin that ignores load, so two heavy connections can share one
//! executor while another idles. With a `Balancer` in `ServerConfig`, the
//! accept loop places each new connection on the executor with the fewest
//! live connections, then the fewest live task handlers, then the lowest
//! index.
//!
//! # Contract
//!
//! - One `Balancer` per `zio.Runtime`, shared by every `Server` on that
//!   runtime. Executor ids index this runtime's executors only, so a
//!   `Balancer` must never be shared across runtimes.
//! - The width is read from the runtime, never supplied by the caller, and
//!   is the runtime's real executor count (a single_executor build resolves
//!   any requested width to 1, and then nothing is balanced).
//! - It is a placement hint for the accept loop, not a promise: if the
//!   placed spawn fails for any reason, the server falls back to its normal
//!   `.auto` spawn. A placement error never closes a connection.
//! - Connections are counted from admission to `connEntry` exit. Task
//!   handlers (HTTP/2 and HTTP/1.1) are counted while they run. Inline
//!   (complete) handlers are not, which is why connections come first:
//!   ranking by handlers first let one mostly-sleeping SSE handler steer every
//!   inline-only connection onto the other executors.
//! - Placement is decided once, at accept. A connection that becomes hot
//!   later is not moved; nothing under pinned scheduling can move it.
const std = @import("std");
const zio = @import("zio");

pub const Balancer = struct {
    conns: []std.atomic.Value(u32),
    handlers: []std.atomic.Value(u32),

    pub const InitError = error{ OutOfMemory, TasksMigrate };

    /// Size the counters by `rt`'s executor count. Work stealing is refused:
    /// there a placed task does not stay placed, and zio rejects the fixed
    /// placement the accept loop would ask for.
    pub fn init(gpa: std.mem.Allocator, rt: *zio.Runtime) InitError!Balancer {
        if (comptime @import("build_options").zio_scheduling == .work_stealing) return error.TasksMigrate;
        const n = rt.executors.items.len;
        std.debug.assert(n >= 1);
        const conns = try gpa.alloc(std.atomic.Value(u32), n);
        errdefer gpa.free(conns);
        const handlers = try gpa.alloc(std.atomic.Value(u32), n);
        for (conns) |*c| c.* = .init(0);
        for (handlers) |*h| h.* = .init(0);
        return .{ .conns = conns, .handlers = handlers };
    }

    /// Every connection and handler must be gone: a non-zero count here is a
    /// release some path forgot.
    pub fn deinit(self: *Balancer, gpa: std.mem.Allocator) void {
        for (self.conns) |*c| std.debug.assert(c.load(.acquire) == 0);
        for (self.handlers) |*h| std.debug.assert(h.load(.acquire) == 0);
        gpa.free(self.conns);
        gpa.free(self.handlers);
        self.* = undefined;
    }

    /// Choose an executor for a new connection and count it there. Null when
    /// there is nothing to choose (one executor). The count is reserved with
    /// a compare-exchange, so two accept loops that read the same minimum do
    /// not both place onto it.
    pub fn reserve(self: *Balancer) ?zio.ExecutorId {
        if (self.conns.len < 2) return null;
        while (true) {
            var best: usize = 0;
            var best_c = self.conns[0].load(.acquire);
            var best_h = self.handlers[0].load(.acquire);
            for (self.conns[1..], self.handlers[1..], 1..) |*cv, *hv, i| {
                const c = cv.load(.acquire);
                const h = hv.load(.acquire);
                if (c < best_c or (c == best_c and h < best_h)) {
                    best = i;
                    best_c = c;
                    best_h = h;
                }
            }
            if (self.conns[best].cmpxchgWeak(best_c, best_c + 1, .acq_rel, .acquire) == null) {
                return @intCast(best);
            }
        }
    }

    pub fn releaseConn(self: *Balancer, ei: zio.ExecutorId) void {
        const prev = self.conns[ei].fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
    }

    pub fn noteHandlerStart(self: *Balancer, ei: zio.ExecutorId) void {
        _ = self.handlers[ei].fetchAdd(1, .acq_rel);
    }

    pub fn noteHandlerEnd(self: *Balancer, ei: zio.ExecutorId) void {
        const prev = self.handlers[ei].fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
    }
};
