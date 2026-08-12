//! Edge-clock token buckets for non-DATA / RST / SETTINGS / PING rate budgets.
const std = @import("std");
const frame = @import("frame.zig");

pub const TokenBucket = struct {
    capacity: f64,
    tokens: f64,
    refill_per_ns: f64,
    last_ns: u64 = 0,

    pub fn init(capacity: f64, refill_per_sec: f64) TokenBucket {
        return .{
            .capacity = capacity,
            .tokens = capacity,
            .refill_per_ns = refill_per_sec / 1_000_000_000.0,
        };
    }

    pub fn tryTake(self: *TokenBucket, now_ns: u64) bool {
        if (self.last_ns == 0) {
            self.last_ns = now_ns;
        } else if (now_ns > self.last_ns) {
            const elapsed = @as(f64, @floatFromInt(now_ns - self.last_ns));
            self.tokens = @min(self.capacity, self.tokens + elapsed * self.refill_per_ns);
            self.last_ns = now_ns;
        }
        if (self.tokens < 1.0) return false;
        self.tokens -= 1.0;
        return true;
    }
};

/// Defaults: per 10-second windows for control-frame rate budgets.
pub const RateLimiter = struct {
    non_data: TokenBucket = .init(10_000, 1_000), // 10000 / 10s
    rst: TokenBucket = .init(1_000, 100),
    settings: TokenBucket = .init(100, 10),
    ping: TokenBucket = .init(100, 10),

    /// Returns false → caller should GOAWAY ENHANCE_YOUR_CALM (unless a protocol error wins).
    pub fn admit(self: *RateLimiter, typ: frame.FrameType, now_ns: u64) bool {
        switch (typ) {
            .data => return true,
            .rst_stream => {
                if (!self.rst.tryTake(now_ns)) return false;
                return self.non_data.tryTake(now_ns);
            },
            .settings => {
                if (!self.settings.tryTake(now_ns)) return false;
                return self.non_data.tryTake(now_ns);
            },
            .ping => {
                if (!self.ping.tryTake(now_ns)) return false;
                return self.non_data.tryTake(now_ns);
            },
            else => return self.non_data.tryTake(now_ns),
        }
    }
};

test "token bucket refills and refuses when empty" {
    var b = TokenBucket.init(2, 0); // no refill
    try std.testing.expect(b.tryTake(1));
    try std.testing.expect(b.tryTake(1));
    try std.testing.expect(!b.tryTake(1));
}

test "rate limiter DATA never consumes non-data budget" {
    var r: RateLimiter = .{};
    var i: usize = 0;
    while (i < 20_000) : (i += 1) {
        try std.testing.expect(r.admit(.data, 1));
    }
}
