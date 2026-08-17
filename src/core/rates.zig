//! Edge-clock token buckets for non-DATA / RST / SETTINGS / PING rate budgets.
//!
//! These buckets exist for one attack family. A frame that costs the server
//! work but no flow-control credit is free for the peer to repeat. RST_STREAM
//! is the well-known case (CVE-2023-44487): a client opens a stream, the server
//! allocates for it, the client resets it at once, and no window ever fills.
//! SETTINGS and PING have the same shape, because each one forces an ack.
//!
//! The buckets do not read a clock. `Session` passes `edge_now_ns`, the
//! timestamp that the connection actor already took. That keeps `core`
//! deterministic and testable: a test drives the rate limiter by choosing the
//! nanosecond value, and needs no wall-clock wait.
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
    /// WINDOW_UPDATE gets its OWN budget and does not charge `non_data`.
    ///
    /// It is the credit for DATA already delivered, so its rate scales with
    /// useful traffic: a connection serving many small frames returns credit
    /// far faster than the 1,000/s the shared bucket allows. Charging it there
    /// silently capped one connection at about 121k SSE events/s — silently,
    /// because the peer kept every stream open and nothing was logged. Two
    /// independent implementations of an unrelated change both plateaued on
    /// exactly that number until this bucket was sized; see
    /// `tools/sse_bench/run.sh`.
    ///
    /// It is NOT exempt like DATA. DATA is exempt because it is self-limiting:
    /// a peer cannot send more than the window allows. WINDOW_UPDATE has no
    /// such property — a peer can send it forever while receiving nothing — so
    /// removing the charge entirely would remove the flood control instead of
    /// sizing it. This bucket is generous enough for credit that tracks real
    /// delivery and still bounds a flood.
    window_update: TokenBucket = .init(100_000, 10_000),
    /// HEADERS gets its OWN budget and does not charge `non_data`.
    ///
    /// A one-shot request is a HEADERS frame. Useful request rate therefore
    /// scales with this bucket the same way WINDOW_UPDATE scales with DATA
    /// credit. Charging HEADERS against the shared 10,000 / 1,000/s bucket
    /// GOAWAY'd a connection after ~10k requests (`ENHANCE_YOUR_CALM`) and
    /// showed up as h2load `started < total` — the not-started class, TLS and
    /// h2c alike. h2load's 2-client threads assign 12.5k requests each at
    /// `-c 10 -n 100000`, which is past that burst; the leftover is
    /// `Process Request Failure`.
    ///
    /// It is NOT exempt like DATA. A peer can open streams forever without
    /// filling a window. Rapid-reset (HEADERS then RST) still hits `rst` and
    /// `non_data`; this bucket bounds stream creation that never resets.
    headers: TokenBucket = .init(100_000, 10_000),

    /// Returns false → caller should GOAWAY ENHANCE_YOUR_CALM (unless a protocol error wins).
    ///
    /// Two buckets charge each abusive type: its own bucket, then the shared
    /// `non_data` bucket. The specific bucket stops one type on its own. The
    /// shared bucket stops a peer that spreads the same total volume across
    /// several types to stay under each individual limit.
    ///
    /// DATA is exempt and takes no token. DATA already pays flow control, so a
    /// peer cannot repeat it for free. A token charge here would let the rate
    /// limiter refuse a legitimate large upload.
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
            // Its own bucket only. See the field comment for why it is neither
            // exempt nor charged against the shared budget.
            .window_update => return self.window_update.tryTake(now_ns),
            .headers => return self.headers.tryTake(now_ns),
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

test "rate limiter WINDOW_UPDATE does not consume the shared non-data budget" {
    var r: RateLimiter = .{};
    var i: usize = 0;
    // Far more than the shared bucket holds, at a rate a real fast connection
    // reaches. The shared budget must be untouched afterwards.
    while (i < 50_000) : (i += 1) {
        try std.testing.expect(r.admit(.window_update, 1));
    }
    try std.testing.expect(r.admit(.ping, 1));
}

test "rate limiter WINDOW_UPDATE flood is still refused" {
    // The defence is kept, not removed: a peer that sends credit it cannot
    // possibly have earned runs its own bucket dry and gets refused. Without
    // this, exempting WINDOW_UPDATE would look identical to sizing it.
    var r: RateLimiter = .{};
    var i: usize = 0;
    while (i < 100_000) : (i += 1) {
        _ = r.admit(.window_update, 1);
    }
    try std.testing.expect(!r.admit(.window_update, 1));
}

test "rate limiter DATA never consumes non-data budget" {
    var r: RateLimiter = .{};
    var i: usize = 0;
    while (i < 20_000) : (i += 1) {
        try std.testing.expect(r.admit(.data, 1));
    }
}

test "rate limiter HEADERS does not consume the shared non-data budget" {
    var r: RateLimiter = .{};
    var i: usize = 0;
    // Past the shared 10k burst, at a frozen timestamp so refill cannot help.
    // Putting HEADERS back on `non_data` fails here and is the not-started bug.
    while (i < 20_000) : (i += 1) {
        try std.testing.expect(r.admit(.headers, 1));
    }
    try std.testing.expect(r.admit(.ping, 1));
}

test "rate limiter HEADERS flood is still refused" {
    var r: RateLimiter = .{};
    var i: usize = 0;
    while (i < 100_000) : (i += 1) {
        _ = r.admit(.headers, 1);
    }
    try std.testing.expect(!r.admit(.headers, 1));
}
