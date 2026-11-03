//! PROXY protocol version 2, the receiving side (t-3759 step 3).
//!
//! # When the server reads it
//!
//! Only on an endpoint whose `proxy_protocol` is `.v2`, and only version 2
//! (the binary form; version 1 is text and is not accepted). The framing is
//! apart from the identity (`server.ClientIdentity`): every connection on such
//! an endpoint starts with a header, which the server reads and strips. With
//! `client_identity = .proxy_source`, for proxies that keep one upstream
//! connection per client (an NLB, HAProxy or nginx `stream`, Envoy TCP proxy),
//! the header names the client and the connection is keyed to it. Behind an
//! L7 proxy that pools connections, a header names whoever opened the pooled
//! connection, so such a deployment uses `.none`: the header is still
//! stripped, and `Request.peer` is its source, but no client is charged.
//!
//! # The order at accept
//!
//! 1. The trusted-source check, on the socket peer, before anything is read
//!    (`Trusted.contains`). An untrusted source is closed at once, never keyed
//!    to its own address. A Unix socket needs no list: only the host reaches
//!    it.
//! 2. A small cap of connections waiting for their header
//!    (`Limits.proxy_header_waiters`), its own, not the TLS handshake slots:
//!    on a TLS endpoint the handshake slot is taken only after the header.
//!    The cap bounds occupancy, not fairness: one stalled trusted proxy can
//!    fill it, and the log names it.
//! 3. The header, read exactly (no byte past it is consumed: what follows is
//!    the TLS ClientHello, the HTTP/2 preface or an HTTP/1.1 head), under the
//!    preface deadline (`Limits.preface_timeout_ns`, from accept).
//!
//! # The wire format (spec section 2.2)
//!
//! ```
//! 12 bytes  signature  \x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A
//!  1 byte   version (high nibble, must be 2) and command (low nibble:
//!           0 LOCAL, 1 PROXY)
//!  1 byte   address family (high nibble: 0 UNSPEC, 1 INET, 2 INET6,
//!           3 UNIX) and transport (low nibble: 0 UNSPEC, 1 STREAM, 2 DGRAM)
//!  2 bytes  length of what follows, big endian
//!  then     the addresses (INET: 4+4+2+2 bytes, INET6: 16+16+2+2, UNIX:
//!           108+108), then TLVs, which are ignored
//! ```
//!
//! What the server does with each:
//!
//! - `LOCAL`: a connection the proxy made for itself (a health check). The
//!   addresses are ignored, as the spec says, and the connection is charged
//!   to the endpoint's one infrastructure account.
//! - `PROXY` over INET or INET6 with STREAM: the source address is the client.
//!   An IPv4-mapped IPv6 source is the IPv4 client, as on a socket.
//! - `PROXY` over UNSPEC or UNIX: the client's address is not known. It is the
//!   one shared unknown identity, never the proxy's own address (which would
//!   put every user of that proxy on one key).
//! - Anything else (another version, another command, DGRAM, an address block
//!   shorter than its family needs) is malformed: the connection is closed.
//! - A length that would make the header longer than
//!   `Limits.proxy_header_bytes` is closed without reading the rest (the
//!   field allows 64 KiB).
const std = @import("std");
const request = @import("../http/request.zig");

pub const signature = [12]u8{ 0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A };

/// The fixed part: signature, version and command, family and transport,
/// length.
pub const fixed_bytes: usize = 16;

/// The smallest cap that holds every address block the server accepts
/// (UNIX, 216 bytes, after the fixed part). `Limits.proxy_header_bytes` may
/// not be lower.
pub const min_header_cap: usize = fixed_bytes + 216;

pub const Command = enum { local, proxy };

pub const Source = union(enum) {
    /// `LOCAL`: the proxy's own connection.
    local,
    /// `PROXY` with a client address.
    peer: request.Peer,
};

pub const FixedError = error{ BadSignature, BadVersion, BadCommand, TooLong };

/// What the fixed part says: the command and how many bytes follow. Checks the
/// signature, the version and the command, and that the whole header fits
/// `cap` bytes.
pub const Fixed = struct {
    command: Command,
    family: u8,
    rest: u16,
};

pub fn parseFixed(head: *const [fixed_bytes]u8, cap: usize) FixedError!Fixed {
    if (!std.mem.eql(u8, head[0..12], &signature)) return error.BadSignature;
    if (head[12] >> 4 != 2) return error.BadVersion;
    const command: Command = switch (head[12] & 0x0f) {
        0 => .local,
        1 => .proxy,
        else => return error.BadCommand,
    };
    const rest = std.mem.readInt(u16, head[14..16], .big);
    if (fixed_bytes + @as(usize, rest) > cap) return error.TooLong;
    return .{ .command = command, .family = head[13], .rest = rest };
}

pub const BodyError = error{ BadFamily, ShortAddress };

/// The client a complete header names, from its fixed part and the `rest`
/// bytes after it. TLVs after the addresses are ignored.
pub fn parseBody(f: Fixed, rest: []const u8) BodyError!Source {
    std.debug.assert(rest.len == f.rest);
    if (f.command == .local) return .local;
    switch (f.family) {
        // INET, STREAM: src addr, dst addr, src port, dst port.
        0x11 => {
            if (rest.len < 12) return error.ShortAddress;
            return .{ .peer = .{ .ip4 = rest[0..4].* } };
        },
        // INET6, STREAM.
        0x21 => {
            if (rest.len < 36) return error.ShortAddress;
            const a = rest[0..16].*;
            if (std.mem.eql(u8, a[0..12], &mapped_v4_prefix)) return .{ .peer = .{ .ip4 = a[12..16].* } };
            return .{ .peer = .{ .ip6 = a } };
        },
        // UNSPEC: the receiver ignores the address block, whatever its
        // length (spec section 2.2).
        0x00 => return .{ .peer = .unknown },
        // UNIX STREAM: src path, dst path, 108 bytes each. No client address,
        // but a block shorter than that is malformed like any other.
        0x31 => {
            if (rest.len < 216) return error.ShortAddress;
            return .{ .peer = .unknown };
        },
        else => return error.BadFamily,
    }
}

const mapped_v4_prefix = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff };

/// One trusted source range: an address and a prefix length.
///
/// Peers are normalised before they are checked: an IPv4 client on a
/// dual-stack socket is `.ip4`, not `::ffff:a.b.c.d`. So a trusted range is
/// normalised the same way, address and prefix together (`checked`): a
/// v4-mapped range of /96 or narrower is the IPv4 range
/// (`::ffff:192.0.2.0/120` is `192.0.2.0/24`). An IPv6 range wider than /96
/// that holds `::ffff:0:0/96` without being a mapped range (`::/0`, for one)
/// would hold IPv4 sources in their socket form while naming none of them;
/// it is not widened to IPv4 silently, and not narrowed silently either:
/// `Server.init` refuses it as a trusted range
/// (`ProxyTrustedRangeHoldsIpv4`), and the list must name the IPv4 range or
/// the mapped range instead.
pub const Cidr = struct {
    addr: request.Peer,
    prefix_bits: u8,

    pub const CheckError = error{
        /// A prefix longer than the address, or no address.
        InvalidCidr,
        /// An IPv6 range wider than /96 that holds `::ffff:0:0/96`.
        HoldsMappedBlock,
    };

    /// `"10.0.0.0/8"`, `"2001:db8::/32"`, or a bare address (a host),
    /// normalised (`checked`). A range that holds the mapped block is valid
    /// text and comes back as written, so the server can name it when it
    /// refuses it.
    pub fn parse(text: []const u8) error{InvalidCidr}!Cidr {
        const slash = std.mem.indexOfScalar(u8, text, '/');
        const host = if (slash) |s| text[0..s] else text;
        const given: ?u8 = if (slash) |s| std.fmt.parseInt(u8, text[s + 1 ..], 10) catch return error.InvalidCidr else null;
        const ip = std.Io.net.IpAddress.parse(host, 0) catch return error.InvalidCidr;
        const raw: Cidr = switch (ip) {
            .ip4 => |a| .{ .addr = .{ .ip4 = a.bytes }, .prefix_bits = given orelse 32 },
            .ip6 => |a| .{ .addr = .{ .ip6 = a.bytes }, .prefix_bits = given orelse 128 },
        };
        return raw.checked() catch |err| switch (err) {
            error.InvalidCidr => error.InvalidCidr,
            error.HoldsMappedBlock => raw,
        };
    }

    /// The range as the accept loop compares it, or why it cannot be a
    /// trusted range. `parse` and `Server.init` both go through it, so a
    /// range built as a literal is held to the same rules as parsed text.
    pub fn checked(self: Cidr) CheckError!Cidr {
        switch (self.addr) {
            .ip4 => if (self.prefix_bits > 32) return error.InvalidCidr,
            .ip6 => |a| {
                if (self.prefix_bits > 128) return error.InvalidCidr;
                const mapped = std.mem.eql(u8, a[0..12], &mapped_v4_prefix);
                if (mapped and self.prefix_bits >= 96) {
                    return .{ .addr = .{ .ip4 = a[12..16].* }, .prefix_bits = self.prefix_bits - 96 };
                }
                if (self.prefix_bits < 96 and prefixEql(&a, &mapped_block, self.prefix_bits)) return error.HoldsMappedBlock;
            },
            .unknown => return error.InvalidCidr,
        }
        return self;
    }

    pub fn contains(self: Cidr, peer: request.Peer) bool {
        return switch (self.addr) {
            .ip4 => |a| switch (peer) {
                .ip4 => |b| prefixEql(&a, &b, self.prefix_bits),
                else => false,
            },
            .ip6 => |a| switch (peer) {
                .ip6 => |b| prefixEql(&a, &b, self.prefix_bits),
                else => false,
            },
            .unknown => false,
        };
    }

    /// `a.b.c.d/n` or `x::/n`, as an operator writes it.
    pub fn format(self: Cidr, w: *std.Io.Writer) std.Io.Writer.Error!void {
        switch (self.addr) {
            .ip4 => |a| try w.print("{d}.{d}.{d}.{d}", .{ a[0], a[1], a[2], a[3] }),
            .ip6 => |a| {
                const u: std.Io.net.Ip6Address.Unresolved = .{ .bytes = a, .interface_name = null };
                try u.format(w);
            },
            .unknown => try w.writeAll("(unknown)"),
        }
        try w.print("/{d}", .{self.prefix_bits});
    }
};

/// `::ffff:0:0/96`, the block every IPv4-mapped address is in.
const mapped_block = mapped_v4_prefix ++ [_]u8{ 0, 0, 0, 0 };

fn prefixEql(a: []const u8, b: []const u8, bits: u8) bool {
    const whole = bits / 8;
    if (!std.mem.eql(u8, a[0..whole], b[0..whole])) return false;
    const rest: u3 = @intCast(bits % 8);
    if (rest == 0) return true;
    const mask = ~(@as(u8, 0xff) >> rest);
    return a[whole] & mask == b[whole] & mask;
}

/// True when `peer` is inside one of `list`. An unknown peer is never
/// trusted.
pub fn trusted(list: []const Cidr, peer: request.Peer) bool {
    for (list) |c| {
        if (c.contains(peer)) return true;
    }
    return false;
}

/// A complete header for tests and examples: `PROXY` over INET or INET6 with
/// `src` as the client (and documentation addresses for the rest), or
/// `LOCAL`. Returns the bytes written.
pub fn encode(out: []u8, src: Source) usize {
    @memcpy(out[0..12], &signature);
    switch (src) {
        .local => {
            out[12] = 0x20;
            out[13] = 0x00;
            std.mem.writeInt(u16, out[14..16], 0, .big);
            return fixed_bytes;
        },
        .peer => |p| switch (p) {
            .ip4 => |a| {
                out[12] = 0x21;
                out[13] = 0x11;
                std.mem.writeInt(u16, out[14..16], 12, .big);
                @memcpy(out[16..20], &a);
                @memcpy(out[20..24], &[_]u8{ 192, 0, 2, 1 });
                std.mem.writeInt(u16, out[24..26], 40000, .big);
                std.mem.writeInt(u16, out[26..28], 443, .big);
                return 28;
            },
            .ip6 => |a| {
                out[12] = 0x21;
                out[13] = 0x21;
                std.mem.writeInt(u16, out[14..16], 36, .big);
                @memcpy(out[16..32], &a);
                @memset(out[32..48], 0);
                out[32] = 0x20;
                out[33] = 0x01;
                out[34] = 0x0d;
                out[35] = 0xb8;
                out[47] = 1;
                std.mem.writeInt(u16, out[48..50], 40000, .big);
                std.mem.writeInt(u16, out[50..52], 443, .big);
                return 52;
            },
            .unknown => {
                out[12] = 0x21;
                out[13] = 0x00;
                std.mem.writeInt(u16, out[14..16], 0, .big);
                return fixed_bytes;
            },
        },
    }
}

fn parseWhole(bytes: []const u8, cap: usize) !Source {
    const f = try parseFixed(bytes[0..fixed_bytes], cap);
    return parseBody(f, bytes[fixed_bytes..][0..f.rest]);
}

test "PROXY v2: v4, v6, v4-mapped v6, LOCAL, UNSPEC round-trip through encode" {
    var buf: [64]u8 = undefined;
    var n = encode(&buf, .{ .peer = .{ .ip4 = .{ 203, 0, 113, 7 } } });
    try std.testing.expectEqual(request.Peer{ .ip4 = .{ 203, 0, 113, 7 } }, (try parseWhole(buf[0..n], 536)).peer);
    const a6: [16]u8 = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 9 };
    n = encode(&buf, .{ .peer = .{ .ip6 = a6 } });
    try std.testing.expectEqual(request.Peer{ .ip6 = a6 }, (try parseWhole(buf[0..n], 536)).peer);
    // A v4-mapped source over INET6 is the IPv4 client.
    var mapped: [16]u8 = @splat(0);
    mapped[10] = 0xff;
    mapped[11] = 0xff;
    mapped[12] = 198;
    mapped[13] = 51;
    mapped[14] = 100;
    mapped[15] = 4;
    n = encode(&buf, .{ .peer = .{ .ip6 = mapped } });
    try std.testing.expectEqual(request.Peer{ .ip4 = .{ 198, 51, 100, 4 } }, (try parseWhole(buf[0..n], 536)).peer);
    n = encode(&buf, .local);
    try std.testing.expectEqual(Source.local, try parseWhole(buf[0..n], 536));
    n = encode(&buf, .{ .peer = .unknown });
    try std.testing.expectEqual(request.Peer.unknown, (try parseWhole(buf[0..n], 536)).peer);
}

test "PROXY v2: malformed and over-cap headers are refused" {
    var buf: [64]u8 = undefined;
    _ = encode(&buf, .{ .peer = .{ .ip4 = .{ 1, 2, 3, 4 } } });
    var bad = buf;
    bad[0] = 'P'; // PROXY v1's "PROXY " text is not accepted
    try std.testing.expectError(error.BadSignature, parseFixed(bad[0..16], 536));
    bad = buf;
    bad[12] = 0x11; // version 1
    try std.testing.expectError(error.BadVersion, parseFixed(bad[0..16], 536));
    bad = buf;
    bad[12] = 0x22; // command 2
    try std.testing.expectError(error.BadCommand, parseFixed(bad[0..16], 536));
    bad = buf;
    std.mem.writeInt(u16, bad[14..16], 65535, .big);
    try std.testing.expectError(error.TooLong, parseFixed(bad[0..16], 536));
    // DGRAM.
    bad = buf;
    bad[13] = 0x12;
    try std.testing.expectError(error.BadFamily, parseWhole(bad[0..28], 536));
    // INET with an address block too short.
    bad = buf;
    std.mem.writeInt(u16, bad[14..16], 4, .big);
    try std.testing.expectError(error.ShortAddress, parseWhole(bad[0..20], 536));
}

test "Cidr: v4 and v6 ranges, hosts, and no cross-family match" {
    const net = try Cidr.parse("10.1.0.0/16");
    try std.testing.expect(net.contains(.{ .ip4 = .{ 10, 1, 200, 3 } }));
    try std.testing.expect(!net.contains(.{ .ip4 = .{ 10, 2, 0, 1 } }));
    const host = try Cidr.parse("127.0.0.1");
    try std.testing.expect(host.contains(.{ .ip4 = .{ 127, 0, 0, 1 } }));
    try std.testing.expect(!host.contains(.{ .ip4 = .{ 127, 0, 0, 2 } }));
    const odd = try Cidr.parse("192.168.0.0/23");
    try std.testing.expect(odd.contains(.{ .ip4 = .{ 192, 168, 1, 255 } }));
    try std.testing.expect(!odd.contains(.{ .ip4 = .{ 192, 168, 2, 0 } }));
    const v6 = try Cidr.parse("2001:db8::/32");
    var a: [16]u8 = @splat(0);
    a[0] = 0x20;
    a[1] = 0x01;
    a[2] = 0x0d;
    a[3] = 0xb8;
    a[15] = 5;
    try std.testing.expect(v6.contains(.{ .ip6 = a }));
    try std.testing.expect(!v6.contains(.{ .ip4 = .{ 32, 1, 13, 184 } }));
    try std.testing.expect(!v6.contains(.unknown));
    try std.testing.expectError(error.InvalidCidr, Cidr.parse("10.0.0.0/33"));
    try std.testing.expectError(error.InvalidCidr, Cidr.parse("nonsense"));
}

test "PROXY v2: UNIX STREAM needs its whole 216-byte address block" {
    var buf: [fixed_bytes + 216]u8 = @splat(0);
    _ = encode(&buf, .{ .peer = .unknown });
    buf[13] = 0x31;
    // None, one short, exact.
    for ([_]u16{ 0, 215, 216 }) |len| {
        std.mem.writeInt(u16, buf[14..16], len, .big);
        const got = parseWhole(buf[0 .. fixed_bytes + len], 536);
        if (len < 216) {
            try std.testing.expectError(error.ShortAddress, got);
        } else {
            try std.testing.expectEqual(request.Peer.unknown, (try got).peer);
        }
    }
}

test "Cidr: a v4-mapped IPv6 range is the IPv4 range, prefix and address together (/120)" {
    // ::ffff:192.0.2.0/120 is 192.0.2.0/24.
    const net = try Cidr.parse("::ffff:192.0.2.0/120");
    try std.testing.expectEqual(request.Peer{ .ip4 = .{ 192, 0, 2, 0 } }, net.addr);
    try std.testing.expectEqual(@as(u8, 24), net.prefix_bits);
    try std.testing.expect(net.contains(.{ .ip4 = .{ 192, 0, 2, 77 } }));
    try std.testing.expect(!net.contains(.{ .ip4 = .{ 192, 0, 3, 1 } }));
}

test "Cidr: a v4-mapped /128, and a bare v4-mapped address, are one IPv4 host" {
    const host = try Cidr.parse("::ffff:192.0.2.5/128");
    try std.testing.expectEqual(@as(u8, 32), host.prefix_bits);
    try std.testing.expect(host.contains(.{ .ip4 = .{ 192, 0, 2, 5 } }));
    try std.testing.expect(!host.contains(.{ .ip4 = .{ 192, 0, 2, 6 } }));
    const bare = try Cidr.parse("::ffff:192.0.2.5");
    try std.testing.expectEqual(@as(u8, 32), bare.prefix_bits);
    try std.testing.expect(bare.contains(.{ .ip4 = .{ 192, 0, 2, 5 } }));
}

test "Cidr: an IPv6 range that holds ::ffff:0:0/96 trusts no IPv4 peer" {
    // Peers are normalised to IPv4, so such a range would widen who may
    // claim an identity without saying so; `Server.init` refuses it.
    const v6all = try Cidr.parse("::/0");
    try std.testing.expect(!v6all.contains(.{ .ip4 = .{ 10, 0, 0, 1 } }));
    const wide = try Cidr.parse("::ffff:0.0.0.0/95");
    try std.testing.expect(!wide.contains(.{ .ip4 = .{ 10, 0, 0, 1 } }));
    // Every IPv6 address is still inside ::/0.
    var a: [16]u8 = @splat(0);
    a[0] = 0x20;
    try std.testing.expect(v6all.contains(.{ .ip6 = a }));
}

test "Cidr.checked: the mapped block refused, a mapped literal normalised, a bad prefix refused" {
    try std.testing.expectError(error.HoldsMappedBlock, (try Cidr.parse("::/0")).checked());
    try std.testing.expectError(error.HoldsMappedBlock, (try Cidr.parse("::/80")).checked());
    try std.testing.expectError(error.HoldsMappedBlock, (try Cidr.parse("::ffff:0.0.0.0/95")).checked());
    // Not holding it: an IPv6 range elsewhere, and ::/96 (the compat block, not the mapped one).
    _ = try (try Cidr.parse("2001:db8::/32")).checked();
    _ = try (try Cidr.parse("::/96")).checked();
    // ::ffff:0:0/96 itself is every IPv4 address.
    const all4 = try (try Cidr.parse("::ffff:0.0.0.0/96")).checked();
    try std.testing.expectEqual(@as(u8, 0), all4.prefix_bits);
    try std.testing.expect(all4.contains(.{ .ip4 = .{ 8, 8, 8, 8 } }));
    // A literal mapped range is normalised as parsed text is.
    var m: [16]u8 = @splat(0);
    m[10] = 0xff;
    m[11] = 0xff;
    m[12] = 192;
    m[14] = 2;
    const lit = try (Cidr{ .addr = .{ .ip6 = m }, .prefix_bits = 120 }).checked();
    try std.testing.expectEqual(request.Peer{ .ip4 = .{ 192, 0, 2, 0 } }, lit.addr);
    try std.testing.expectEqual(@as(u8, 24), lit.prefix_bits);
    try std.testing.expectError(error.InvalidCidr, (Cidr{ .addr = .{ .ip4 = .{ 10, 0, 0, 0 } }, .prefix_bits = 33 }).checked());
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("::/0", try std.fmt.bufPrint(&buf, "{f}", .{try Cidr.parse("::/0")}));
    try std.testing.expectEqualStrings("10.0.0.0/8", try std.fmt.bufPrint(&buf, "{f}", .{try Cidr.parse("10.0.0.0/8")}));
}
