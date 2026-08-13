//! Response content-coding negotiation and compressible-type rules.
//!
//! Wire rules live beside the decision code (RFC 9110 §12.5.3 Accept-Encoding).
//! Compression itself is out of this file: this is pure header logic so the
//! case table can be unit-tested without an encoder.
const std = @import("std");

/// Whether the client accepts brotli (`br`) for a response body.
///
/// RFC 9110 §12.5.3:
/// - Codings are tokens; matching is case-insensitive.
/// - `q=0` (and `q=0.000`) refuses that coding; missing q means 1.0.
/// - `*` matches every coding not named explicitly. If `br` is named, `*` does
///   not speak for it.
/// - Multiple Accept-Encoding field lines combine as a comma-joined list; the
///   caller joins them before calling this.
/// - No header (null / empty after trim of the whole value) means identity only.
/// - A malformed member is ignored; it does not poison the rest of the list.
pub fn acceptsBrotli(header_value: ?[]const u8) bool {
    const raw = header_value orelse return false;
    // Empty field value is not "missing": it is an empty list → identity only.
    if (raw.len == 0) return false;

    var saw_br = false;
    var br_q: f32 = 0;
    var saw_star = false;
    var star_q: f32 = 0;
    var any_valid = false;

    var members = std.mem.splitScalar(u8, raw, ',');
    while (members.next()) |member_raw| {
        const member = trimAscii(member_raw);
        if (member.len == 0) continue;

        const parsed = parseMember(member) orelse continue;
        any_valid = true;

        if (eqlIgnoreCase(parsed.coding, "br")) {
            saw_br = true;
            br_q = parsed.q;
        } else if (eqlIgnoreCase(parsed.coding, "*")) {
            saw_star = true;
            star_q = parsed.q;
        }
    }

    if (!any_valid) return false;
    if (saw_br) return br_q > 0;
    if (saw_star) return star_q > 0;
    return false;
}

const Member = struct {
    coding: []const u8,
    q: f32,
};

fn parseMember(member: []const u8) ?Member {
    // coding [ ";" parameter ]*
    // parameter = token "=" ( token / quoted-string )
    // Only `q` is meaningful here; other parameters are ignored.
    var parts = std.mem.splitScalar(u8, member, ';');
    const coding_raw = parts.next() orelse return null;
    const coding = trimAscii(coding_raw);
    if (coding.len == 0) return null;
    if (!isToken(coding) and !(coding.len == 1 and coding[0] == '*')) return null;

    var q: f32 = 1.0;
    while (parts.next()) |param_raw| {
        const param = trimAscii(param_raw);
        if (param.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, param, '=') orelse continue;
        const name = trimAscii(param[0..eq]);
        var value = trimAscii(param[eq + 1 ..]);
        if (value.len >= 2 and value[0] == '"' and value[value.len - 1] == '"') {
            value = value[1 .. value.len - 1];
        }
        if (!eqlIgnoreCase(name, "q")) continue;
        q = parseQ(value) orelse return null;
    }
    return .{ .coding = coding, .q = q };
}

/// qvalue = ( "0" [ "." 0*3DIGIT ] ) / ( "1" [ "." 0*3("0") ] )
fn parseQ(s: []const u8) ?f32 {
    if (s.len == 0 or s.len > 5) return null;
    if (s[0] != '0' and s[0] != '1') return null;
    if (s.len == 1) return if (s[0] == '0') 0.0 else 1.0;
    if (s[1] != '.') return null;
    if (s[0] == '1') {
        for (s[2..]) |c| if (c != '0') return null;
        return 1.0;
    }
    // 0.xxx
    if (s.len > 5) return null;
    var frac: u32 = 0;
    var scale: f32 = 1.0;
    for (s[2..]) |c| {
        if (c < '0' or c > '9') return null;
        frac = frac * 10 + (c - '0');
        scale *= 10.0;
    }
    return @as(f32, @floatFromInt(frac)) / scale;
}

fn isToken(s: []const u8) bool {
    if (s.len == 0) return false;
    for (s) |c| {
        // RFC 9110 tchar
        switch (c) {
            '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~' => {},
            '0'...'9', 'A'...'Z', 'a'...'z' => {},
            else => return false,
        }
    }
    return true;
}

fn trimAscii(s: []const u8) []const u8 {
    var start: usize = 0;
    while (start < s.len and (s[start] == ' ' or s[start] == '\t')) : (start += 1) {}
    var end = s.len;
    while (end > start and (s[end - 1] == ' ' or s[end - 1] == '\t')) : (end -= 1) {}
    return s[start..end];
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

/// Content types eligible for response compression.
///
/// text/*, application/json, application/javascript, application/xml,
/// image/svg+xml, text/event-stream, and any type ending in +json / +xml.
/// Comparison is case-insensitive on type/subtype; parameters after `;` are
/// stripped before matching.
pub fn isCompressibleContentType(content_type: []const u8) bool {
    const bare = bareMediaType(content_type);
    if (bare.len == 0) return false;

    if (startsWithIgnoreCase(bare, "text/")) return true;
    if (eqlIgnoreCase(bare, "application/json")) return true;
    if (eqlIgnoreCase(bare, "application/javascript")) return true;
    if (eqlIgnoreCase(bare, "application/xml")) return true;
    if (eqlIgnoreCase(bare, "image/svg+xml")) return true;
    if (eqlIgnoreCase(bare, "text/event-stream")) return true;

    if (endsWithIgnoreCase(bare, "+json")) return true;
    if (endsWithIgnoreCase(bare, "+xml")) return true;
    return false;
}

fn bareMediaType(ct: []const u8) []const u8 {
    const semi = std.mem.indexOfScalar(u8, ct, ';') orelse ct.len;
    return trimAscii(ct[0..semi]);
}

fn startsWithIgnoreCase(hay: []const u8, prefix: []const u8) bool {
    if (hay.len < prefix.len) return false;
    return eqlIgnoreCase(hay[0..prefix.len], prefix);
}

fn endsWithIgnoreCase(hay: []const u8, suffix: []const u8) bool {
    if (hay.len < suffix.len) return false;
    return eqlIgnoreCase(hay[hay.len - suffix.len ..], suffix);
}

/// Join multiple Accept-Encoding field values with ", " (RFC 9110 combined form).
pub fn joinAcceptEncoding(allocator: std.mem.Allocator, values: []const []const u8) std.mem.Allocator.Error![]u8 {
    if (values.len == 0) return try allocator.dupe(u8, "");
    var total: usize = 0;
    for (values, 0..) |v, i| {
        total += v.len;
        if (i + 1 < values.len) total += 2;
    }
    var out = try allocator.alloc(u8, total);
    var off: usize = 0;
    for (values, 0..) |v, i| {
        @memcpy(out[off..][0..v.len], v);
        off += v.len;
        if (i + 1 < values.len) {
            out[off] = ',';
            out[off + 1] = ' ';
            off += 2;
        }
    }
    return out;
}

/// Merge `accept-encoding` into an existing Vary value without duplicating it.
pub fn mergeVaryAcceptEncoding(allocator: std.mem.Allocator, existing: ?[]const u8) std.mem.Allocator.Error![]u8 {
    const token = "accept-encoding";
    if (existing) |v| {
        var it = std.mem.splitScalar(u8, v, ',');
        while (it.next()) |part| {
            if (eqlIgnoreCase(trimAscii(part), token)) {
                return try allocator.dupe(u8, v);
            }
        }
        const trimmed = trimAscii(v);
        if (trimmed.len == 0) return try allocator.dupe(u8, token);
        return try std.fmt.allocPrint(allocator, "{s}, {s}", .{ trimmed, token });
    }
    return try allocator.dupe(u8, token);
}

pub fn headerHasContentEncoding(comptime H: type, headers: []const H) bool {
    for (headers) |h| {
        if (eqlIgnoreCase(h.name, "content-encoding")) return true;
    }
    return false;
}

pub fn findHeaderValue(comptime H: type, headers: []const H, name: []const u8) ?[]const u8 {
    for (headers) |h| {
        if (eqlIgnoreCase(h.name, name)) return h.value;
    }
    return null;
}

// --- tests -----------------------------------------------------------------

test "acceptsBrotli: missing and empty" {
    try std.testing.expect(!acceptsBrotli(null));
    try std.testing.expect(!acceptsBrotli(""));
    try std.testing.expect(!acceptsBrotli("   "));
}

test "acceptsBrotli: plain br and case" {
    try std.testing.expect(acceptsBrotli("br"));
    try std.testing.expect(acceptsBrotli("BR"));
    try std.testing.expect(acceptsBrotli("Br"));
    try std.testing.expect(acceptsBrotli(" gzip, br "));
    try std.testing.expect(acceptsBrotli("br, gzip"));
}

test "acceptsBrotli: q-values" {
    try std.testing.expect(acceptsBrotli("br;q=1"));
    try std.testing.expect(acceptsBrotli("br;q=1.0"));
    try std.testing.expect(acceptsBrotli("br;q=0.001"));
    try std.testing.expect(!acceptsBrotli("br;q=0"));
    try std.testing.expect(!acceptsBrotli("br;q=0.0"));
    try std.testing.expect(!acceptsBrotli("br;q=0.000"));
    try std.testing.expect(!acceptsBrotli("br; q=0"));
}

test "acceptsBrotli: star" {
    try std.testing.expect(acceptsBrotli("*"));
    try std.testing.expect(acceptsBrotli("*;q=0.5"));
    try std.testing.expect(!acceptsBrotli("*;q=0"));
    // Explicit br outranks *: br;q=0 with * still refuses br.
    try std.testing.expect(!acceptsBrotli("br;q=0, *"));
    try std.testing.expect(!acceptsBrotli("*, br;q=0"));
    try std.testing.expect(acceptsBrotli("gzip;q=0, *"));
}

test "acceptsBrotli: identity alone and unknown codings" {
    try std.testing.expect(!acceptsBrotli("identity"));
    try std.testing.expect(!acceptsBrotli("identity;q=0"));
    try std.testing.expect(acceptsBrotli("gzip, deflate, br"));
    try std.testing.expect(acceptsBrotli("unknown, br, other"));
    try std.testing.expect(!acceptsBrotli("gzip, deflate"));
}

test "acceptsBrotli: malformed member ignored" {
    try std.testing.expect(acceptsBrotli("@@@, br"));
    try std.testing.expect(acceptsBrotli("br, @@@"));
    try std.testing.expect(!acceptsBrotli("br;q=notanumber, gzip")); // malformed br ignored; gzip alone → no br
    try std.testing.expect(!acceptsBrotli("br;q=notanumber"));
    try std.testing.expect(acceptsBrotli("nope;, br;q=1"));
}

test "acceptsBrotli: whitespace and long list" {
    try std.testing.expect(acceptsBrotli("  br  ;  q=0.8  "));
    var list: std.ArrayList(u8) = .empty;
    defer list.deinit(std.testing.allocator);
    var i: usize = 0;
    while (i < 49) : (i += 1) {
        var tmp: [16]u8 = undefined;
        const piece = try std.fmt.bufPrint(&tmp, "c{d}, ", .{i});
        try list.appendSlice(std.testing.allocator, piece);
    }
    try list.appendSlice(std.testing.allocator, "br");
    try std.testing.expect(acceptsBrotli(list.items));
}

test "acceptsBrotli: no substring match" {
    try std.testing.expect(!acceptsBrotli("brotli"));
    try std.testing.expect(!acceptsBrotli("x-br"));
    try std.testing.expect(!acceptsBrotli("brx"));
    try std.testing.expect(!acceptsBrotli("abr"));
}

test "isCompressibleContentType table" {
    try std.testing.expect(isCompressibleContentType("text/plain"));
    try std.testing.expect(isCompressibleContentType("TEXT/HTML; charset=utf-8"));
    try std.testing.expect(isCompressibleContentType("text/event-stream"));
    try std.testing.expect(isCompressibleContentType("application/json"));
    try std.testing.expect(isCompressibleContentType("application/javascript"));
    try std.testing.expect(isCompressibleContentType("application/xml"));
    try std.testing.expect(isCompressibleContentType("image/svg+xml"));
    try std.testing.expect(isCompressibleContentType("application/ld+json"));
    try std.testing.expect(isCompressibleContentType("application/atom+xml"));
    try std.testing.expect(!isCompressibleContentType("image/png"));
    try std.testing.expect(!isCompressibleContentType("application/octet-stream"));
    try std.testing.expect(!isCompressibleContentType(""));
}

test "mergeVaryAcceptEncoding" {
    const a = try mergeVaryAcceptEncoding(std.testing.allocator, null);
    defer std.testing.allocator.free(a);
    try std.testing.expectEqualStrings("accept-encoding", a);

    const b = try mergeVaryAcceptEncoding(std.testing.allocator, "Accept-Encoding");
    defer std.testing.allocator.free(b);
    try std.testing.expectEqualStrings("Accept-Encoding", b);

    const c = try mergeVaryAcceptEncoding(std.testing.allocator, "origin");
    defer std.testing.allocator.free(c);
    try std.testing.expectEqualStrings("origin, accept-encoding", c);
}
