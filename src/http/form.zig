//! `application/x-www-form-urlencoded` decoding for query strings.
//!
//! This is NOT `std.Uri` percent-decoding, and the difference is load-bearing.
//! `std.Uri` implements RFC 3986, where `+` is a literal plus. Form encoding —
//! what a browser's `URLSearchParams` and every HTML form produce — encodes a
//! space as `+` and a literal plus as `%2B`. Decoding a form-encoded value with
//! an RFC 3986 decoder turns `John Smith` into `John+Smith`, silently, on the
//! most ordinary input there is. The standard library has no form decoder, so
//! this one exists.
//!
//! Decoding is STRICT here: a malformed escape is an error, not a literal.
//! `std.Uri` is lenient because it decodes arbitrary real-world URLs, where a
//! bare `%` is common and rejecting it would refuse URLs browsers accept. A
//! query parameter is a different contract — it was produced by an encoder, so
//! anything malformed is a bug or an attack, and passing it through would give
//! the caller bytes that no encoder ever emitted.
const std = @import("std");

pub const DecodeError = error{
    /// A `%` not followed by exactly two hex digits.
    MalformedPercent,
    OutOfMemory,
};

pub const ParamError = error{
    /// The parameter does not appear in the query.
    Missing,
    /// The parameter appears more than once.
    Duplicate,
};

/// Decode one form-encoded component.
///
/// Two passes, as `httpz` does. The first validates every escape and computes
/// the exact decoded length; the second writes. That ordering buys three
/// things: a malformed escape is refused before anything is written, the
/// allocation is exactly the right size so there is no resize, and the write
/// loop needs no bounds checks because the first pass proved them.
///
/// The result MAY ALIAS `encoded`: when there is no escape and no `+`, nothing
/// is allocated and the input is returned as-is. So `arena` is not merely a
/// naming preference — the result is valid for as long as BOTH the arena and
/// `encoded` live, and it must never be freed individually.
///
/// That is why there is no ownership flag and no `deinit`. A value that
/// sometimes owns its memory and sometimes does not pushes a runtime question
/// onto every caller, and in this stack the answer never matters: query bytes
/// and decoded bytes both live in the request arena and die with the request.
/// A caller that needs unambiguous ownership uses `decodeComponentInto` with
/// its own buffer.
///
/// Both passes test the two characters SEPARATELY, with `std.ascii.isHex` here
/// and `std.fmt.charToDigit` in the writer. Never replace either with one
/// `parseInt` over the pair: `parseInt` accepts a leading sign, so `%+A` would
/// decode to the same byte as `%0A`, and two spellings of one byte let a filter
/// reading the raw query and a consumer reading the decoded bytes disagree
/// about content. `std.Uri` and the Datastar SDK both have that bug; `httpz`
/// and `dusty` avoid it the same way this does.
pub fn decodeComponent(arena: std.mem.Allocator, encoded: []const u8) DecodeError![]const u8 {
    var decoded_len = encoded.len;
    var has_plus = false;
    var i: usize = 0;
    while (i < encoded.len) {
        switch (encoded[i]) {
            '%' => {
                if (i + 2 >= encoded.len or
                    !std.ascii.isHex(encoded[i + 1]) or
                    !std.ascii.isHex(encoded[i + 2])) return error.MalformedPercent;
                decoded_len -= 2;
                i += 3;
            },
            '+' => {
                has_plus = true;
                i += 1;
            },
            else => i += 1,
        }
    }
    if (decoded_len == encoded.len and !has_plus) return encoded;

    const out = try arena.alloc(u8, decoded_len);
    errdefer arena.free(out);
    const written = try decodeComponentInto(out, encoded);
    std.debug.assert(written.len == decoded_len);
    return written;
}

/// Decode into a caller-provided buffer, as `std.Uri.percentDecodeBackwards`
/// does. `output` must have room for the decoded result; `encoded.len` is
/// always sufficient, because decoding only shrinks. Returns the subslice
/// actually written.
pub fn decodeComponentInto(output: []u8, encoded: []const u8) error{MalformedPercent}![]u8 {
    var read: usize = 0;
    var write: usize = 0;
    while (read < encoded.len) {
        switch (encoded[read]) {
            '%' => {
                if (read + 2 >= encoded.len) return error.MalformedPercent;
                const hi = std.fmt.charToDigit(encoded[read + 1], 16) catch return error.MalformedPercent;
                const lo = std.fmt.charToDigit(encoded[read + 2], 16) catch return error.MalformedPercent;
                output[write] = hi * 16 + lo;
                read += 3;
            },
            '+' => {
                output[write] = ' ';
                read += 1;
            },
            else => {
                output[write] = encoded[read];
                read += 1;
            },
        }
        write += 1;
    }
    return output[0..write];
}

/// Find one parameter's still-encoded value.
///
/// A repeated parameter is an error rather than first-or-last-wins. Duplicate
/// parameters are the parameter-pollution primitive: two components that pick
/// different occurrences disagree about what the request said. Refusing is the
/// only answer that cannot be split.
pub fn findParam(query: []const u8, name: []const u8) ParamError![]const u8 {
    var found: ?[]const u8 = null;
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
        if (!std.mem.eql(u8, pair[0..eq], name)) continue;
        if (found != null) return error.Duplicate;
        found = pair[eq + 1 ..];
    }
    return found orelse error.Missing;
}

/// Find a parameter and decode it.
pub fn param(
    arena: std.mem.Allocator,
    query: []const u8,
    name: []const u8,
) (DecodeError || ParamError)![]const u8 {
    return decodeComponent(arena, try findParam(query, name));
}

test "decodes a form-encoded component" {
    // An arena, because that is the contract: the result may alias the input,
    // and nothing is ever freed individually.
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    {
        try std.testing.expectEqualStrings("a b", try decodeComponent(arena, "a%20b"));
    }
    {
        // `+` is a space, and a literal plus arrives as %2B. This is the rule
        // `std.Uri` does not implement.
        try std.testing.expectEqualStrings("John Smith+Co", try decodeComponent(arena, "John+Smith%2BCo"));
    }
    {
        // Nothing to decode: the input comes straight back, no allocation.
        var counting = std.testing.FailingAllocator.init(arena, .{});
        const input = "plain";
        const got = try decodeComponent(counting.allocator(), input);
        try std.testing.expectEqualStrings(input, got);
        try std.testing.expectEqual(input.ptr, got.ptr);
        try std.testing.expectEqual(@as(usize, 0), counting.allocations);
    }
}

test "a signed escape is refused, because it aliases a valid one" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    // `std.fmt.parseInt(u8, "+A", 16) == 0x0A`, so one parse over both
    // characters would make `%+A` a second spelling of `%0A`, and `%+0` a way
    // to inject a NUL. Collapsing the two `charToDigit` calls into one
    // `parseInt` is the mutation this test exists to catch.
    const gpa = arena_state.allocator();
    try std.testing.expectError(error.MalformedPercent, decodeComponent(gpa, "%+A"));
    try std.testing.expectError(error.MalformedPercent, decodeComponent(gpa, "%+0"));
    try std.testing.expectError(error.MalformedPercent, decodeComponent(gpa, "%-A"));
    // The properly encoded plus still decodes.
    try std.testing.expectEqualStrings("+A", try decodeComponent(gpa, "%2BA"));
}

test "a truncated escape is refused" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const gpa = arena_state.allocator();
    try std.testing.expectError(error.MalformedPercent, decodeComponent(gpa, "%4"));
    try std.testing.expectError(error.MalformedPercent, decodeComponent(gpa, "ab%"));
}

test "decoding does not allocate per escape" {
    // The count must not scale with the input: pass one computes the exact
    // length, so pass two writes into a single buffer. A per-byte append loop
    // would make these two differ.
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var small = std.testing.FailingAllocator.init(arena, .{});
    const a = try decodeComponent(small.allocator(), "%7B%22a%22%3A%22x+y%22%7D");
    try std.testing.expectEqualStrings("{\"a\":\"x y\"}", a);

    var large = std.testing.FailingAllocator.init(arena, .{});
    const b = try decodeComponent(large.allocator(), "%41" ** 512);
    try std.testing.expectEqual(@as(usize, 512), b.len);

    try std.testing.expectEqual(small.allocations, large.allocations);
}

test "decodeComponentInto allocates nothing at all" {
    var buf: [64]u8 = undefined;
    const got = try decodeComponentInto(&buf, "hello+%77orld");
    try std.testing.expectEqualStrings("hello world", got);
}

test "finds a parameter and refuses duplicates" {
    try std.testing.expectEqualStrings("1", try findParam("a=1&b=2", "a"));
    try std.testing.expectEqualStrings("2", try findParam("a=1&b=2", "b"));
    try std.testing.expectError(error.Missing, findParam("a=1", "c"));
    try std.testing.expectError(error.Duplicate, findParam("a=1&a=2", "a"));
    // A valueless key is not a match for a named parameter.
    try std.testing.expectError(error.Missing, findParam("a&b=2", "a"));
}

test "param finds and decodes" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    try std.testing.expectEqualStrings("hello world", try param(arena_state.allocator(), "x=1&msg=hello+world", "msg"));
}
