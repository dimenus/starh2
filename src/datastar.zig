//! Datastar signal reads.
//!
//! Datastar sends its client-side state ("signals") to the server as JSON: in
//! the `datastar` query parameter for a GET, in the body otherwise. This module
//! is that convention and nothing else — the generic parts (form decoding,
//! parameter lookup) live in `http/form.zig`, because they are ordinary HTTP
//! and every consumer wants them whether or not it speaks Datastar.
//!
//! It deliberately does NOT re-export the Datastar SDK. The SDK's emitters take
//! an allocator and return bytes, so they have no transport coupling and a
//! consumer calls them directly. Re-exporting them would only oblige every user
//! of an HTTP/2 server to compile a hypermedia SDK.
//!
//! The JSON is attacker-controlled, so size, depth, and field count are bounded
//! before the typed parse. The size caps are the load-bearing ones: `std.json`
//! does NOT recurse on document depth — `Scanner` tracks nesting in a
//! heap-backed `BitStack`, `skipValue` walks containers with
//! `skipUntilStackHeight`, and dynamic `Value.jsonParse` keeps an explicit
//! stack and loops. A document of nothing but opening braces therefore costs
//! memory and returns a catchable `OutOfMemory`; it is not an uncatchable stack
//! overflow. `scanJsonLimits` is cheap early rejection, not a crash guard.
//!
//! Everything parses into the request arena, so signal values live exactly as
//! long as the request that carried them.
const std = @import("std");
// Reach these THROUGH the starh2 module, never by relative path: this file is
// its own module, and a path import would compile a second copy of Request,
// giving a distinct type that a handler's `*starh2.Request` cannot satisfy.
const starh2 = @import("starh2");
const request = starh2.http.request;
const form = starh2.http.form;

pub const SignalError = error{
    Missing,
    Duplicate,
    MalformedPercent,
    MalformedJson,
    NestingTooDeep,
    TooManyFields,
    WrongType,
    QueryTooLarge,
    BodyTooLarge,
    OutOfMemory,
};

const max_query = 16 * 1024;
const max_body = 256 * 1024;
const max_depth = 64;
const max_fields = 1_024;

pub fn readSignalsFromQuery(comptime T: type, req: *const request.Request) SignalError!T {
    if (req.query.len > max_query) return error.QueryTooLarge;
    const signals = form.param(req.arena, req.query, "datastar") catch |err| return switch (err) {
        error.Missing => error.Missing,
        error.Duplicate => error.Duplicate,
        error.MalformedPercent => error.MalformedPercent,
        error.OutOfMemory => error.OutOfMemory,
    };
    // `signals` may alias `req.query`; both live in the request arena, so it
    // outlives this call either way.
    return parseSignals(T, req.arena, signals);
}

pub fn readSignalsFromBody(comptime T: type, req: *const request.Request) SignalError!T {
    if (req.body.len > max_body) return error.BodyTooLarge;
    return parseSignals(T, req.arena, req.body);
}

fn parseSignals(comptime T: type, allocator: std.mem.Allocator, json_bytes: []const u8) SignalError!T {
    // Bound the document before the parser allocates for it. See the module
    // header: this is early rejection, not a crash guard.
    try scanJsonLimits(json_bytes);
    // The LEAKY parse is the correct one here, because `allocator` is the
    // request arena: every value dies with the request, and no per-parse
    // cleanup is wanted. `parseFromSlice` would build a second arena inside
    // this one and hand back a `Parsed(T)` whose `deinit` nobody may call.
    // `ignore_unknown_fields` is required — the client sends its whole signal
    // set, and a handler declares only the signals it reads.
    return std.json.parseFromSliceLeaky(T, allocator, json_bytes, .{
        .ignore_unknown_fields = true,
    }) catch return error.MalformedJson;
}

/// Bound depth and field count before the parser allocates.
///
/// This is a byte scan and not a parse. It does not validate JSON — that is the
/// real parser's job. String tracking with escape handling is required even so:
/// a brace inside a quoted string is data, and counting it would reject valid
/// documents.
///
/// It is defence in depth, not a crash guard: `std.json` parses iteratively, so
/// deep nesting costs memory rather than native stack, and `max_query` /
/// `max_body` already bound that. See the module header.
fn scanJsonLimits(bytes: []const u8) SignalError!void {
    var depth: i32 = 0;
    var max_d: i32 = 0;
    var fields: usize = 0;
    var in_string = false;
    var escape = false;
    for (bytes) |c| {
        if (in_string) {
            if (escape) {
                escape = false;
            } else if (c == '\\') {
                escape = true;
            } else if (c == '"') {
                in_string = false;
            }
            continue;
        }
        switch (c) {
            '"' => in_string = true,
            '{', '[' => {
                depth += 1;
                if (depth > max_d) max_d = depth;
                if (max_d > max_depth) return error.NestingTooDeep;
            },
            '}', ']' => depth -= 1,
            ':' => fields += 1,
            else => {},
        }
        if (fields > max_fields) return error.TooManyFields;
    }
}

/// Build a Request that owns nothing but its arena — signal reads only touch
/// `query`, `body`, and `arena`.
fn testRequest(arena: std.mem.Allocator, query: []const u8, body: []const u8) request.Request {
    return .{
        .method = .GET,
        .scheme = "http",
        .authority = "localhost",
        .path = "/x",
        .query = query,
        .headers = &.{},
        .body = body,
        .trailers = &.{},
        .arena = arena,
    };
}

const TestSignals = struct { a: i64 = 0, b: []const u8 = "" };

test "signals decode from a percent-encoded query" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // {"a":7,"b":"hi"}
    const req = testRequest(arena.allocator(), "datastar=%7B%22a%22%3A7%2C%22b%22%3A%22hi%22%7D", "");
    const got = try readSignalsFromQuery(TestSignals, &req);
    try std.testing.expectEqual(@as(i64, 7), got.a);
    try std.testing.expectEqualStrings("hi", got.b);
}

test "the signal parameter's own contract: named, present, and unique" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // Only the `datastar` parameter carries signals.
    const other = testRequest(a, "other=%7B%7D", "");
    try std.testing.expectError(error.Missing, readSignalsFromQuery(TestSignals, &other));
    // A repeated parameter is refused rather than resolved — see form.findParam.
    const dup = testRequest(a, "datastar=%7B%7D&datastar=%7B%7D", "");
    try std.testing.expectError(error.Duplicate, readSignalsFromQuery(TestSignals, &dup));
    // A decode failure surfaces as itself; form.zig owns which escapes are legal.
    const bad = testRequest(a, "datastar=%+A", "");
    try std.testing.expectError(error.MalformedPercent, readSignalsFromQuery(TestSignals, &bad));
}

test "oversized input is refused before any parse" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const big_query = try a.alloc(u8, max_query + 1);
    @memset(big_query, 'x');
    try std.testing.expectError(error.QueryTooLarge, readSignalsFromQuery(TestSignals, &testRequest(a, big_query, "")));
    const big_body = try a.alloc(u8, max_body + 1);
    @memset(big_body, 'x');
    try std.testing.expectError(error.BodyTooLarge, readSignalsFromBody(TestSignals, &testRequest(a, "", big_body)));
}

test "signal parse allocates once, not twice" {
    // Mutation canary for the double parse: the body was parsed by
    // `parseFromSlice` (which builds its own arena) and then parsed AGAIN by
    // `parseFromSliceLeaky`, with the first result discarded. Both parses
    // allocated, so the allocation count is what makes the wasted pass visible
    // — the returned value was identical either way and no behavioural test
    // could see it.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var counting = std.testing.FailingAllocator.init(arena.allocator(), .{});
    const req = testRequest(counting.allocator(), "", "{\"a\":7,\"b\":\"hi\"}");
    const got = try readSignalsFromBody(TestSignals, &req);
    try std.testing.expectEqual(@as(i64, 7), got.a);
    // The single leaky parse of this document needs one allocation for `b`.
    // The discarded first parse cost three more.
    try std.testing.expect(counting.allocations <= 1);
}
