//! Datastar SDK re-exports and bounded signal helpers.
const std = @import("std");
const request = @import("http/request.zig");

pub const sdk = @import("datastar");
pub const patchElements = sdk.patchElements;
pub const patchElementsFmt = sdk.patchElementsFmt;
pub const patchSignals = sdk.patchSignals;
pub const executeScript = sdk.executeScript;
pub const PatchElementsOptions = sdk.PatchElementsOptions;
pub const PatchSignalsOptions = sdk.PatchSignalsOptions;
pub const ExecuteScriptOptions = sdk.ExecuteScriptOptions;
pub const PatchMode = sdk.PatchMode;

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
    const raw = extractDatastarParam(req.query) catch |e| return e;
    const decoded = percentDecode(req.arena, raw) catch return error.MalformedPercent;
    return parseSignals(T, req.arena, decoded);
}

pub fn readSignalsFromBody(comptime T: type, req: *const request.Request) SignalError!T {
    if (req.body.len > max_body) return error.BodyTooLarge;
    return parseSignals(T, req.arena, req.body);
}

fn extractDatastarParam(query: []const u8) SignalError![]const u8 {
    var found: ?[]const u8 = null;
    var iter = std.mem.splitScalar(u8, query, '&');
    while (iter.next()) |pair| {
        const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
        const key = pair[0..eq];
        const val = pair[eq + 1 ..];
        if (std.mem.eql(u8, key, "datastar")) {
            if (found != null) return error.Duplicate;
            found = val;
        }
    }
    return found orelse error.Missing;
}

fn percentDecode(allocator: std.mem.Allocator, encoded: []const u8) SignalError![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < encoded.len) {
        if (encoded[i] == '%') {
            if (i + 2 >= encoded.len) return error.MalformedPercent;
            const hi = std.fmt.parseInt(u8, encoded[i + 1 .. i + 2], 16) catch return error.MalformedPercent;
            const lo = std.fmt.parseInt(u8, encoded[i + 2 .. i + 3], 16) catch return error.MalformedPercent;
            // fix: need both nibbles
            const byte = std.fmt.parseInt(u8, encoded[i + 1 .. i + 3], 16) catch return error.MalformedPercent;
            _ = hi;
            _ = lo;
            try out.append(allocator, byte);
            i += 3;
        } else if (encoded[i] == '+') {
            try out.append(allocator, ' ');
            i += 1;
        } else {
            try out.append(allocator, encoded[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice(allocator) catch return error.MalformedPercent;
}

fn parseSignals(comptime T: type, allocator: std.mem.Allocator, json_bytes: []const u8) SignalError!T {
    // Depth/field limits via a scan then typed parse
    try scanJsonLimits(json_bytes);
    const parsed = std.json.parseFromSlice(T, allocator, json_bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }) catch return error.MalformedJson;
    // Caller arena owns — leak Parsed's arena into request arena by duping?
    // With alloc_always into allocator (request arena), deinit would free — don't deinit.
    // std.json.Parsed requires deinit; for arena we use parseFromSliceLeaky.
    _ = parsed;
    return std.json.parseFromSliceLeaky(T, allocator, json_bytes, .{
        .ignore_unknown_fields = true,
    }) catch return error.MalformedJson;
}

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
