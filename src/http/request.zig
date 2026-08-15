//! The handler-facing request.
//!
//! Every slice here points into storage owned by the handler job: decoded
//! request storage transferred out of Session, or the job's scratch arena. The
//! connection releases both after the handler returns. Nothing borrows from
//! Session or from a wire chunk, so the actor cannot free or recycle a
//! handler-visible pointer underneath it.
//!
//! The fields are already validated. `core.fields` rejected a malformed or
//! smuggling-shaped field block before this struct was built, so a handler
//! reads them without re-checking.
const std = @import("std");

/// A closed set with an `other` member, and not a string. A handler switches on
/// the method, and the router compares it as an integer. An unknown method
/// reaches `other` rather than an error, because rejection belongs to the
/// router's 405 answer and not to the parser.
pub const Method = enum {
    GET,
    POST,
    PUT,
    DELETE,
    HEAD,
    OPTIONS,
    PATCH,
    other,

    pub fn parse(s: []const u8) Method {
        if (std.mem.eql(u8, s, "GET")) return .GET;
        if (std.mem.eql(u8, s, "POST")) return .POST;
        if (std.mem.eql(u8, s, "PUT")) return .PUT;
        if (std.mem.eql(u8, s, "DELETE")) return .DELETE;
        if (std.mem.eql(u8, s, "HEAD")) return .HEAD;
        if (std.mem.eql(u8, s, "OPTIONS")) return .OPTIONS;
        if (std.mem.eql(u8, s, "PATCH")) return .PATCH;
        return .other;
    }

    pub fn toString(self: Method) []const u8 {
        return switch (self) {
            .GET => "GET",
            .POST => "POST",
            .PUT => "PUT",
            .DELETE => "DELETE",
            .HEAD => "HEAD",
            .OPTIONS => "OPTIONS",
            .PATCH => "PATCH",
            .other => "OTHER",
        };
    }
};

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Request = struct {
    method: Method,
    scheme: []const u8,
    authority: []const u8,
    path: []const u8,
    /// After a prefix route match: path bytes past the prefix. Empty for exact routes.
    path_remainder: []const u8 = "",
    query: []const u8,
    headers: []const Header,
    body: []const u8,
    trailers: []const Header,
    arena: std.mem.Allocator,
};
