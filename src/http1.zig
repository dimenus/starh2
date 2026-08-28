//! HTTP/1.1 sibling: a request/response codec and a one-shot connection helper.
//!
//! Not an h2c Upgrade path and not a second parser bolted onto `Session`.
//! HTTP/1.1 messages are a different framing from HTTP/2 frames, so they live
//! in this module and share `std.Io` the same way the h2 edge does.
//!
//! First cut: Content-Length bodies, `Connection: close` that actually
//! half-closes, a small server, and a one-shot client. Chunked encoding,
//! pipelining, keep-alive reuse, `CONNECT`, and TLS wait.
pub const codec = @import("http1/codec.zig");
pub const oneshot = @import("http1/oneshot.zig");

pub const Limits = codec.Limits;
pub const Header = codec.Header;
pub const Error = codec.Error;
pub const HeadEnd = codec.HeadEnd;
pub const RequestHead = codec.RequestHead;
pub const ResponseHead = codec.ResponseHead;
pub const parseRequestHead = codec.parseRequestHead;
pub const parseResponseHead = codec.parseResponseHead;
pub const writeRequest = codec.writeRequest;
pub const writeResponse = codec.writeResponse;
pub const WriteError = codec.WriteError;

pub const Handler = oneshot.Handler;
pub const Reply = oneshot.Reply;
pub const Response = oneshot.Response;
pub const Server = oneshot.Server;
pub const ServerConfig = oneshot.ServerConfig;
pub const BindState = oneshot.BindState;
pub const ServeError = oneshot.ServeError;
pub const serveConn = oneshot.serveConn;
pub const get = oneshot.get;
pub const exchange = oneshot.exchange;
pub const readResponse = oneshot.readResponse;
pub const AcceptDisposition = oneshot.AcceptDisposition;
pub const acceptDisposition = oneshot.acceptDisposition;

test {
    _ = codec;
    _ = oneshot;
}
