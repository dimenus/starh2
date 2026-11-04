//! Routes that may hold the whole server, for tests of something else.
//!
//! A task route's share of each resource it holds is the server's limit over
//! the number of task routes (`accounts.RouteTable`), and while the server is
//! at its pressure mark a route past its share is refused. So a test that
//! drives a server-wide limit, or a client's share, from one route among
//! several sees the route refused first. Such a test is about the server's
//! own caps or the client shares, not about route fairness
//! (`tests/routes.zig` has those): it gives every task route a share of the
//! whole server, the way it sets `client_identity = .none` to keep the client
//! shares out.
const starh2 = @import("starh2");

/// `routes` with every task route's share set to `limits`' server-wide limit
/// of each route resource, so no route share refuses before the server does.
/// Before route shares existed (`starh2.Route` has no `share`) it returns
/// `routes` as they are, so a file that uses it still compiles there.
pub fn wholeServer(comptime n: usize, routes: [n]starh2.Route, limits: starh2.Limits) [n]starh2.Route {
    var out = routes;
    if (comptime !@hasField(starh2.Route, "share")) return out;
    for (&out) |*r| {
        if (r.handler != .task) continue;
        r.share = .{
            .task_handlers = limits.max_task_handlers,
            .request_bytes = limits.request_bytes_per_server,
            .compression = limits.compression_contexts_per_server,
        };
    }
    return out;
}
