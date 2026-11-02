## Pick

Keep the token as the hard ceiling (option 1). Add a per-route orphan cap counted in both handlers and arena bytes, which refuses new task handlers on that route only (option 2, route variant). Log the first orphan per route, and keep the rest as counters. On shutdown, wait up to a deadline, then exit the process without `deinit` and name the routes that still hold orphans (option 4b). Do not build a per-peer cap (option 3).

## Reasons

- [LOGIC] An orphan comes from a defect in a route's handler. The peer only triggers it. So the route is the right unit for the limit. A route cap keeps the damage inside the broken route, and every other route keeps working. A server-wide cap (option 2, global) refuses every route. Today's token does the same, only later.
- [LOGIC] The token count is not the limit that binds first. The request pool is. `request_bytes_per_server = 256 MiB` and `request_arena_bytes = 4 MiB` (src/core/limits.zig:196, :203), so 64 orphans with full arenas hold the whole pool. Then every request is refused, oneshots and HTTP/1.1 included. After the gate closes, an orphan's arena is still charged "against its own cap and the server's only" (request_pool.zig ~:651). That is why the route cap must count bytes as well as handlers.
- [LOGIC] The per-connection limits do not apply to orphans. `orphanSlot` gives back the connection's budget (`b.uncharge(link.arena.conn_charged)`). The session also releases the stream token when the connection ends (connection.zig:2338). So one client that opens a connection, sends requests to the bad route and closes it, again and again, builds up orphans. It never needs 16 connections at the same time.
- [PREMISE: production sits behind NAT, a proxy, or rotating IPv6] A per-peer cap uses an identity that is not reliable, and it blames the wrong party.
- [PREMISE: a supervisor such as systemd or k8s sends SIGKILL after a grace period] If the wait has no limit, one stuck handler blocks every deploy until SIGKILL, and no log names the cause. A bounded wait followed by exit keeps premise 3: memory is never freed while a task can reach it, because the process ends.

## What each option understates

- Option 1 [LOGIC]: misses (a) the arena bytes in the request pool, (b) the task itself: `resourceUpperBound` counts tasks as `max_streams_per_server + cancellation_reaper_tasks` (limits.zig:656-659); orphans run outside the stream count. Logging every orphan lets an attacker control log volume.
- Option 1 [LOGIC]: a handler that never yields holds one executor thread for ever. No admission policy can fix that.
- Option 2 global [LOGIC]: moves the failure earlier and still hits the whole server; counts only handlers, not bytes.
- Option 4a [LOGIC]: `waitOrphanedHandlers` swallows cancellation (`zio.sleep(...) catch {};`, server.zig:413), so even a runtime shutdown cannot leave the loop. Inference: if a cancel stays set after delivery, it spins.

## Failure scenario

`/upload` buffers 4 MiB then swallows cancel. Route cap 16 handlers / 32 MiB. A client repeats 16 POSTs + close. The byte cap trips at 8 orphans; `/upload` is refused, other routes work, pool keeps 224 MiB. Residual: N non-yielding orphans (N = executors) stop the server.

## Corrections

- Serial connections give the same result as 16 concurrent ones.
- Complete oneshots never take a token (connection.zig:7731). HTTP/1.1 task handlers take the same token (h1.zig:830).
- `orphaned_handlers` is read only by `waitOrphanedHandlers` and the deinit assert; nothing exposes or logs it. `StreamLink.matched` holds the route.
- Default jobs (4096) == max_streams_per_server (4096); the first orphan makes the token the tighter limit.
