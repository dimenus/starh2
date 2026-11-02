## Pick

Keep the single server-wide task-handler token as the memory bound (renamed), add a small per-ROUTE orphan cap that refuses new task dispatches on the route that made the orphans (with a 503, not REFUSED_STREAM), log each orphan with its route and age, and make shutdown wait a configured limit, then log the survivors and return an error from `serve` so the process exits without `deinit`.

## Reasons

- [LOGIC] An orphan is an application bug on one route. The route is the unit to refuse; refusing it stops the leak at its source. Other routes keep working.
- [LOGIC] The global token must stay: it keeps `Limits.resourceUpperBound` honest; after the spike the only bound on live handler tasks, orphans included, is the token (`StreamLink.task_token`, released in `dropLinkRef`, connection.zig:1676).
- [LOGIC] Any orphan cap can only refuse new dispatches; orphaning is not a choice.
- [LOGIC] `Found` carries no route identity (router.zig:60-65). A per-route counter needs `Router.match` to return the route index; an array sized `routes.len` in `GlobalAccounting`.

## What each option understates

- Option 1 [PREMISE: defaults]: the request pool runs out at the same count: 256 MiB / 64 KiB = 4096 blocks (limits.zig:196, request_pool.zig:83), same as 4096 tokens; each orphan holds at least one block. The lease is taken at HEADERS decode and a failed `newLease` answers 503 (connection.zig:4946). So at 4096 orphans every request on the server gets 503.
- [LOGIC] The token is shared with HTTP/1.1 (h1.zig:830).
- [LOGIC] Token refusal on HTTP/2 is RST_STREAM REFUSED_STREAM (connection.zig:7737). RFC 9113 8.7 allows retry. [PREMISE: client behaviour] Go and curl retry automatically (not checked), giving a silent retry loop. Use 503.
- Option 2 global [LOGIC]: strictly worse than option 1; 256 orphans from one connection refuse all task routes.
- Option 3 [LOGIC]: wrong axis; no per-peer dimension exists in the code. [PREMISE] CGNAT/proxy blocks many users; attackers rotate.
- Option 4 [LOGIC]: no-limit wait and exit-without-deinit end the same: the supervisor kills it with no log. A limit plus a log per survivor is better.
- [LOGIC] The deinit guard `std.debug.assert(... orphaned_handlers == 0)` (server.zig:752) is removed in ReleaseFast: a use-after-free of the request pool. Must be a real runtime check (@panic).

## Failure scenario

`/export` swallows cancel inside a 20 s section; 20 of 30 users close the tab. Cap 16: for 20 s every new `/export` gets 503 though it would recover. A prefix route (`/api/*`) is refused as a whole. Memory: token stays the hard bound; stacks are outside Limits (limits.zig:661); at 1 MiB stacks, 4096 orphans could hold up to 4 GiB.

## Corrections

- The name `cancellation_reaper_jobs` / `reaper_reserved` no longer fits HTTP/2; nothing in HTTP/2 puts a job into `ReaperPool.jobs` (only `h1.zig:1212`). Rename to `max_task_handlers`, separate from HTTP/1.1's reaper queue.
- `resourceUpperBound` (limits.zig:658) bounds handler tasks by `max_streams_per_server`, but the real bound is the token count; wrong when an operator raises it.

## Reporting

Log at `orphanSlot` (route, stream id, peer, age), at the final `dropLinkRef` (age), and when a route trips its cap. Gauges: total and per route. Shutdown logs each survivor before `serve` returns `error.HandlersStuck`.
