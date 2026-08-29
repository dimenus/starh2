# Middleware research (starh2)

Worktree `starh2-wt-mw` at `bf8d689` (`starh2/middleware-research`, same as
`origin/master` when this note was written). Fork is `dimenus/starh2`.

This file is a research verdict. It is not protocol. `4ed40c0` rejected a
`docs/` tree for world facts; this note sits under `notes/` so it is not
protocol beside the code.

## 1. Recommendation

Do not add middleware, interceptors, before/after hooks, or a composable
wrapper list to starh2.

Keep auth, cookies, and QueryDsl view choice in the app (qmdsync). If a
handler needs a shared check, wrap `CompleteHandler` / `TaskHandler` in that
app. A five-line `fn(Handler) Handler` at the call site is enough.

Do not add a new transport hook for this question. Body cap, peer identity,
timeouts, and control-frame rate limits already live on `Limits`, and on
`Route.max_request_body_bytes` / `Request.peer` on the later
`helm/oidc-peer-body-cap` line that qmdsync already pins. Those are named
fields, not a pipeline.

starh2 is a protocol library with a small handler surface. httpz, zap, dusty,
and datastar.zig are web frameworks. Their middleware is evidence of that
shape, not a vote that this library should grow one.

## 2. Survey

| Library | Shape | What it wraps | Cost / fit vs starh2 |
|---|---|---|---|
| **starh2** (this tree) | No middleware. `Handler` is `complete` or `task` function pointers. `ServerConfig` is `endpoints`, `routes`, `tls`, `limits`. | The route table. H1 (`src/edge/h1.zig`) and H2 (`src/edge/connection.zig`) both call `router.match`. | Complete handlers may run on the connection actor and must not sleep, wait, or do I/O (`CompleteResponse` in `src/http/response.zig`). A pipeline that can block would stall ingest on oneshots. |
| **httpz** (karlseguin/`http.zig`) | Interface: struct with `init`/`execute`/`deinit`; `execute(req, res, executor)` calls `executor.next()`. Global + per-route list; `middleware_strategy` is `.append` or `.replace`. | Matched route actions. Also a `Handler.dispatch` method. | Author text in `readme.md`: use a custom dispatch for logging, authentication, and authorization; middleware is for complex route-specific logic. Dispatch is the closer analog to qmdsync `handleApiOn`. |
| **dusty** (lalinsky; vendored by datastar.zig) | Same Executor/`next` shape as httpz (`Middleware(Ctx)`, `executeFn`). | httpz-like `Request`/`Response`/`Action`. | A framework beside the server. Not a protocol library. |
| **zap** | Onion: `Middleware.Handler(Context)` with `on_request` and `handleOther`. Optional `EndpointHandler` wrap. | `zap.Request` plus a typed context struct the chain mutates. | Microframework on facil.io. Context bag is the request-global starh2 refuses. |
| **datastar.zig** (zigster64) | `Func = *const fn (*HTTPRequest) anyerror!void`. Named `Pipeline` lists. `server.use` / `usePipeline`, group, per-route. `http.halted = true` stops the rest. `http.assign` / `assigned` is a string-key bag on the request. | After route match, before the handler. Built on `std.http.Server` (HTTP/1.1). | A Datastar-aware **web framework**, not the SDK. `halted` and `assign` are request-globals. Middleware that only covers that H1 listener does not cover a starh2 H2 (or H1 channel) surface. |
| **httpx.zig** | Express-style `server.use`; `Middleware.handler(ctx, next)`. Built-ins: CORS, logger, rate limit, basic auth, compression, helmet. | A mutable `Context`. | Framework clone. Weaker evidence: it copies Express, it is not a widely used Zig HTTP core, and the `Next` trampoline in `src/server/middleware.zig` is not a model to copy. |
| **std.http.Server** | No middleware. | One connection request loop. | datastar.zig wraps this. starh2 does not. |
| **Official Datastar Zig SDK** (`starfederation/datastar/sdk/zig`) | Deprecated. README is one line. | SSE transformers, not HTTP. | Not a server. Not middleware. |

httpz `Middleware` (from `src/httpz.zig`):

```zig
pub fn Middleware(comptime H: type) type {
    return struct {
        ptr: *anyopaque,
        deinitFn: *const fn (ptr: *anyopaque) void,
        executeFn: *const fn (ptr: *anyopaque, req: *Request, res: *Response, executor: *Server(H).Executor) anyerror!void,
        // ...
    };
}
```

datastar.zig `Func` (from `src/middleware.zig`):

```zig
pub const Func = *const fn (req: *HTTPRequest) anyerror!void;
```

## 3. Datastar.zig, vs rumor

Rumor: “Datastar.zig is the Zig SDK, and it needs HTTP middleware.”

Fact, from source:

- **Official SDK** (`~/Source/oss/datastar/sdk/zig/README.md`): deprecated. The ADR (`sdk/ADR.md`) is SSE event generators. It says a send **Should** flush at once, and notes that compression middleware may interfere. That is a warning against buffering wrappers on the SSE path, not a request for an onion.
- **datastar.zig** (`~/Source/oss/datastar.zig`): a full HTTP server. README title: “A Web Framework for Zig 0.16”. Router, pipelines, pub/sub, hot reload, `*HTTPRequest`. Optional backends include httpz and dusty. SDK transformers are a separate import (`datastar-sdk.zig`) if you already have a server.
- **starh2 `starh2_datastar`** (`src/datastar.zig`): `readSignalsFromQuery` / `readSignalsFromBody` only. It does not re-export the SDK. It has no HTTP glue and no middleware.
- **qmdsync**: `build.zig.zon` pins `zigster64/datastar.zig` and `dimenus/starh2`. Production `/v1` dispatch is starh2 (`src/tasks/serve.zig` `apiRoutes` / `handleApiOn`). A prior session already recorded that datastar.zig middleware covers the HTTP/1.1 listener only; the live surface is starh2.

starh2 README Datastar section: the SDK emitters take an allocator and return bytes, so they have no transport coupling. A consumer calls them directly.

## 4. What would fit starh2 (if anything)

### This tree’s public attach surface

```zig
// src/http/router.zig — this worktree
pub const TaskHandler = struct {
    ptr: *anyopaque,
    runFn: *const fn (*anyopaque, *const request.Request, *response.Response) anyerror!void,
    stream_request: bool = false,
};
pub const CompleteHandler = struct {
    ptr: *anyopaque,
    runFn: *const fn (*anyopaque, *const request.Request, *response.CompleteResponse) anyerror!void,
};
pub const Handler = union(enum) {
    complete: CompleteHandler,
    task: TaskHandler,
};
pub const Route = struct {
    method: request.Method,
    path: []const u8,
    prefix: bool = false,
    handler: Handler,
};

// src/edge/server.zig
pub const ServerConfig = struct {
    endpoints: []const EndpointConfig,
    routes: []const router_mod.Route,
    tls: ?TlsConfig,
    limits: limits_mod.Limits = .defaults,
};
```

`Request` (`src/http/request.zig`) is a validated, const view: method, path,
query, headers, body, arena. No mutable bag. No `halted`. No thread-local.
No request-global.

H1 and H2 share that `Handler`. Middleware that only runs on one protocol is
a miss by the fit criteria.

README non-goal (also `dfa5e37`): “Framework surface: middleware, sessions,
CORS, route parameters, wildcards, static files, templates.”

### Seams that already exist

| Seam | Where | Job |
|---|---|---|
| Route table | `ServerConfig.routes` | Exact then longest prefix. 404 vs 405. |
| `Limits.request_body_bytes` | `src/core/limits.zig` | Server-wide body cap (413). This tree. |
| Timeouts | `Limits` (`field_block_timeout_ns`, `request_body_idle_timeout_ns`, `slow_consumer_timeout_ns`, `preface_timeout_ns`) | Slow peer. |
| Control-frame rate | `src/core/rates.zig` | RST/SETTINGS/PING/WINDOW_UPDATE flood. Not app HTTP rate limits. |
| `Request.peer` | `helm/oidc-peer-body-cap` (`6c9be0c`, `03d6d829`) | TCP peer without port, for an app rate-limit key. **Not on this worktree.** qmdsync pins `03d6d829`. |
| `Route.max_request_body_bytes` | same branch (`6c9be0c`, H2 ingest in `d1e1cfc`) | Per-route cap so H1 413s before it reads the body. **Not on this worktree.** qmdsync already sets it on `/v1/auth/apple/*`. |

Those last two are the named transport hooks. They are fields on `Request` /
`Route`, not a callback list. Commit `6c9be0c` rejected Host hashing, IP+port
hashing, and a global 16 KiB body cap.

### Why a library pipeline does not fit

1. **Two handler kinds.** Complete runs on the actor (`CompleteResponse.send`
   only). Task runs on its own task and may stream. One `next()` chain cannot
   be honest for both: a blocking middleware on a complete route stalls
   ingest; a chain that always spawns destroys the complete-on-actor path.
2. **Const request, explicit errors.** datastar.zig `assign` / `halted` and
   zap context bags are request-globals. starh2 passes `*const Request` and
   an error union. A bag on `Request` would hide control flow.
3. **SSE flush.** Datastar ADR: flush at once; compression middleware may
   interfere. starh2 `startSse` already writes through to the wire. A
   buffering wrapper around `Response` is the defect that ADR names.
4. **Duplicate of the app dispatcher.** qmdsync already has the “dispatch”
   httpz recommends: one `handleApiOn` that branches on path and auth.

### App-side wrapper (preferred if qmdsync wants less copy)

Against this tree’s types. Two wrappers, because the two handler kinds are
different functions. No list in the library. No `halted` flag.

```zig
const BearerGate = struct {
    inner: starh2.CompleteHandler,
    key: []const u8,

    fn run(ptr: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
        const self: *BearerGate = @ptrCast(@alignCast(ptr));
        if (!checkBearerReq(req, self.key)) {
            try resp.send(401, &.{.{ .name = "content-type", .value = "application/json" }}, "{\"error\":\"unauthorized\"}");
            return;
        }
        return self.inner.runFn(self.inner.ptr, req, resp);
    }

    fn handler(self: *BearerGate) starh2.Handler {
        return .{ .complete = .{ .ptr = self, .runFn = run } };
    }
};
```

A task-handler twin uses `*starh2.Response` and the same check. Compose at
route-build time in the app. Skip the wrap on `/v1/health` and
`/v1/auth/apple/*`.

Rejected library shapes:

- `[]Middleware` on `ServerConfig` — hides the actor vs task split; H1/H2
  would both pay it, including complete-on-actor.
- Single `onRequest: fn(*Request) error{Skip}!void` — looks small, still
  hides control flow, still cannot send a 401 without a `Response`, still
  cannot be one type for complete and task.
- Onion `fn(Handler) Handler` **inside starh2** — the app can already do
  this. A library list adds merge strategy (httpz `append`/`replace`) that
  this stack does not need.

## 5. What must stay in qmdsync

Verified in `~/Source/mine/qmdsync` (`src/tasks/serve.zig`, `src/tasks/auth.zig`).

- **Bearer key** (`checkBearerReq`): `/v1/*` except health. Fleet identity.
- **QueryDsl** (`checkQueryDslAuth`): bearer or `x-api-key`. A different
  check on `/api/querydsl/*`.
- **SIWA cookie**: `POST /v1/auth/apple/native` mints `tasks_session`.
  `auth.zig` states the cookie is not an API credential; `/v1/*` still uses
  the bearer key. Cookie parse/mint/verify stay in `auth.zig`.
- **Public auth routes**: nonce, native, logout return before the bearer
  check. Peer throttle (`authThrottled` / `Request.peer`) and
  `native_body_max` are app policy that consume starh2 transport fields.
- **Run / capability**: `X-Task-Run` and `X-Task-Capability` are read in
  `handleApiOn` and checked in `dispatchWithHeaders` (register run, register
  repo, household project/task writes). That is store-backed capability, not
  transport.
- **Household vs fleet views**: QueryDsl `FROM household` vs agent-queue
  views live in `query_views.zig`. Route prefix `/v1/household/` is app
  dispatch.

The two-db split (household SIWA vs fleet run/capability) is app identity
plus which sqlite file a handler opens. It is not a missing starh2 hook.

Treat “auth middleware” for that split as a smell: two checks that differ by
path and by store will diverge if they share one pipeline, and the pipeline
cannot see the store. Keep the checks next to the dispatch that already
knows the path.

`qmdsync-wt-two-db` on `household/two-db` had no unique commits vs
`origin/master` in this pass. The split may still be in progress elsewhere.
The rule does not depend on that branch: household SIWA and fleet
run/capability stay in qmdsync.

## Decision

Do not grow middleware in starh2.

Do not open an implementation follow-up in this library for auth, cookies,
or QueryDsl.

If qmdsync wants less copy, wrap handlers there. If a new consumer recopies
a **transport** cap (body, peer, timeout), that is a `Limits` / `Route`
field, in the same shape as `max_request_body_bytes`, not a pipeline.
