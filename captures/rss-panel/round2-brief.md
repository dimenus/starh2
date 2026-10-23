# RSS panel, round 2: respond to the other reviewers

Round 1 brief: `tools/rss-brief.md`. Premises P1-P4 still hold, and the
[LOGIC] / [PREMISE] rule still applies: unlabelled findings are discarded.

Four answers are in this directory: `fable.md` (Anthropic; condensed record
of its answer), `gpt-5.6-sol-xhigh.md` (OpenAI), `grok-4.7-xhigh.md` (xAI),
`gpt-6-astra.md` (OpenAI; two drafts of one answer).

## Step 0, before you read any file

As the first line of your reply, restate from your own memory of round 1, in
one sentence, the design you proposed for per-request memory (Request.arena).
Do not open your own round-1 file to do this.

## Settled by all four (do not re-argue)

- The `Connection.init` loop (`connection.zig:1655-1662`) is redundant for
  every `HandlerJob` field except `arena`; removing it needs a per-slot
  "arena initialised" flag outside the job array, arena setup on first
  header lease or dispatch, and teardown that skips uninitialised slots.
- DATA frames above ~503 payload bytes are GPA-allocated
  (`frame_slab_bytes = 512`, `connection.zig:4908`, `session.zig:274-280`),
  and `leaseWriteChunk` falls back to the GPA (`connection.zig:4941`). Both
  contradict "no allocation on the write path" and must go.
- `max_streams_per_connection = 256` is too high a default for this
  workload; the server preface must advertise the configured value.
- Returning memory to the general allocator does not by itself lower RSS.

## Facts checked against the code by the panel owner

- F1. The general allocator a starh2 application gets is glibc's
  `c_allocator`, not `SmpAllocator`: every starh2 module sets
  `link_libc = true` (`build.zig:93,118,152`), and Zig 0.17's `std.start`
  picks `std.heap.c_allocator` for `std.process.Init.gpa` whenever libc is
  linked and the safe allocator is off (`lib/std/start.zig:794-797`).
  CAUTION: the RSS numbers in the round-1 brief were measured with an
  HttpArena entry that hard-coded `std.heap.smp_allocator`, so they are
  SmpAllocator numbers, not glibc numbers.
- F2. `frame.serverPrefaceSettings` hard-codes `max_concurrent_streams =
  256` (`frame.zig:523`) under a comment saying it mirrors
  `Limits.max_streams_per_connection`; nothing enforces that.
- F3. The trap test helper at `connection.zig:6725-6743` sets job fields
  one at a time but never `arena` or `task_counted`, then calls
  `finishHandlerJob`, which uses both. It depends on the init loop.

## The three disputes. Answer each one.

D1. Per-request memory. Two shapes are on the table:
  - Pool (Fable, Grok): one server-wide pool of fixed request buffers,
    `max_streams_per_server` of them, rented per request with the lowest
    free bit, never zeroed, counted once in `resourceUpperBound`; a bump
    allocator over the buffer; `OutOfMemory` past it; a connection with no
    live stream holds none.
  - Metered (Sol, Astra): request arenas on the general allocator, charged
    against per-stream / per-connection / server ceilings, freed (no
    retention) when the handler finishes, plus explicit render scopes.
  With F1 (glibc retains freed chunks below its mmap threshold in
  per-thread arenas), which shape actually lowers RSS for the workload in
  the brief? If you keep the metered shape, say exactly what makes its
  freed memory leave RSS under glibc, and what that costs. If you take the
  pool, answer the pool's weak point: resident pages of a pool buffer stay
  resident once touched, so the pool's RSS is its high-water of concurrent
  requests times bytes written. Is that bounded well enough?

D2. Budget size and large bodies. 64 KiB (Fable) or 256 KiB (Grok, Sol,
  Astra) per request? And for a body bigger than the budget: Fable proposes
  `Response.bodyWriter()` (a `std.Io.Writer` draining into the stream slab),
  Grok a second rented render buffer, Astra render scopes. Pick one and say
  what a Datastar SSE handler that renders a 200 KiB fragment every few
  seconds does under it.

D3. Write chunks. Keep them per connection with a LIFO (or lowest-free-bit)
  free list and fewer chunks (Fable, Astra), or move to one server-wide
  pool (Sol)? Consider: an exhausted pool must stop the drain turn and wake
  the right actor later; a per-connection pool can only be exhausted by its
  own connection. Also say whether DATA should be encoded straight into the
  leased chunk (Fable, Sol, Astra) or into `plaintext_scratch` first (Grok),
  given that `makeDataFrame` debits flow control before the sink accepts the
  bytes.

## Then

- Say where you changed your mind since round 1, and why. "No change" is
  fine when true.
- Name the single strongest point another reviewer made that you missed.
- Do not edit any file. Write your full answer as your final message.
