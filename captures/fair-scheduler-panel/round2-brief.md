# FairScheduler panel, round 2: respond to the other reviewers

Round 1 brief: `tools/fair-scheduler-brief.md`. The premises P1-P7 in it still
hold, and the [LOGIC] / [PREMISE] labelling rule still applies: unlabelled
findings are discarded.

Four reviewers answered round 1. Their answers are in this directory:

- `fable.md` (Anthropic; a condensed record of its answer)
- `gpt-5.6-sol-xhigh.md` (OpenAI)
- `grok-4.7-xhigh.md` (xAI)
- `gpt-6-astra.md` (OpenAI; the file holds two drafts of one answer)

All four picked reading B (round-robin fairness, id order incidental). That is
settled; do not re-argue it.

## Step 0, before you read any file

As the first line of your reply, restate from your own memory of round 1, in
one sentence, the mechanism you proposed for waking a window-blocked stream.
Do not open your own round-1 file to do this.

## Facts checked against the code by the panel owner

- F1. `src/core/flow.zig:109`: `availableSend` returns 0 when either the
  stream window or the connection window is <= 0, and otherwise
  `@min(self.send, conn_send)`. `Session.streamSendAvailable` returns that
  value for a live stream. So the `stream_win` callback cannot tell "this
  stream has no credit" from "the connection has no credit".
- F2. `src/edge/fair_scheduler.zig:725-732`: `takePending` frees the slot
  (returning the slab to the pool) and then returns a copy that still points
  at that slab. Nothing in `src` or `tests` calls it.

## The four disputes. Answer each one.

D1. Combined credit. Grok's design moves a stream to the blocked list after a
successful quantum when `stream_win <= 0`, and does not rearm blocked streams
on a connection WINDOW_UPDATE. With F1, a quantum that uses up the last
connection credit makes `stream_win` return 0 for a stream that still has its
own credit. Is that stream then stranded until `checkSlowConsumers`? Does any
other round-1 design have the same defect? Is Astra's stream-local credit
query the fix, or is there a cheaper one?

D2. Waking blocked streams. Three proposals: Fable's coarse
`send_credit_gen` counter (wake every blocked stream on any credit increase);
Grok's one pending stream id plus a broadcast flag (a second distinct id in
one ingest becomes a broadcast); Astra's Session-owned journal of up to
`max_streams` changed ids plus an `all_changed` bit on overflow (Sol asked for
an exact per-stream notification without naming a transport). Pick one. Judge
them on this shape: a client downloading N large responses at once sends a
WINDOW_UPDATE for many of those N streams in a single read, every read.

D3. Guarding the builder callback. Today `emitOneData` re-checks
`pw.stream_id == sid` after `build_data_frame`, because a nested cancel can
remove the stream during the build. Once slots come from a free list, can
the slot be freed AND reused for a different stream inside one build call, so
that the id check is not enough? Astra proposed a per-slot generation. Is it
needed? Cite the code path that could reuse a slot inside a build, or say why
none exists.

D4. First-byte wait. With a ready ring, a new stream joins the tail and waits
up to one full pass (up to (n-1) x 16 KiB of other streams' DATA) before its
first frame. The workload this stack is built for is long-lived SSE streams
beside one-shot page loads on the same connection (module comment at the top
of `fair_scheduler.zig`). Is that acceptable? If not, propose an insertion
rule that keeps "no second quantum before every ready stream had one" and
says where a new stream goes. Name the case your rule makes worse.

## Then

- Say where you changed your mind since round 1, and why. Saying "no change"
  is fine when true.
- Name the single strongest point another reviewer made that you missed.
- Do not edit any file. Write your full answer as your final message.
