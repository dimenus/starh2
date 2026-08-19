# Brief: memory-BIO TLS decoupling (t-843 step 1, revised)

This supersedes `tools/try-fill-brief.md` as step 1 of the t-843 plan of
record (revision 5). The fork try-fill (t-856) is PARKED: do not build it.
Everything here is starh2-only; the zio fork does not change.

## Provenance

The zio author reviewed the starh2 branch directly (zio#685 thread,
2026-08-19) and said: "std.Io.Select on the hot path is expensive, don't do
that", and: with a libssl-compatible API, "you are best using memory BIO
and then zio directly" — or move blocking socket reads to a separate task
posting to a queue, with one task consuming a shared queue to decrypt/
handle or encrypt/write. His nats.zig runs TLS from two tasks; READ IT
FIRST as prior art — a local clone is at ~/Source/oss/nats.zig
(src/connection.zig, the TlsRuntime comment near line 268, readerLoop /
flusherLoop near line 1675).

Read it for the TASK TOPOLOGY and the test discipline, not as a template:
nats.zig implements the OTHER alternative — it splits a tls.zig
`tls.nonblock.Connection` into an encrypt copy and a decrypt copy, one per
task, lock-free, because the two halves of that cipher are disjoint state
(pinned by a KeyUpdate round-trip test). That split does not transfer to
BoringSSL: an SSL object cannot be split. This brief builds design (a),
memory BIOs with a single SSL owner; the split-cipher design is recorded
on t-843 as the candidate NEXT step if grading shows crypto serialization
on the pump is the remaining residue. KeyUpdate needs no cross-task signal
in design (a) — the single owner handles it internally.

Phase-0 evidence at 8ac7c94 (nachos, observe build, 800k requests,
migration-on, /tmp/p0-trace.json): pump_select 0.5557/req,
pump_select_peek 0.2911/req. The Select and its park are the target.

## The design

The old premise "BoringSSL cannot share an SSL object" forbids sharing the
OBJECT, not the bytes. Memory BIOs decouple them:

- The SSL object keeps exactly one owner (the pump task). Its BIOs are
  memory BIOs (`BIO_s_mem` pair or equivalent in the boring package), not
  socket-coupled callbacks.
- A dedicated READ task does plain blocking zio socket reads of ciphertext
  and posts chunks to the pump's single inbound queue. Blocking reads on a
  dedicated task have no readiness edges to steal and no Select.
- The PUMP task consumes ONE queue (`queue.get()`, no Select): inbound
  ciphertext chunks get `BIO_write` + `SSL_read` (plaintext to the actor,
  as today); outbound work gets `SSL_write` + `BIO_read` of ciphertext +
  socket write. Whether the socket write happens on the pump or on a third
  write task is the builder's measured choice — nats.zig's two-task split
  is the reference; state the choice and why.
- Handshake: `SSL_accept` drives the same memory BIOs; the read task is
  already the ciphertext source. Leftover pipelined preface handling stays.

What this removes, by construction: the two-arm Select, the park-per-record
on the pump, the WANT_READ/peekGreedy coupling in `tls_async.zig`'s BIO
callbacks, and the entire readiness-edge-theft class (5ce4f91 and 6881e51
both died on it; nothing in this design touches an edge).

What it costs: one ciphertext memcpy per chunk into the memory BIO
(measured memcpys on this path are tens of ns), and one more live task per
TLS connection (the read task — the same count h2c always had).

## Constraints, unchanged from the plan of record

- One owner for the SSL object. The read task never touches SSL.
- Handler API untouched: startSse, Body.writeAll/flush/finish, distinct
  terminal errors. CompleteResponse still has no startSse.
- Production wire output stays inside FairScheduler's sink; the
  test_queue_wire_bypass canary (or its successor) stays at zero.
- Bounded memory: the ciphertext chunk pool and queue are declared limits
  terms, not unbounded buffers. `limits.resourceUpperBound()` stays true.
- No wall-clock sleep/poll/timeout in production paths.
- Do not fight zio: blocking reads, channels, and queues ARE the grain
  here — that is the point of the design.
- The chip lists in t-842/t-843 stay closed. Do not touch the task store;
  the driving session owns the records.

## Grading — carried over from the try-fill brief, all lines required

A-B-A on nachos, fix arm vs current HEAD control, own worktrees, SHA
confirmed before every run (the section-6 push/checkout flow of
tools/try-fill-brief.md applies verbatim):

1. Mixed SSE at the offered count every fix round (exact numbers; ±2
   jitter is known and shared with the Go arm).
2. Stall 31/32 delivering, every fix run.
3. Mixed oneshot within 5% of the same-session control band, or above.
4. Oneshot-only: every fix round beats every same-session control round.
   Report the margin honestly; do not tune the recipe.
5. Server CPU (mixed.sh line) within +10% of control.
6. Packing oracle exit 0 (redirect, not pipe, for the exit code).
7. `./zb build ci` on Darwin including tls-smoke — /big is the historical
   catcher for transport-ownership mistakes; a Linux-only green fails.
8. Counter re-run on the fix arm: pump_select/req must COLLAPSE (target
   near zero — the design removes the Select; a throughput win with
   unchanged Select counters is measuring something else).
9. SSE 200-stream p50 (tools/sse_bench/run.sh) must not regress vs
   control on the same box: the read task must not add a hop to the
   event path. Report Darwin too if a quiet window exists (t-855 context:
   Darwin per-event latency is the open anomaly this design may move).

## Degenerate builds this brief refuses

- A memory-BIO shim that still Selects (the Select is the defect, not the
  BIO wiring).
- A second task that touches the SSL object (the object has one owner).
- An unbounded ciphertext queue (bounded, declared, accounted).
- Buying oneshot-only by parking SSE or starving the stall story.
- Keeping tls_async.zig's socket-coupled BIO callbacks beside the memory
  BIOs "temporarily" — delete the dead path in the same change.

## Report

The nats.zig pattern summary (what it does, what you took, what you
rejected and why); the diff summary and milestone SHAs; per-round grading
lines verbatim with log paths and binary sha256; the nine hard-line
results one per line; the counter table before/after; what you could not
do and why. Every claim cites a file the grader can recompute from.
