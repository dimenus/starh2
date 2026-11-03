# Brief: t-3794, h2c settles each written chunk by the bytes the socket accepted

You take this from the same seat and worktree as DST stages 0 and 1
(`/Users/rsaunderson/Source/mine/starh2-wt-dst`, branch `carmack/dst-witness`).
Tripwire: HEAD is the commit adding this brief; its first parent is `ca9f89f`.

## The defect (owner's decision, 2026-11-03)

The h2c `WritePump` writes a batch of chunks with one `writeVecAll` and one
`flush`. When either fails, it fails EVERY chunk of the batch
(`src/edge/wire_pump.zig`, the `write_result catch` and `flush() catch`
blocks in `WritePump.run`), including chunks whose bytes the socket had
already accepted (`SocketWriter` counts accepted bytes at every partial write).
So a handler's `send` can return an error for a response the client received
in full. TLS acks at staging (`tls.zig`, "ack each chunk once its records are
staged"), so the two transports disagree.

Decision: settle each h2c chunk by the bytes the socket accepted. A chunk
whose bytes were all accepted before the failure settles ok; the chunk the
failure cut through, and every later chunk, settle failed. Then the DST oracle
can forbid "send error, peer saw the complete response" on both transports
(stage 3 depends on this; its brief has an exception that this fix removes).

## Premises

- No scheduling hooks in the actor. Do not change the batching, the flush
  barrier semantics ("a cancelled pump must not ack a receipt whose bytes are
  still sitting here"), the write-byte clock, or the order acks are posted in.
- Accepted means accepted by the socket, not copied into the writer's buffer.
  A chunk still in the `std.Io.Writer` buffer at the failure is not accepted.
- Read `SocketWriter.drain`, `WritePump.run`, `releaseChunk`, and every
  consumer of a failed chunk ack (tickets, `takeWriteAck`, outbound budget)
  before changing anything; a chunk that now settles ok must release exactly
  what an ok ack released before.

## Tests

- A failing test first on `ca9f89f`: an h2c response whose bytes the socket
  accepts in full, followed in the same batch by a chunk whose write fails;
  the first response's `send` must return ok. Make the failure deterministic
  (a transport-level seam that already exists, or a socketpair whose peer
  closes at a known byte count). No hooks in the actor.
- The cut chunk and later chunks still fail, with the right cause.
- Mutation-test: settle the cut chunk ok; settle the whole batch ok; settle
  the whole batch failed (the old code). Each must fail a test.

## Gates

As in your stage-1 brief: Mac and nachos (`~/src/dst-witness`) ci once each
on `git archive` exports; the new tests 5 times per machine run directly;
`dst-transport` still green; never write the shared Zig cache by hand. Bench:
this is the h2c write hot path, so 6 alternating pairs of `ca9f89f` against
your commit on the Mac, h2c and TLS arms, `failed == 0` first.

## Commit and report

One commit with a decision-record message. Never push. Append a "t-3794"
section to `tools/dst-stage1-report.md`. Stop and report on any design question.
