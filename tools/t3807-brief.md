# Brief: t-3807, can a TLS pump write again after a failDrain that posted no shutdown?

Same seat, worktree and branch as t-3794 (`carmack/dst-witness`). Tripwire:
HEAD is the commit adding this brief; its first parent is `8a8d4e9`.

## The question (from the task you filed)

`Pump.failDrain` gives back a held chunk without advancing the stash order
(`stash_written`). After it, a write that stages again trips `popPending`'s
"TLS stash wrote chunk N after chunk M" panic in safe builds. HTTP/1.1 is now
covered by `Pump.stopped`, which is set only by a shutdown post. Candidate:
`readOne`'s decrypt error calls `failDrain` and `postEof` with no shutdown
post; HTTP/2 may then write (a GOAWAY, an RST, a handler's DATA).

## What to do

1. Decide reachability from the code, then prove it by running: a test that
   drives a real HTTP/2 TLS connection (h2_client over a socket is fine; no
   hooks in the actor) to a decrypt error (a corrupted record from the peer)
   while a chunk is held or right before the server writes. In ReleaseSafe or
   Debug, does it panic? Run it enough times to say (state n).
2. If it is reachable: the failing test first, then the fix. The fix must keep
   the ownership rule (every chunk returned exactly once) and the stash order;
   prefer one rule for both protocols (for example, every `failDrain` leaves
   the pump in the refusing state, or `failDrain` keeps the stash order
   consistent), and say why you chose it. Extend `dst-transport` if the
   HTTP/1.1 driver can reach the same state (a receive error then a write).
3. If it is not reachable: write down why, with file:line, and a test that
   would fail if it ever becomes reachable (for example, a unit test of the
   pump: failDrain without shutdown, then writeChunks, must not panic or must
   refuse).

## Gates

As before: Mac and nachos ci once each on `git archive` exports; new tests 5
times per machine; `dst-transport` green; mutation-test the fix; never write
the shared Zig cache by hand. Append a "t-3807" section to
`tools/dst-stage1-report.md`. Stop and report on any design question.
