# Panel brief: option A, real backpressure on the connection outbound budget (t-3538)

You are a design reviewer from a different vendor than the author. Your cwd
is a git worktree of starh2 on branch `carmack/option-a`, at master 3ba492c
plus this file. Read the code; do not edit anything; do not run the test
suite.

## What is wrong today (measured; settled)

starh2 is a Zig 0.17 HTTP/2 server on the zio coroutine runtime. Each
connection has an outbound byte budget, `outbound_bytes_per_connection`
(4 MiB default; `src/core/limits.zig`), beside a per-stream cap
(`outbound_bytes_per_stream`, 64 KiB) and a server cap
(`outbound_bytes_per_server`). The budget counts two kinds of bytes
(`OutboundKind { pending, wire }`, `Connection.tryReserveOutboundBytes`):
pending (a handler wrote them; they wait in the scheduler) and wire (handed to
the write pump; released by the write ack, `applyWriteAck`).

When more than 4 MiB is in flight on one connection, the budget FAILS instead
of applying backpressure (investigation `tools/t3538-brief.md` and the t-3538
commit message history on master; reproduced 10 of 10 on h2c and TLS with
`h2load -c 1 -m 100 /fragment-task?kb=64`):

1. A handler write is refused at once when the connection budget is full
   (`enqueuePending` waits for room in the STREAM's slab,
   `waitForStreamSpace`, but not for the connection budget), so a committed
   response is reset with `RST_STREAM(INTERNAL_ERROR)`.
2. On h2c, moving a DATA frame from pending to wire (`sendAccountedWire`)
   reserves its wire bytes while its pending bytes are still charged, so it
   briefly needs the frame's size twice; when that fails, the drain returns
   OutOfMemory and the connection ends (`data_sink_catch`).
3. On TLS, the RST burst from (1) overflows the 32-entry terminal control
   queue (`max(control_entries_per_connection / 8, 16)`; t-3562) and the
   connection ends.

Boundary: in-flight bytes above 4 MiB on both modes; 200 streams of 16 KiB
pass.

## Settled (do not reopen; a finding that depends on their negation is out of scope)

- The owner chose **option A**: count each byte once; park a handler's write
  until the budget has room (a write completion wakes it); the scheduler
  leaves DATA queued instead of failing. A full budget is backpressure, as
  `waitForStreamSpace`'s doc comment already says the design is.
- Rejected by the owner: failing only the stream (option B) and raising the
  default budget (option C; it only moves the boundary).
- Recent master, read their commit messages: 233dbe3 (t-3561, the h2c
  write-ack ledger, `waitForAckRoom`, pause hooks) and 3ba492c (slot pool
  stage 3: write-pool backpressure, ONE merged h2c pause rule `h2cPauseFor`,
  small frames packed into the drain batch, a bare flush barrier waiting
  outside `session_mu`). Option A must build on that pause rule, not beside
  it.
- zio owns concurrency (`std.Io` abstractions banned); nothing may park
  while holding `session_mu`; no external users, breaking changes free.

## Questions (answer each; be decisive; a survey with no pick is discarded)

1. **Count once.** Define the budget's states and every transition of a
   byte from handler write to write ack, on h2c and on TLS (the TLS stash
   and pump differ). Where exactly does the double count in (2) happen, and
   what is the single-count rule? Quote the lines that change.
2. **Where a handler parks.** In `enqueuePending` (or where?), how does a
   handler wait for connection-budget room without holding `session_mu`, and
   what wakes it (h2c write ack; TLS completion)? Reuse `waitForStreamSpace`
   / `waitForAckRoom`'s protocol or not? What does a waiter do on cancel,
   on a stream reset, on the connection ending, on a slow-consumer deadline?
3. **Wake order and fairness.** Many streams wait on one budget. FIFO, round
   robin, or wake-all-and-retry? What stops one large stream from starving
   the others, and what stops a thundering herd? Is a reservation (a waiter
   is promised N bytes before it wakes) needed?
4. **The scheduler side.** DATA must stay queued when the wire side has no
   room. Is that a new condition in `h2cPauseFor` (and its TLS
   counterpart), or does counting once make the drain unable to exceed the
   budget at all? What about HEADERS and frame headers, which are charged as
   wire bytes with no pending counterpart?
5. **Deadlock proof.** List everything that holds budget bytes, and show
   none of them waits on something that waits on the budget. In particular:
   a stream's bytes are pending in the scheduler while its handler waits;
   controls (WINDOW_UPDATE, RST, GOAWAY) must still leave when the budget is
   full; a peer that never reads (the slow-consumer deadline must still
   fire); the per-stream cap and the server cap nesting with the connection
   cap.
6. **Fold or not.** (a) t-3562: the terminal queue holds 32 while 256
   streams can each need an RST; with A, does the RST storm still happen
   (are there other sources of RST bursts: peer resets, deadlines)? Size it,
   or leave it? (b) t-3567: TLS bare flush barriers push to the TLS stash
   outside its pause hooks; is that the same wait as A's, and should it be in
   this change? Recommend for each, with the reason.
7. **The proof.** Which tests and which A/B show it: `tools/t3538-rate.sh`
   cells that must go from failing to 0 (h2c and TLS, `-m 100` and `-m 200`
   at kb=64 and kb=128); no throughput loss on the standard shapes; a test
   that a full budget parks and resumes without a reset.
8. Anything that would make this design fail, or a part of it you think is
   wrong (with evidence).

## Output

Every finding must carry exactly one label, or it is discarded:

- [LOGIC]: holds regardless of any deployment fact.
- [PREMISE]: depends on a deployment fact; name the fact.

Quote at least one line of code for each claim about the code.
