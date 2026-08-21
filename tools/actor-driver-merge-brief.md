# Brief: merge the TLS driver into the actor (one task per connection)

The TLS connection today runs TWO long-lived tasks: the actor
(`Connection.run`, `src/edge/connection.zig`) and the CQ driver
(`tls_edge.Pump.run`, `src/edge/tls.zig`). A oneshot request crosses them
about five times: driver wakes on the recv completion, actor wakes on the
plaintext chunk, driver wakes on the outbound chunk, driver parks on the
socket write, actor wakes on the write ack. This cut makes the actor the
CQ driver. One task owns the SSL object, the socket completions, the
session, the scheduler, and the wire: a request is one recv completion in,
one send completion out, on one task. h2c keeps ReadPump/WritePump and is
out of scope.

Read `AGENTS.md` first. Its Premises and Ownership sections are binding.
The numbers and the design below are the owner's; where this brief and the
code disagree, stop and report, do not guess.

## Premises (settled; a finding that depends on their negation is out of scope)

- The stack is malleable with no external consumers. Breaking internal
  APIs is free and preferred over a compatible shim. Do not keep the old
  two-task TLS shape reachable behind a flag.
- `src/edge` takes zio directly (decided 2026-08-20 for the CQ driver). No
  interface over the CQ or the channels.
- `session_mu` is the one mutex, covering Session + FairScheduler + the TLS
  cipher. A NEW mutex is the signal that a task should have owned the state.
  This cut adds no mutex.
- Only the actor emits: every production byte passes through
  `FairScheduler`'s sink (`drainEmit` -> `queueWire`). The mutation canary
  `test_queue_wire_bypass == 0` must stay true.
- One SSL owner. After the merge that owner IS the actor task. No other task
  calls SSL_read/SSL_write.
- Handlers stay separate tasks (SSE) or inline (complete oneshots, as today).
  Their writes reach the wire only through the actor, as today.
- The t-866 park invariant survives: the actor must never wait while
  `Conn.pendingInbound()` is true (`tls.zig` pins the predicate with a
  deterministic record test; keep it green).
- Bounded by construction: no new per-connection allocation beyond what
  `limits.resourceUpperBound()` already declares. If you need a buffer, use
  one that exists (see M1) or add it as a declared term and say so.

## What is measured, and the control you must beat (nachos, Linux, 12c/24t)

Measured 2026-08-21 on master `8928046` with `tools/sse_bench/perf-arms.sh`
(conns50 oneshot shape: 50 TLS conns, 500 workers, every arm pinned to the
same executor width) and `tools/sse_bench/mixed.sh` (32 SSE streams + 8
oneshot workers on ONE TLS connection, width 2). Logs:
`captures/nachos-syscalls-b68a356/`, `captures/nachos-hyper-b68a356-width/`.

| row | starh2 today | hyper (Rust) today |
|---|---|---|
| conns50, width 12: CPU per request | 18.7 µs | 10.1 µs |
| conns50, width 12: io_uring_enter per request | 2.96 | (epoll 0.47 + futex 1.09) |
| conns50, width 12: voluntary context switches per request | 1.81 | 0.63 |
| conns50, width 2: CPU per request | 6.6 µs | 5.1 µs |
| mixed, width 2: oneshot rps while 32 SSE streams live | 164-166k | 187-191k |
| mixed, width 2: SSE delivery p50 / p99 (3 rounds) | 21-27 / 76-103 µs | 26-27 / 117-126 µs |
| mixed stall row: SSE p99 of the OTHER streams, oneshot rps | 74-95 µs, 165k | 117 µs, 190k |

The cost is wakes per request across two tasks. The experiment on branch
`cq-send-driver` (commit `24b1a7d`, one file) moved only the socket write
onto the CQ and bought 3% at width 12 while costing 10 µs of SSE p50: a
staged send SQE reaches the kernel at the executor's next `poll`, and with
two tasks the driver's extra turn delays it. Read that commit and its
message before you start; its staging/send path is the one you reuse.

## The design

One task. The actor's `waitForActivity` select gains `.io = &cq` (the
driver's CompletionQueue: one armed recv completion, at most one armed send
completion) and LOSES `.reads` and `.acks` for TLS connections, because:

- **Inbound.** The recv completion fires; the actor feeds the ciphertext
  into the BIO pair and SSL_reads plaintext straight into its ingest path
  (`ingestWireChunk`), then re-arms the recv when the read buffer is free.
  `read_ch`, the read-chunk pool lease (`read_free_ch`) and `WireChunk`
  hand-off for TLS go away; the inbound backpressure is the unarmed recv
  (the kernel buffer fills, TCP pushes back), exactly as the CQ driver does
  today, one task earlier.
- **Outbound.** `queueWire` for TLS no longer sends a `WireChunk` into
  `tls_write_ch`; it SSL_writes the packed wire bytes into the pair, stages
  ciphertext into the send buffer, and arms the send completion (the
  `cq-send-driver` path: `stageOutbound`, `armSend`, `onSendComplete`,
  `pending_writes`, `partial_off`). The write ack is a local function call
  (`applyWriteAck` today) at the point the record is written, so
  `write_ack_ch` has no TLS producer any more. Tickets, receipts,
  `outbound_release`, `control_release`, `complete_batch_receipt` keep
  their exact meanings; only the hop disappears.
- **Backpressure on a slow peer.** The pair is bounded and the staging
  buffer is 16 KB. When SSL_write returns WantWrite and staging is full
  behind an in-flight send, the unfinished wire batch stays pending on the
  actor (`pending_writes`), the actor takes nothing more from the
  scheduler until the send completes, and it is NOT parked on the socket:
  its select still serves handler completions, deadlines, the doorbell and
  the slow-consumer kill. That is the property the old pump split
  defended ("a peer that stops reading must not stop the actor"); state in
  the commit how the merged actor keeps it, and prove it with the stall row.
- **Teardown.** `shutdownCq` (close, `cancelAll(.keep)`, drain) runs on
  the actor at the end of `run`, after the session is terminal, before the
  socket closes. Every pending or in-flight chunk gets exactly one outcome
  (ok ack, failed ack, or `fail_all`), as the WritePump exit contract says
  in `wire_pump.zig`; nothing is dropped silently.
- **Handshake.** Unchanged: the handshake subtask on the actor, leftover
  plaintext ingested before the driver state is armed. The blocking
  `tcp_writer` is handshake-only; its buffer becomes the send staging
  buffer (no new memory).
- **The SSE path does not gain a hop.** A handler write is enqueued under
  `session_mu` and rings the doorbell; the actor emits it on its next turn
  and now also stages and arms the send on that same turn. Its latency must
  not get worse (bar below).

## Hard bars (every one must hold; none may be traded for another)

1. `./zb build ci` rc=0 (suite, exact, fuzz smoke, `tls-smoke`, release
   targets). `tls-smoke` is the ONLY gate that reaches the TLS edge; the
   unit suite cannot. Paste the `tls-smoke PASS` line.
2. **One task.** `io.concurrent(tls_edge.Pump.run, …)` no longer exists;
   `tls_edge.Pump` as a separate task type is deleted or folded; for a TLS
   connection the only tasks spawned after the handshake are handlers.
   Evidence: `grep -n "concurrent(" src/edge/connection.zig` output in the
   report, and the diag snapshot (`STARH2_PUMPWAIT` / actor park snapshot)
   no longer has a pump site.
3. `test_queue_wire_bypass == 0` stays a passing mutation canary (do not
   touch the test).
4. No new mutex, no new atomic flag that a second task reads as a wake
   (`grep -n "Mutex\|ResetEvent\|atomic.Value" src/edge/tls.zig` before and
   after, explained line by line in the report).
5. `limits.resourceUpperBound()` unchanged, or changed by a declared term
   with the reason in the commit.
6. Stall row: `STALL=1 tools/sse_bench/mixed.sh` (any OS): the stalled
   stream is the only non-delivering stream (`delivering = streams - 1`),
   oneshot rps within 10% of the same run's non-stall mixed row, and the
   SSE p99 of the others within 2x of the same run's non-stall row. Paste
   the rows.
7. macOS `mixed.sh` (3 rounds, width 2): starh2 SSE p50 and p99 not worse
   than the run's own `8928046` control by more than 10% (run the control
   from a worktree at `8928046` on the same machine in the same hour; paste
   both). The Linux bars below are graded by the owner on nachos; you do
   not need Linux access, but your report must say which bars you could
   not run.
8. Wire parity: `./zb build test-exact` and the h2spec exclusions list are
   untouched.

Linux bars the owner grades after your report (nachos, same scripts):
`perf-arms` width 12 CPU per request <= 14 µs AND io_uring_enter per
request <= 2.0; width 2 CPU per request <= 6.6 µs; `wedge-probe` PASS at
WORKERS=2 and 8; mixed SSE p50 <= 30 µs. If the width-12 bar fails but
every correctness bar holds, that is a finding, not a failure of the cut.

The laziest passing output: keep both tasks and rename, or make the actor
call into the pump synchronously through a channel that is still a hop.
Bar 2 plus the io_uring_enter bar are what rule that out; bars 6 and 7 rule
out buying the enters with SSE latency or the stall property.

## Dependency files you must read before writing a line (name them in the report)

- `zig-pkg/zio-*/src/completion_queue.zig` (the pinned zio; `ls zig-pkg`):
  `submit`, `next`, `asyncWait`, `cancelAll`, and the tests at the bottom.
  The select future protocol is what lets `.io` sit beside channels.
- `zig-pkg/zio-*/src/select.zig`: the `AsyncWaitState` protocol and the
  cancel epilogue (the fork carries the conservation fixes; read them, do
  not assume upstream semantics).
- `zig-pkg/zio-*/src/ev/completion.zig`: `NetRecv`, `NetSend`, `ReadBuf`,
  `WriteBuf`; `src/ev/backends/linux/io_uring.zig` `submit` and `poll`
  (SQEs are flushed at `poll`, one `io_uring_enter2` per poll; there is no
  eager flush, so do not add one).
- `src/edge/tls.zig` on master AND on `cq-send-driver`
  (`git show cq-send-driver:src/edge/tls.zig`): the driver loop, the t-866
  invariant, `feedCipher`, `readOne`, `ingestCipher`, the staging/send path.
- `src/edge/connection.zig`: `run`, `waitForActivity`, `queueWire`,
  `drainEmit`, `applyWriteAck`, `drainWriteAcks`, `drainWriteAcksForced`,
  `takeWriteAck`, `ingestWireChunk`, `runPendingInline`, `releaseSlot`,
  the teardown `defer` in `run`.
- `src/edge/wire_pump.zig` header (the exit contract) and `WriteCompletion`.
- `src/edge/emit_batch.zig` (packing stays here; do not put a record loop
  elsewhere).

## Milestones (commit each; the message carries problem, change, evidence)

- **M1** Land the `cq-send-driver` staging/send path into master's
  `tls.zig` as the driver's write path (the experiment commit, rebased).
  Gates: `./zb build test`, `tls-smoke`. This is a stepping stone; it is
  fine that it costs SSE latency for one commit.
- **M2** Move the recv side into the actor: the actor owns `cq`, the recv
  op, `feedCipher`/`readOne`; `read_ch` and the read pool lease are no
  longer used for TLS; `waitForActivity` gains `.io`. The pump task still
  exists for writes at this point if that keeps the diff reviewable.
- **M3** Move the write side into the actor: `queueWire` -> SSL_write +
  stage + arm; acks local; delete the pump task and `tls_write_ch` for TLS;
  teardown on the actor. Bars 1-8.
- **M4** Report: every bar with its evidence pasted, the dependency files
  you read, what you could not run, and anything in this brief you found
  wrong against the code (that is a finding; do not silently adapt).

## Rejected without new evidence (do not retry)

- Keeping `read_ch`/`tls_write_ch` for TLS "for symmetry with h2c".
- An eager `io_uring_enter` after arming the send (zio has no such API, and
  a submit-only enter per write is a syscall per request).
- A second socket-owner task for writes, however named.
- Acking on staging AND on completion (two write paths for one contract).
- Touching h2c's ReadPump/WritePump.

## Delegation mechanics

- You work in a git worktree cut from master at the commit that added this
  file. Tripwire first: `git rev-list --count HEAD..master` must print 0;
  otherwise STOP and report, do not rebase.
- Commit per milestone. Do not push. Do not touch `captures/`,
  `DIAG_BUILD_SPEC.md`, `HANDOFF.md`, or any golden fixture; a failing
  golden is a finding.
- Zig 0.16: build with `./zb`; look up std APIs with `zigstd`; never guess.
- The owner grades by rerunning the gates, not from your numbers. Paste raw
  tool output, not summaries.
