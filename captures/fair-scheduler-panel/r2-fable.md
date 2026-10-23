# Fable round 2 (condensed record of the subagent hand-back, written by the panel owner)

Step 0 (from memory): Session-owned `send_credit_gen` counter bumped on stream WINDOW_UPDATE, connection WINDOW_UPDATE and SETTINGS initial_window_size; on change, drain moves every blocked slot to the ready tail once per turn. (Matches round 1.)

Read: round2-brief, the four round-1 answers; connection.zig 4402-4419, 4603-4622, 1465-1490, 3509-3630, 3194-3199; session.zig 515-553, 685-701, 265-272, 218-235, 991-1022; flow.zig 106-111.

## D1 [LOGIC]
Grok's design strands the stream: makeDataFrame debits both windows (session.zig:545-546), Grok's step 7 probes stream_win after that debit, F1 makes it 0, the slot goes to blocked, and a connection WINDOW_UPDATE does not rearm. Waits for a per-stream update it may never get, or a slow_consumer kill (checkSlowConsumers, connection.zig:2400-2440). Sol has the same defect from enqueue-time classification while conn_send == 0. Astra does not (split query). Fable round 1 avoided it only because the counter also bumped on connection WINDOW_UPDATE ("correct by accident").
Cheaper fix is a rule: only the pre-build probe taken under conn_win > 0 may send a slot to blocked; after a successful build the slot goes to the ready tail unconditionally and the next pop re-probes; delete the post-build probe. Then FlowBlocked for a live stream can only mean a RST tombstone (session.zig:523-524).
Recommends Astra's stream-local query ON TOP of the rule, because the rule alone fails toward silence if a later edit adds a post-build probe. `Session.streamLocalSendAvailable`: live stream -> s.window.send clamped at 0; non-no_error tombstone -> 0; missing stream without RST -> maxInt(i32) (mirrors streamSendAvailable 1633-1638). Quantum = min(len, DATA_QUANTUM, stream_local, conn_win). tests/scheduler.zig:353 and :438 carry over as its contract.

## D2
Pick Astra's journal, fed only by successful applySendUpdate (session.zig:1004) and positive initial_window_size deltas (933-940 -> all_changed); NOT by connection WINDOW_UPDATE. Drained before drainEmit and drainEmitReceipt; cleared only by the consumer (stays intact if a turn skips drainEmit).
On the brief's shape all three are O(N)/turn against N frames. The shape that separates them: slow client, large stream-blocked set, one id credited per read, on TLS where sched.pause (connection.zig:3198) makes drain return after ~1 frame -> counter and Grok-broadcast walk up to 255 blocked slots per frame; journal does one lookup. [PREMISE: large blocked set + few credited ids per read.] No dedupe in v1; duplicates overflow to all_changed, which is bounded and correct. Rearm never probes stream_win; the next pop does.

## D3 [LOGIC]
No slot can be freed or reused inside a build on this tree. Builders: SinkCtx.buildData (connection.zig:4410-4419), ReceiptSink.buildData (4611-4622, "no nested drain"); both call only session.makeDataFrame. makeDataFrame reaches Connection only via the rentFrame hook (frame pool) and h.onRelease (`if (c.config.accounting) |a| a.releaseStream();`, connection.zig:1474-1479). removePending/takePending/freePending callers: handleWriterFailed, checkSlowConsumers, wakeHandlerWaiters (cancelHandler, sweepHandlerWaitersLocked), emitOneData itself; none reachable from makeDataFrame; actor holds session_mu for all of drainEmit. So the comment at fair_scheduler.zig:574-578 describes a path not in the tree. Generation not needed. Replace the silent `if (pw.stream_id == sid)` with an assert: a silent skip would emit the frame without the shrink and double-send the bytes on the next quantum. [PREMISE: no StreamHooks callback or makeDataFrame branch calls Connection's cancel paths; grep stream_hooks call sites is the check.]

## D4
Plain tail insertion is a regression for the named workload (module header lines 9-11); today's cursor serves most newcomers in the current pass. Rule: a slot not yet served a quantum joins the ready ring at the HEAD; after a quantum or a rearm from blocked, the TAIL. Per-slot `served: bool`; a re-entering stream (slot freed when drained) is a newcomer again. Keeps "no second quantum before every ready stream had one": between two quanta of X a newcomer is served at most once.
Worse case: a burst of new streams during a bulk download, each delaying it one quantum; a churning client can hold a download back up to max_streams_per_connection frames. Same as today's cursor; worse than tail insertion. SSE events as newcomers delay bulk by one quantum each: intended. [PREMISE: bulk download + high arrival rate.] Rejected: separate newcomer queue; id order for newcomers.

## Changed mind
D1 (rule + stream-local query), D2 (counter -> journal), D3 (remove-in-build path does not exist; assert instead of skip), D4 (head insertion for first service). Unchanged: B, index, live list, eager free, limits accounting, tests.

## Strongest missed point
Astra's: streamSendAvailable folds connection credit in, so blocked-from-that-value strands or forces rescans. Second: Grok's TLS pause point turning per-drain costs into per-frame costs.
