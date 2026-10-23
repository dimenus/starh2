A stream parked on the blocked list is woken by `rearmStream` on the next drain when Session recorded that one stream id from a successful `applySendUpdate`, or by `rearmAllBlocked` when a second distinct id or a positive initial-window change set the broadcast flag; a connection `WINDOW_UPDATE` does not rearm anyone, because the ready FIFO was left in place.

## D1. Combined credit

**[LOGIC]** Yes. That stream is stranded, and `checkSlowConsumers` does not deliver the bytes. It resets the stream and drops the slab (`connection.zig` `checkSlowConsumers`).

The quantum is sized from `stream_win` before the build. `makeDataFrame` then debits both windows (`session.zig` lines 544–546). If that debit takes the last connection credit and leaves `StreamWindow.send > 0`, the post-quantum `stream_win` call is `availableSend`:

```106:110:src/core/flow.zig
    pub fn availableSend(self: *const StreamWindow, conn_send: i32) i32 {
        // Negative peer windows are legal after SETTINGS_INITIAL_WINDOW_SIZE
        // reductions; treat as zero until WINDOW_UPDATE restores credit.
        if (self.send <= 0 or conn_send <= 0) return 0;
        return @min(self.send, conn_send);
    }
```

That returns 0, so the design parks the stream on the blocked list. A later connection `WINDOW_UPDATE` does not rearm that list. A stream-level update rearms only the id it names. This is the ordinary multi-stream case: the shared connection window hits zero while stream windows are still open, and a peer is allowed to restore only the connection window. The stream then sits until the slow-consumer timeout kills it.

**[LOGIC]** Sol has the same hole. Steps 5–6 of Sol's `emitOneData` park the stream when "only stream credit is exhausted" after the build, and a connection `WINDOW_UPDATE` wakes only the ready ring. Sol never adds a query that can see stream credit alone, so those steps run on `stream_win`, which F1 says is the combined value. Fable does not have it: after a successful build, `len > 0` goes back to the ready tail, and the connection gate at `fair_scheduler.zig:589` returns before any pop while `conn_send <= 0`. Astra does not have it, because classification uses stream-local credit.

**[LOGIC]** Astra's stream-local query is a correct fix and a more expensive one than this needs. After a successful build, put a remainder on the ready tail and do not read `stream_win` again in that call. The next `emitOneData` still begins with the connection gate, so a stream that lost only connection credit stays on the ready FIFO and is served once that gate opens. A pop that already observed `conn_win > 0` and then sees `stream_win <= 0` is the stream window or an RST tombstone, and that is the pop that may move the slot to blocked. No new Session accessor. The stream-local query is the right tool only if a design insists on parking in the same call as the debit.

## D2. Waking blocked streams

**[LOGIC]** Fable's `send_credit_gen`. On this shape the client credits many of the N streams in every read, and those N streams are the blocked set. Waking them is one walk of that set per drain. One counter bumped at the credit sites, compared once in `drain`, does that walk. It matches the work: each of those streams has to move to the ready tail.

Grok's single id becomes a broadcast on the second distinct id, so on this shape every read takes the full walk and the fast path never runs. Astra's journal also does Θ(N) work per read, as N appends plus N lookups, and duplicate updates in one ingest overflow it into `all_changed`, which is Fable's walk after the journal was already filled. The exact list is the better structure when one stream of a large blocked set is credited. That is not this shape.

Keep Fable's bump off the connection `WINDOW_UPDATE` once D1's remainder stays on the ready FIFO. A connection credit then has nothing blocked to repair. Bump on a successful stream `applySendUpdate` and on a positive `SETTINGS_INITIAL_WINDOW_SIZE`. The settings bump is the real broadcast: it retunes every open stream.

## D3. Builder callback

**[LOGIC]** A generation is not needed. A different stream in that slot would fail `pw.stream_id == sid` (`fair_scheduler.zig:643`) and would not be shrunk. The production builder also cannot free the slot, so it cannot reuse it.

`drainEmit` holds `session_mu` for the whole drain (`connection.zig:4280`). The builder it passes is `buildData`, which calls `Session.makeDataFrame` and returns (`connection.zig:4410–4418`). The same is true of `drainEmitReceipt`. Inside `makeDataFrame`, the calls back into `Connection` are `rentFrame` (frame-pool pop), `onRelease` (the accounting counter at `connection.zig:1474–1478`), `releaseRequestBytes` (another counter), and `releaseHeaderList` (arena reset). None of them call `removePending` or `enqueueDataBytes`. Another task cannot slip in: the scheduler is only touched under the mutex the actor is holding. `takePending` does free a slot and hand back a pointer into it (F2), and nothing calls it.

Same-id reuse in that window would fool the id check, and a generation would catch it. Nothing on this stack does that either. HTTP/2 does not recycle the id onto a new stream.

## D4. First-byte wait

**[LOGIC]** Tail insertion is acceptable for the workload in the module comment.

```9:13:src/edge/fair_scheduler.zig
//! which stream's bytes go next. A naive queue gives whoever wrote first the
//! whole connection, which is what makes one large download stall an entire
//! browser tab's worth of requests. That is precisely the Datastar shape this
//! stack serves: long-lived SSE streams beside one-shot page loads on the same
//! connection.
```

That shape is one long-lived SSE stream beside short responses. A new one-shot joins behind that one ready fat stream and waits one quantum, 16 KiB, then its own first frame goes out before the SSE gets another. That is the cap the quantum exists to enforce. The `(n - 1) × 16 KiB` wait is n streams simultaneously holding a full quantum. For the one-SSE connection it is one quantum, the same bound id-order already imposes on a stream the cursor has passed.

**[PREMISE]** The wait becomes a problem if many fat streams are ready at once. That premise is a different workload from the one the comment names. A rule that serves the new stream sooner spends the same round on whichever stream was already queued.

## What changed

The post-quantum park was wrong. A remainder goes to the ready tail, and `stream_win` is not sampled after the debit. The wake is Fable's counter, not the one-id-plus-broadcast flag: two stream updates in one read, which is this client's normal read, made that flag a broadcast every time. Tail insertion stays; for the SSE-beside-one-shots shape I now treat the first-byte wait as one quantum, which is the bound the scheduler already chose.

The strongest point I missed is Astra's: `streamSendAvailable` folds the connection window into the per-stream result, so a blocked-list transition based on that result after a debit strands a stream that still has its own credit. My round-1 wake path never repaired it.
