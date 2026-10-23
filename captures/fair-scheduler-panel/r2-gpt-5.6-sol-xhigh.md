[LOGIC] I proposed an exact, nonallocating Session-to-Connection credit notification that moves the affected stream from the blocked set to the ready tail, with an all-stream reclassification for SETTINGS changes.

## D1. Combined credit

[LOGIC] Grok’s post-quantum rule strands the stream. After the quantum consumes the last connection credit, `stream_win` returns zero even if the stream still has local credit. Moving it to the stream-blocked list is wrong because a later connection WINDOW_UPDATE does not rearm that list. Absent another stream WINDOW_UPDATE, SETTINGS change, reset, or removal, `checkSlowConsumers` is the next guaranteed recovery and will eventually kill it.

[LOGIC] Fable’s design does not have that defect: a successful quantum with remaining bytes returns the stream to ready, and only a later selection with positive connection credit can classify it as stream-blocked. My round-one design also explicitly kept entries ready when the connection gate was closed. Astra avoids the ambiguity by querying stream-local credit.

[LOGIC] Astra’s new query is valid but unnecessary. The cheaper rule is:

1. After any successful quantum with remaining bytes, return the stream to the ready tail without post-debit classification.
2. On selection, check `conn_win` first.
3. Only when `conn_win > 0` may combined `stream_win <= 0` mean stream-local blockage or an RST tombstone; move it to blocked then.

Each exhausted stream causes at most one stale-ready probe, not repeated blocked scans. The same rule handles credit notifications: move the notified blocked stream back to ready without querying it. If its local window remains nonpositive, it returns to blocked once the global gate is open.

A `FlowBlocked` result should similarly inspect connection credit first. If connection credit is zero, preserve ready order and stop; if it is positive and combined stream credit is zero, block that stream.

## D2. Waking blocked streams

[LOGIC] I pick Astra’s fixed Session-owned journal of changed IDs plus `all_changed`.

For a read containing WINDOW_UPDATEs for `K` of `N` large downloads:

- Fable’s generation performs an O(N) blocked walk even when `K = 1`.
- Grok’s scheme is exact for one ID but broadcasts on the second distinct ID, making two updates potentially recheck all N blocked streams.
- Astra performs O(K) indexed activations and becomes O(N) only when the operation genuinely affects all streams or the journal overflows.

When `K ≈ N`, Astra and broadcast are both O(N); Astra pays extra fixed storage and journal writes. It remains preferable because it avoids querying unrelated blocked streams when read coalescing produces only a subset of the N updates. That matters especially for missing Session entries whose availability check can reach the tombstone list.

The journal rules should be:

- Successful stream WINDOW_UPDATE: append that stream ID.
- Connection WINDOW_UPDATE: no journal entry; it only opens the global gate.
- INITIAL_WINDOW_SIZE change: set `all_changed`.
- Overflow: discard individual entries and set `all_changed`.
- Consume before both `drainEmit` entry points.
- Reclassification is idempotent, so repeated IDs cannot duplicate ready membership.

An individual journal entry moves that blocked stream to the appropriate unserved ready segment without first querying combined credit. `all_changed` walks the blocked/active set once. A SETTINGS decrease can leave some ready entries stale; each is demoted once when selected.

## D3. Builder callback and slot generation

[LOGIC] No production code path at this commit can free and reuse a scheduler slot inside one builder call.

Both builders do only this:

```4410:4418:src/edge/connection.zig
fn buildData(ctx: *anyopaque, stream_id: u31, bytes: []const u8, end: bool) anyerror![]u8 {
    const c: *Connection = @ptrCast(@alignCast(ctx));
    const data_payload = c.session.makeDataFrame(stream_id, bytes, end) catch |err| {
        if (err == error.FlowBlocked) return error.FlowBlocked;
        return err;
    };
    c.pending_data_outbound_release = bytes.len;
    c.pending_data_wake_sid = stream_id;
    return data_payload;
}
```

`makeDataFrame` can close and remove the Session stream after END_STREAM, but its `onRelease` hook only releases server-wide accounting. It does not call `cancelHandler`, `removePending`, or `enqueueDataBytes`. Frame allocation only rents from `frame_pool`.

Cancellation is materialized before draining:

```4823:4828:src/edge/connection.zig
fn processIntents(self: *Connection) anyerror!void {
    self.assertSessionHeld("processIntents");
    try self.materializeIntents();
    try self.drainEmit();
}
```

Inbound processing and handlers cannot intervene because the actor holds `session_mu`. Thus the existing “nested cancellation” comment describes a broader callback possibility than the actual production call graph.

[LOGIC] Even a hypothetical remove-A/enqueue-B callback would be caught by `pw.stream_id == sid`, because B has a different ID. A generation is needed only for same-ID ABA. HTTP/2 stream IDs are never reused, and no in-tree builder removes and re-enqueues the same ID.

I would therefore omit the generation, detach the selected slot from its ring before calling the builder, and after the callback re-look it up by stream ID rather than trusting a saved pointer or links. If arbitrary reentrant external builders remained supported, generation would be prudent, but P1 permits tightening that nonexistent compatibility surface.

[LOGIC] F2 should be corrected independently: `takePending` should return metadata only, or be deleted. Returning a slab pointer after returning that slab to the shared pool is an invalid ownership contract even though there are currently no callers.

## D4. First-byte wait

[PREMISE: many SSE streams remain continuously ready while a one-shot page response becomes ready.] A simple tail insertion is not acceptable for this stack. At the default 256-stream limit, the page can sit behind almost 4 MiB of DATA. On a constrained client link that is a visible TTFB regression, not merely an incidental order change.

[LOGIC] Use three FIFO segments:

1. `newcomers`: streams that have never emitted DATA.
2. `unserved`: established ready streams that have not received a quantum in the current round.
3. `served`: streams already served this round and awaiting the next round.

Selection drains `newcomers`, then `unserved`. A successful stream with remaining data joins `served`. Only when both first-turn queues are empty does `served` become the next round’s `unserved`.

A newly created stream goes to the tail of `newcomers`. Therefore its first frame waits only behind earlier newcomers, not behind every established SSE stream. Once served, its remaining body receives no special treatment.

This preserves “no second quantum before every ready stream had one”: `served` cannot run while either first-turn queue contains work. Per-slot `ever_emitted` and `last_served_round` must survive an empty slab; otherwise every SSE event would masquerade as a new stream and regain the boost.

[PREMISE: sustained stream-opening churn.] This rule makes established transfers worse. A continuous supply of newcomers can delay the established `unserved` queue and prevent the next round from beginning. Newcomer FIFO avoids LIFO starvation among new streams, but cannot eliminate that tradeoff without weakening first-frame preference or implementing real priority.

## Changes from round one

[LOGIC] I changed my mind on tail insertion. In round one I accepted the full-pass first-byte delay as the cost of an arrival-order ring. The SSE-plus-page workload makes that cost too important; the newcomer/round segmentation preserves fairness while protecting the first DATA frame.

[LOGIC] I also replaced my unspecified exact callback transport with Astra’s bounded journal. The underlying requirement did not change, but the journal gives it a concrete no-allocation batching and overflow contract.

[LOGIC] I did not change my view on combined credit: connection exhaustion must never dismantle per-stream readiness. I also do not now find a production justification for per-slot generations.

## Strongest missed point

[PREMISE: established SSE streams are carrying full quanta.] Fable’s first-byte-latency observation was the strongest point I missed. It connects the ring’s insertion policy directly to the workload named by the scheduler itself and materially changes the design rather than merely adding a defensive invariant.
