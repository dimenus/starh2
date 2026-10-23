In round 1, I proposed a Session-owned journal of changed stream IDs, with an overflow-to-broadcast flag, to reactivate blocked streams using stream-local credit while treating connection credit as a separate global gate.


In round 1, I proposed a Session-owned journal of changed stream IDs, with an overflow-to-broadcast flag, to reactivate blocked streams using stream-local credit while treating connection credit as a separate global gate.

## D1. Combined credit

**[LOGIC] Yes: Grok’s post-quantum classification can strand the stream.** For example:

1. A stream has 64 KiB of stream credit, 2 KiB pending, and 1 KiB of connection credit.
2. Its first quantum consumes that 1 KiB of connection credit.
3. `stream_win` now returns zero despite substantial remaining stream credit, so Grok’s design moves the remaining DATA to blocked.
4. A connection-only WINDOW_UPDATE restores credit but generates no rearm notification in that design.

The entry remains blocked until some other qualifying rearm or teardown event. `checkSlowConsumers` does **not** resume it: it resets the stream and removes its pending DATA (`connection.zig:2396–2442`). With that timeout disabled, the stall can persist indefinitely.

**[LOGIC] The other proposals do not necessarily share this defect.**

- **Fable:** no. It requeues successful, unfinished streams and broadcasts reactivation on connection-credit increases.
- **Sol:** its stated rule—block only when *stream* credit is exhausted—excludes the defect, but implementing that rule requires checking connection credit again after the debit. The combined callback alone cannot establish it.
- **Mine:** the separate stream-local query avoids it.

**[LOGIC] My query split works, but it is not necessary.** A cheaper implementation retains the existing callbacks:

- Requeue unfinished work at the ready tail after successful emission.
- Before inspecting the next candidate, check connection credit.
- Only interpret `stream_win <= 0` as stream blockage while connection credit is positive.

The actor holds `session_mu`; between those pre-build queries, credit cannot independently change. The important distinction is that **the builder subsequently debits it**. Grok’s initial connection check therefore cannot justify its post-build classification.

This makes the ring a queue of **candidates**, rather than an always-exact eligibility index. A stream that exhausts its own credit incurs one unsuccessful later visit, then leaves the ring. It is not scanned on every subsequent emission.

**[LOGIC] The same distinction applies during reactivation.** A stream WINDOW_UPDATE received while connection credit is zero must not be checked through the combined callback, rejected, and forgotten. Either retain the notification or enqueue the stream as a candidate.

## D2. Waking blocked streams

**[PREMISE: the specified workload batches updates for many concurrent downloads in each read.] I choose Fable’s coarse generation counter.** This changes my round-1 recommendation.

Let `K` be the number of updated stream IDs and `B` the blocked population.

| Proposal | Work after a batch | Consequence on the specified shape |
|---|---|---|
| Fable: credit generation | Reconsider up to `B` blocked entries once | Straightforward batching without an ID journal |
| Grok: one ID, then broadcast | Targeted for one ID; up to `B` for two or more | Broadcast becomes the ordinary case, not a rare fallback |
| Astra: bounded ID journal | Up to `K` indexed notifications; bulk refresh on overflow | Preserves targeting, but adds storage and bookkeeping |

**[LOGIC] When both `K` and `B` scale with `N`, all three perform O(N) readiness work per batch.** My journal does not remove that work. If most blocked streams received credit, most must become candidates anyway. Grok’s claim that broadcasting is rare does not hold for the supplied workload.

**[PREMISE: updates are broadly distributed, rather than concentrated on a few streams among many permanently blocked ones.] The generation counter is the simplest adequate choice.** This is a complexity and implementation judgment, not a measured claim that it will execute faster than a journal.

**[LOGIC] I would specify its operation precisely:**

- Increment the generation after successful stream or connection WINDOW_UPDATE, and after a positive INITIAL_WINDOW_SIZE adjustment.
- Before pending-DATA selection, compare it with the scheduler’s last consumed generation.
- If connection credit is still zero, leave the generation unconsumed.
- Otherwise, append the blocked population to the candidate queue once and record the consumed generation.
- Preserve the order of candidates already waiting. Do not reset the queue head.
- Do not increment the generation for DATA debits or merely because another drain occurs.
- SETTINGS reductions can invalidate candidates; the guarded selection query removes those now blocked.

Both drain entry points and the single-emission test entry must use this reconciliation.

**[LOGIC] The honest bound is amortized, not strict O(1) for every call.** The first selection after a credit event can reject O(B) candidates. Between credit events, rejected streams stay excluded. This removes repeated blocked-stream examination on ordinary emission and transport-completion wakes.

**[PREMISE: `K` is small while `B` is large.] My journal is better for that different shape.** The coarse counter unnecessarily revisits unrelated blocked streams, including on connection-only updates. That is its explicit disadvantage; I would not conceal it behind the high-fan-out example.

## D3. Builder callback and slot generations

**[LOGIC] Reuse for a different stream does not defeat the existing ID check.** If slot A changes from stream 1 to stream 3, then:

> `if (pw.stream_id == sid) {`  
> — `src/edge/fair_scheduler.zig:640`

rejects it. A generation would distinguish **same-ID removal and recreation**, not the different-ID example in the question.

**[LOGIC] I found no current production builder path that frees and reallocates a scheduler pending slot inside one build call.** The trace is:

- Both Connection builders call `Session.makeDataFrame` (`connection.zig:4410–4418`, `4611–4622`).
- `makeDataFrame` allocates/encodes a frame, debits windows, and possibly calls `releaseConcurrency` (`session.zig:515–552`).
- `releaseConcurrency` releases Session concurrency, request storage, and the Session map entry—not FairScheduler pending state (`session.zig:685–700`).
- Its production `onRelease` hook calls `GlobalAccounting.releaseStream`, an atomic decrement (`connection.zig:1474–1479`, `811–814`).
- Frame rental and request/header cleanup do not enqueue scheduler DATA or dispatch another handler.

The second builder explicitly says:

> `// Frame DATA without enqueueing into Session intents (no nested drain).`  
> — `src/edge/connection.zig:4613`

The scheduler comment describing nested cancellation is therefore not evidence of a currently reachable production reallocation path. Introducing a free list changes allocation selection; it does not introduce reentrancy.

**[LOGIC] I withdraw the per-slot generation requirement.** Retain the cheap ID check, and state the builder’s non-reentrant scheduling contract. A hypothetical future callback that removes and recreates the same pending ID would require revisiting that contract; it does not justify extra state today.

**[LOGIC] F2 remains a separate ownership defect.** `takePending` returns a slab-bearing copy after returning that slab to the pool. A slot generation would not make the returned bytes valid. Since the API has no callers, deleting it is simpler than preserving that misleading contract.

## D4. First-byte wait

**[LOGIC] I would retain tail insertion as the default.** It provides a useful bound: every candidate already ahead of a newcomer can consume at most one quantum before that newcomer, and later arrivals cannot continually overtake it. The same protection applies to established SSE streams.

**[PREMISE: many streams simultaneously have full quanta and positive credit.] Grok correctly identifies a substantial regression relative to favorable cursor placement.** At 256 eligible streams, the payload ahead of a newcomer can approach 4 MiB. The one-byte benchmark conceals that cost. This is a bound on competing pending DATA, not wall-clock latency: controls, framed DATA, transport pauses, and credit availability also matter.

**[LOGIC] Long-lived SSE streams are not necessarily continuously ready streams.** Idle SSE streams contribute nothing to that bound. Tail insertion waits behind one quantum of actual ready work, not behind every open stream or every response’s entire body.

**[PREMISE: the application needs first-byte latency below one active-pass serialization time.] Tail insertion would not meet that requirement.** No such latency threshold is supplied, so I would not introduce preferential first service on that assumption.

**[LOGIC] The tempting alternative has a real fairness cost.** One can maintain a current-round unserved queue and a next-round served queue, inserting newcomers at the current-round tail, before second turns. That preserves “no second turn before current-round participants receive one,” but admits newcomers into an extensible round.

**[PREMISE: short new streams keep arriving and completing while established SSE streams remain backlogged.] That extensible round can postpone the established streams’ next turn indefinitely.** Bounding admission to the current round restores a wait for later newcomers. I prefer the ordinary tail rule’s bounded overtaking to that tradeoff.

## Changes of mind and strongest missed point

**[LOGIC] I changed two recommendations:** use the guarded combined-credit query rather than requiring a new stream-local API, and omit slot generations because the alleged same-build reuse path is absent.

**[PREMISE: the supplied batched, many-download update pattern.] I also replace my ID journal with Fable’s generation counter.** Exact notification transport adds little when broad reactivation is routine.

**[LOGIC] Fable’s strongest point that I missed was coalescing credit changes at the drain boundary with a single generation.** I treated exact readiness maintenance as necessary; it is sufficient to keep known-blocked streams out between credit events and reconsider them once per changed generation.In round 1, I proposed a Session-owned journal of changed stream IDs, with an overflow-to-broadcast flag, to reactivate blocked streams using stream-local credit while treating connection credit as a separate global gate.

## D1. Combined credit

**[LOGIC] Grok’s post-quantum classification can strand the stream.** For example:

1. Connection credit is 16 KiB; stream credit and pending DATA are both 48 KiB.
2. One quantum leaves connection credit at zero and stream credit at 32 KiB.
3. Grok’s post-build `stream_win <= 0` check moves the remaining DATA to blocked.
4. A connection-only WINDOW_UPDATE restores credit but sets neither of Grok’s notification fields.
5. The stream remains blocked despite having both kinds of credit.

The initial connection-window check does not protect the **post-debit** check. `Session.makeDataFrame` explicitly performs:

> `self.windows.conn_send -= @intCast(take);`  
> — `src/core/session.zig:546`

**[LOGIC] Slow-consumer handling is termination, not recovery.** Unless another event rearms the stream, `checkSlowConsumers` eventually resets it and removes its pending DATA (`connection.zig:2430–2437`). With that timeout disabled, the scheduler-induced stall can persist indefinitely.

**[LOGIC] The other proposals do not necessarily share this defect.** Fable requeues remaining DATA and broadcasts on connection-credit increases. My stream-local query distinguishes the two causes. Sol explicitly says to block only when **stream** credit is exhausted and preserve ready ordering when connection credit is zero; that is correct, but implementation must enforce that distinction after building and during enqueue classification—not merely at entry to `emitOneData`.

**[LOGIC] My separate query is sufficient, but not necessary. A cheaper fix is lazy classification:**

- After successful service, append remaining work to the ready tail without a post-build window query.
- Before selecting a candidate, check connection credit. If zero, stop DATA selection without changing membership.
- Only while connection credit is positive may combined `stream_win <= 0` move a candidate to blocked.

Under P2, the window cannot change between those selection checks through another task. New work arriving while connection credit is zero should likewise remain a candidate, not be classified as stream-blocked.

This relaxes “ready” to “candidate awaiting validation”: an exhausted stream can incur one failed probe before leaving the ring, but is not repeatedly scanned on subsequent emissions. It avoids both the new query API and the erroneous post-debit classification.

## D2. Waking blocked streams

**[PREMISE — the specified workload batches updates for many concurrent downloads] I choose Fable’s coarse `send_credit_gen`.** The journal offers precision, but that precision has limited benefit when many streams change together on every read.

**[LOGIC] The relevant comparison is per batch, not per WINDOW_UPDATE.** Let `K` be the number of recorded updates and `B` the blocked population:

| Proposal | Work after a batched read | Main drawback |
|---|---|---|
| Fable’s counter | One blocked-set reconsideration, O(B) | Rechecks streams that received no credit |
| Grok’s single ID plus broadcast | O(B) as soon as two distinct IDs appear | Its targeted fast path disappears on the specified workload |
| My bounded journal | O(K) indexed checks; bulk fallback on overflow | Additional storage and recording, including updates to already-ready streams |

**[PREMISE — K and B both scale with N]** All three do O(N) work on this shape. The journal has no asymptotic advantage. If connection credit is the limiting factor and most streams remain candidates rather than individually blocked, `B` can instead be small: the counter avoids looking up every updated, already-ready stream.

**[LOGIC] Implement the counter with these boundaries:**

- Bump it after successful stream or connection credit increases, and positive initial-window changes.
- Reconcile once before DATA selection in either drain entry point—not once per update or emitted frame.
- While connection credit is zero, **do not consume the unseen generation while leaving blocked entries untouched**. Defer reconciliation.
- Once connection credit is positive, walk blocked entries once, append those now eligible to the ready tail, and record the observed generation.
- Repeated drains without a new generation do not revisit blocked entries.

Ordinary and terminal controls must still drain when DATA is connection-blocked. SETTINGS reductions separately require ready-state invalidation or lazy candidate validation; an increase-only counter does not handle reductions by itself.

**[PREMISE — a large blocked population receives sparse updates]** This choice is worse than the journal: one credited stream can cause O(B) reconsideration. That is a real remaining scan, not “scan-free scheduling.” I accept it as credit-event maintenance for the specified batched-download shape; I would not describe it as constant-time reactivation.

**[LOGIC] Grok’s claim that broadcast is rare does not hold on the shape in the question.** Two distinct updated IDs are enough to trigger it on every read. Its additional notification state buys little there.

## D3. Guarding the builder callback

**[LOGIC] Reuse for a different stream does not defeat the existing ID check.** If a slot changes from stream A to stream B, `pw.stream_id == sid` becomes false. A generation would protect against **same-ID ABA**—remove A, then recreate A in the same slot—not ordinary different-ID reuse.

**[LOGIC] I found no current production build path that frees and reallocates a scheduler pending slot inside the callback.** Both builders call `Session.makeDataFrame`, then set the deferred accounting fields (`connection.zig:4410–4418` and `4611–4622`). The second explicitly says:

> `// Frame DATA without enqueueing into Session intents (no nested drain).`  
> — `src/edge/connection.zig:4613`

Tracing the implementation:

- Frame allocation reaches `Connection.rentFrame` and the frame slab pool; it does not enqueue pending DATA.
- END_STREAM can call `Session.releaseConcurrency`.
- Its production `onRelease` hook only releases global stream accounting.
- Request cleanup releases request storage/accounting and header-list leases.
- None calls `enqueuePending`, `enqueueDataBytes`, `materializeIntents`, or handler dispatch.

Thus the scheduler comment describing nested cancellation is not evidence of a currently reachable production reuse path. Adding a free list does not introduce reentrancy.

**[LOGIC] I withdraw the per-slot generation recommendation.** Keep the existing ID recheck, and do not carry stale list links across the callback, but a generation has no demonstrated job here.

**[LOGIC] F2 is a separate ownership problem, not evidence of builder-time ABA.** A generation would not make `takePending`’s returned slab pointer valid after its lease was returned. Since there are no callers, removing that unused API is simpler than preserving its misleading snapshot contract.

## D4. First-byte wait

**[LOGIC] I would retain tail insertion as the default.** Its important property is bounded overtaking: once a stream joins the queue, later arrivals cannot continually push ahead of it. Existing streams likewise retain progress despite a stream of new one-shot requests.

**[PREMISE — SSE streams are idle between events]** Open SSE streams do not necessarily contribute to the first-byte wait. Only streams with eligible pending DATA belong in the ring; “256 open streams” is not automatically “255 full quanta ahead.” The code explicitly recognizes that SSE streams are idle between events (`fair_scheduler.zig:654–657`).

**[PREMISE — many streams are simultaneously ready with full quanta]** Grok’s regression is nevertheless real. A newcomer that numerical cursor order would select immediately can instead wait behind `(n−1) × 16 KiB` of payload. At 256 eligible streams, that is almost 4 MiB, before transport and flow-control delays. I accept tail insertion as the starvation-free scheduling default, **not as proof that this delay meets a page-load latency requirement**.

**[LOGIC] “Insert newcomers before already-served streams” is not a free improvement.** A current-round/next-round split could place newcomers after existing unserved streams but before second turns. That preserves the literal first-before-second rule for that round.

**[PREMISE — newcomers continually arrive and finish]** However, continually admitting them into the current round can postpone existing streams’ second turns indefinitely. It makes the long-lived SSE side worse. Bounding those admissions requires an explicit round-admission policy and gives some newcomers the wait back. Without a stated latency requirement justifying that trade, I prefer the simpler tail rule.

## Changes since round 1

**[LOGIC]** I withdraw two unnecessary mechanisms: the mandatory stream-local query, because guarded lazy classification suffices, and per-slot generations, because the production builder has no demonstrated same-ID ABA path.

**[PREMISE — the batched-update workload specified here]** I change my notification choice from the bounded journal to Fable’s counter: less state, no asymptotic loss when many blocked streams change together, and potentially less work when updated streams were already ready.

**[LOGIC]** I retain tail insertion and the existing sink/accounting boundaries. I would remove unused `takePending` rather than introduce a replacement result type.

**[PREMISE — a newcomer would be today’s next cursor hit while existing streams have full quanta] The strongest point another reviewer made that I missed in my final round-1 downside analysis was Grok’s concrete first-byte regression.** It identifies a user-visible loss that the one-byte throughput profile hides, even though it does not persuade me to trade bounded service for newcomer preference.

