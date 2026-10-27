# Panel brief: t-3594, a handler-side change must wake the actor, by construction

starh2 is a Zig 0.17 HTTP/2 server on zio (coroutines on a thread pool). Each
connection has one ACTOR task that owns the protocol state and, on TLS, also
writes the socket; handler tasks run the application and take the
connection's `session_mu` to change shared state (queue response frames, wait
for room, fail a stream). The worktree is at master 58b88c9.

## The defect class

The actor parks in `waitForActivity` (`src/edge/connection.zig`). On TLS its
select is the completion queue (`pump.cq`), the handler completion channel
(`completion_ch`), the doorbell (`doorbell`, capacity 1, written only by
`ring()`), the shutdown event and a timer. It is NOT the write stash, and not
any flag a handler sets. So when a handler task changes state the actor must
act on, the actor learns of it only if that code path calls `ring()`.

t-3567 (58b88c9) found three members of this class in three review rounds:

1. A bare flush barrier pushed to the TLS stash from a handler's task: no ring.
2. A task handler's empty-body `send` drains on its own task and pushes
   HEADERS to the stash: no ring. A GET arms no actor timer, so both sides
   wait for ever (reproduced: the idle-204 case in
   `tests/t3567_tls_barriers.zig` fails by deadline without the ring).
3. A failure a handler task publishes: `handleWriterFailed` (reached, for
   example, when materializing a response refuses a CONTINUATION,
   `ctrl_fragment_full`) wakes the handlers and closes the stash, but never
   rings the actor. Today it is rescued only when the handler's task exits
   (its exit posts to `completion_ch`). Not yet fixed.

58b88c9 fixed 1 and 2 with one ring in the TLS branch of `pushWriteChunk`,
skipped when the pushing task is the actor (`actor_marker`, a
`zio.TaskLocal(*Connection)` bound by `runBody`). Other ring sites today:
`enqueuePending` (2), `startCb`, the budget grant to an inline entry
(`grantBudgetLocked`), and one more near line 7190. Each was added where
someone noticed a missing wake.

## The question

How should "a handler-side change wakes the actor" become a property of the
code's shape, so the next new handler path cannot forget it?

## Candidates (evaluate all; pick one; say what each understates)

- **D1. Ring on every non-actor release of `session_mu`.** Every acquisition
  and release goes through `lockSession` / `lockSessionUncancelable` /
  `unlockSession` already (a comment there says so). In `unlockSession`, after
  the unlock, ring when `actor_marker.get() != self`. Remove the per-site
  rings that this covers. Questions: the cost (a `TaskLocal.get`, a short list
  walk, plus a doorbell `trySend` that takes a channel mutex, on every handler
  unlock, including the unlock inside every wait loop: `waitForStreamSpace`,
  `parkForBudget`, `waitForAckRoom`, `waitTicket` paths); spurious actor
  wakes when a handler unlocks only to wait; whether any handler-side change
  happens WITHOUT `session_mu` (atomics set outside the lock); ring before or
  after the unlock; h2c (is the same class present there, and should D1 apply
  to both transports?).
- **D2. A dirty flag.** Handler-side mutations set `actor_work_pending`; the
  release rings only when it is set. Cheaper on unlocks that changed nothing,
  but every mutation must remember to set the flag: the same forgetting
  problem one level down, unless the flag can be set by construction.
- **D3. Make the actor select on what handlers produce.** For example, add
  the stash's receive readiness to the TLS select, so a push wakes the actor
  with no ring. Covers pushes; does not cover a published failure or a flag.
- **D4. Site patches only.** Add the missing ring in `handleWriterFailed`
  (marker-suppressed) and keep the convention. Smallest; the class stays
  guarded by memory.

## Premises (settled)

- Nothing is in production. Breaking internal changes are fine.
- The actor never waits for a handler, and the lock order is `session_mu`,
  then `budget_mu` (option A, a70a088).
- The doorbell coalesces: a full doorbell already means a wake is pending, and
  the actor drains all its inputs on every turn.
- Throughput matters: the nachos A/B shapes run up to about 3M requests/s on
  h2c and 1.36M on TLS; a cost per handler unlock shows in `h2c task4` and
  `tls mixed`. Any choice must be measured before it lands.
- Out of scope: the terminal queue size (t-3562), `wakeAllSpace` fairness
  (t-3593).

## What to return

- A decisive pick, and the evidence from the code: quote what you read
  (`connection.zig` line ranges), especially for "does any handler-side
  change happen outside `session_mu`" and for the wait loops' unlocks.
- For the pick: where exactly the ring goes, what it replaces, what it costs
  per handler operation, and a test that would fail if a future handler path
  forgot to wake the actor (a test that can fail on a path nobody has written
  yet is the point).
- What the other candidates understate.
- Label every claim **[LOGIC]** (holds regardless of deployment facts) or
  **[PREMISE]** (name the fact). Unlabelled claims are discarded.
