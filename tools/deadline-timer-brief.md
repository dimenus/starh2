# Brief: handler deadlines on zio timers, heap deleted (t-883)

Delete the actor's handler-deadline heap and give every handler its own
zio timed wait. The heap is the last hand-built timing subsystem in the
edge, it carries the t-850 ghost, and its founding justification is
measured-retired (below). Everything grades against the t-882-landed
baseline `0e921e6`+.

## Provenance and the retired premise

The heap exists because of t-824's mechanism: zio timers armed on the
sleeper's HOME executor, and an actor that stays hot never polls its
loop, so handler sleeps froze. The fork's `53504b4` (Ryan, 2026-08-18)
arms sleep/select/AutoCancel timers on a dedicated executor thread that
is not a steal victim, and the timeout wake RUNS THE WAITER THERE.

Measured, not read (`tools/deadline-timer-probe`, ReleaseSafe, 5/5 runs):
one executor, migration off, a 500 ms hog that never yields, a 50 ms
`zio.ResetEvent.timedWait` armed under it fires at 51-52 ms. The waiter
resumes DURING the hog. The t-824 starvation is gone at the runtime
level, so the actor no longer needs to be the handlers' clock.

## What the heap provides today, and where each duty goes

| duty | today | becomes |
|---|---|---|
| SSE cadence wait (`waitUntilCb`, connection.zig ~5112) | session lock + heap insert + per-slot Event reset/recheck loop + heap remove | one per-slot zio timed wait; `error.Timeout` IS the deadline |
| early wake on RST/terminal/writer-fail | `wakeStreamDeadline` sets `deadline_ready[i]` + `deadline_events[i]` | same wake, same primitive; a normal (non-timeout) return means "woken early, recheck cause" |
| fire-due on hot actor turns (`fireDueHandlerDeadlines`) | actor walks the heap every turn | DELETED - no actor role in handler timing |
| actor park timer contribution (`nextDeadlineNs` heap min) | heap min competes with idle/slow/grace | DELETED from the park; idle/slow/grace remain |
| `deadline_armed` flag + doorbell ring | flag-before-ring | DELETED |

## Design decisions (settle before code)

- The per-slot wait primitive must be one whose TIMED wait provably arms
  on the dedicated timer executor. `zio.ResetEvent.timedWait` is
  probe-verified. `std.Io.Event.waitTimeout` routes through the futex
  path, which `53504b4` does NOT name — VERIFY it (extend the probe) or
  convert `deadline_events` to `zio.ResetEvent` per slot. Do not assume;
  the probe costs one minute.
- Reuse-per-slot means the event IS reset between waits; the reset-race
  is bounded because both the waiter and the waker hold well-defined
  turns (waker sets ready-flag then event, waiter checks flag after
  reset — the existing discipline). State the ordering in comments; do
  not invent a new protocol.
- The donate-yield after `fired > 0` in the actor loop dies with
  `fireDueHandlerDeadlines`. Check what replaces its scheduling effect
  (a same-home ready handler used to get one slice after a fire); the
  timer-thread wake makes the handler runnable without the actor's help,
  which is the point.
- Complete oneshots must still never register a deadline (t-824 premise,
  unchanged).
- The timer-arm churn at SSE-200/10ms is ~20k arms/sec through the timer
  executor. The SSE grading lines are the check that this scales; if
  they regress beyond noise, STOP and report (the fallback is today's
  heap, which is correct).

## Grading lines (against 0e921e6 controls, same machine, n>=3)

1. Full suite green; `zb build ci` rc=0. Soak: 30 Darwin + 100 nachos
   suite runs, zero accounting crashes (the deinit ledger panic is the
   instrument).
2. Mixed inspect at STARH2_EXECUTORS=2, oneshot workers 8: SSE reaches
   16000 with oneshot within noise of control (the t-824 acceptance).
3. SSE-200 p50 within noise or better on both boxes (nachos control
   13-17us from captures/actor-select-ab.md; re-derive, do not cite).
4. Oneshot w2/w8 within noise (nachos ~117k/~186k bands) — this cut
   should not touch the oneshot path at all; a shift means it did.
5. wedge-probe 3/3 both shapes at the 15000/25000 floors;
   h2load-wedge-rate CLEAN 0/30.
6. T1 wedge soak, PAIRED: 100 nachos runs of this cut vs 100 of
   0e921e6. If the wedge vanishes with the heap, t-850 closes as
   retired-surface; if it persists, t-850's model narrows to zio
   delivery with the heap exonerated. Either result is evidence; record
   it on t-850.
7. Mutation lines:
   - Remove the early-wake (`wakeStreamDeadline` call on RST/terminal);
     the RST-of-SSE tests (deadlines T2-shape) must fail or hang-detect.
   - Force the per-slot timed wait to `.none`; the deadline tests must
     fail.
   - A rewrite that cannot fail these gates is ungraded.
8. RSS within +1 MiB at peak-rss-200 (the heap frees max_streams
   entries; expect flat or smaller).

## Boundary conditions

- Do not touch the actor's select shape (t-882), FairScheduler, or the
  TLS pump. `test_queue_wire_bypass == 0` stays a canary.
- Session idle/slow-consumer/grace deadlines stay actor-owned; this cut
  is HANDLER deadlines only.
- No polling, no sleep-loops, no yield-based cadence (the t-824 rejected
  list stands).
- If zio needs a patch, fork first with Ryan's sign-off, and stop until
  it lands (house rule; it fired twice in t-882 and both were real).
- Stale-constraint note: t-824's brief pinned zio at 489f31f and forbade
  zio changes. Both are superseded — the pin is 3025548 (which contains
  53504b4 and the select conservation fixes), and the fork process
  exists. Do not re-import those constraints from the old brief.
