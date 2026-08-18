# Actor-owned handler deadlines

A fresh session implements this. Do not satisfy it by yielding the actor,
sleeping 1 µs, retargeting zio, bumping executors, or leaving `/sse` on
`zio.sleep`.

## Premises (settled — state these if you delegate further)

- Malleable stack, no external consumers. Breaking change is allowed; name it.
- Share-nothing. `session_mu` covers Session + FairScheduler + TLS. A new mutex
  is the signal that a task should have owned the state.
- Do not fight `std.Io` / zio. **Do not start by changing zio.** Lukáš closed
  [lalinsky/zio#686](https://github.com/lalinsky/zio/pull/686) (dedicated timer
  executor). [lalinsky/zio#687](https://github.com/lalinsky/zio/pull/687) makes
  `Io.sleep(.zero)` a `yield()`, not a timer; that is a compatibility patch for
  the wrong factorization, not this cut. `maybeYield` is budget-gated and is
  not “yield now.”
- Production wire through FairScheduler sink. `test_queue_wire_bypass == 0`.
- Complete oneshots stay inline (receipt pipeline on `2612a13`). They must not
  register a handler deadline and must not pay a yield.
- Only the actor drives the TLS cipher.
- `zig build test` does not reach TLS. `tls-smoke` is the TLS gate.
  `./zb build ci` is definition of done.

Rejected without new evidence (do not retry):

- Dedicated zio timer executor / `yieldPark` / overflow-drain / re-home rules /
  `pinSpawnsToCurrent` (fork-only; not on upstream).
- `spawnBlocking` for `Connection.run` or `ReadPump` (they are the Io path).
- Actor `Io.sleep` of 1 µs or 50 µs to reprint 16k SSE (Darwin coalesces;
  oneshot p50 blows out).
- `zio.maybeYield` in `Connection.run` as the cadence fix.
- `STARH2_EXECUTORS` ≠ 2 on the mixed inspect, or oneshot workers ≠ 8.
- Softening mixed oneshot so SSE can print 16k (12k oneshot is a fail).
- Changing `/sse` interval, stream count, or client warmup to manufacture 16k.
- Retargeting `build.zig.zon` to lalinsky/zio#687 as the solution (I4
  `large SSE event over outbound_bytes_per_stream` fails on that pin even
  with no starh2 yield; stay on the committed dimenus `489f31f` pin).

Working tree landmine: uncommitted `build.zig.zon` / `tools/lock.json` may
already point at `b09c6ea` (#687), and `connection.zig` may already have
`yieldToTaskHandlers` (`Io.sleep(.zero)`). **Revert those** before you start
if they are present. This cut does not need them. Base is `2612a13`.

```sh
git rev-list --count HEAD..master   # must be 0; STOP if not
```

## The defect

Pipeline (`2612a13`) stopped the actor `waitTicket`ing complete oneshots.
Oneshot-only moved next to Go (~33–37k rps, p50 ~210 µs, 8 workers, 1 TLS
conn, `STARH2_EXECUTORS=2`). Mixed SSE then missed the 16 000-event target
(~8–12.8k, 32/32 still delivering). Go mixed stays 16 000 at the same oneshot
rate.

Mechanism: zio is cooperative. `zio.sleep` / `Io.sleep(positive)` arms a
timer on the sleeper’s **home** executor. `Connection.run` does not return to
`Executor.run` while oneshot ingest stays ready (`tryGet` hits). That home
loop never `poll`s, so handler sleeps do not fire. A sibling ReadPump that
also never parks starves waiters that round-robined onto it. This is
[lalinsky/zio#685](https://github.com/lalinsky/zio/issues/685), closed as
expected cooperative behavior.

`waitForActivity` already `select`s `actor_wake`, shutdown, and
`nextDeadlineNs()` (idle / slow-consumer / grace). That timer **only runs
when the actor parks**. Hot ingest never parks. Handler cadence is not in
that heap; it is `zio.sleep` inside `/sse`.

`/sse-broadcast` still sleeps in `tickerTask`. Do not treat that arm as the
fix. A Datastar handler must be able to wait for a time (or an app event)
and then `writeAll` **its** bytes.

## Design

Keep the connection split (actor does not wait handler writes). Move **time
waits** onto the actor.

1. **Actor-owned deadline heap**, one optional deadline per live task-handler
   slot, capacity `limits.max_streams_per_connection`. Actor is the only
   mutator. No new mutex.
2. **Public API** on `Response` (and a one-line `Body` wrapper):
   `waitUntil(deadline: std.Io.Timestamp) ResponseError!void`
   (`Canceled` / `PeerReset` / `ConnectionClosed` / `SlowConsumer` via the
   existing terminal cause). Sleep-to-a-deadline, never sleep-for-a-duration.
   Implementation: callback into the actor (same shape as `writeFn`), insert
   `{stream_id, deadline}` on the heap, flag-before-wake `actor_wake` so a
   parked actor re-`select`s the new `nextDeadlineNs`, then the handler parks
   on a **per-slot Event that is not `space_events`**. Occupancy and time are
   different waits; do not overload `space_events`.
3. **`nextDeadlineNs` includes the heap min.** Idle `waitForActivity` already
   knows how to wait a deadline; extend it. Do not add a second timer select.
4. **Every actor turn, before `waitForActivity`:** `now = nowNs`; pop/fire
   every heap entry with `deadline <= now`; `Event.set` those slots
   (flag-before-wake on the slot, then set). Then if `read_ch` is ready,
   ingest **one** chunk as today. The actor already is one-chunk-per-turn;
   the missing check is the heap fire on that turn, not a new quantum.
5. **After firing at least one deadline**, the actor must donate so a
   same-home ready handler can `writeAll` before the next oneshot chunk is
   ingested. That donation is `Io.sleep(.zero, .awake)` **once per fire**,
   not once per loop while handlers exist. The woken handler is on the ring
   (`Event.set`); `yield()`’s empty-queue fast path does not apply. A
   sleeping `zio.sleep` waiter is not on the ring — that is why yield-every-
   turn lost to ingest.
6. **`examples/bench_server.zig` `/sse`** uses `waitUntil` / `Body.waitUntil`.
   It must not call `zio.sleep` or `io.sleep` with a positive duration. Keep
   sleep-to-a-deadline (Go ticker schedule). Cadence counters stay.
7. **Cancel / `releaseSlot` / RST / shutdown** remove the heap entry and set
   the slot Event so `waitUntil` returns a terminal error. Leak of a heap
   slot is a fail. Complete oneshots never insert.
8. Handler-authored payload. The actor wakes; it does not synthesize SSE
   DATA. A test (below) writes a token only the handler knows.

Two-executor mixed: handlers still round-robin. If a waiter homes on the
ReadPump worker, that pump must not hog for the whole oneshot flood. Allowed:
`ReadPump` `Io.sleep(.zero, .awake)` **only while** a connection-owned
`live_task_handlers > 0` (you may add that counter; complete oneshots must
not increment it). Forbidden: yield on every oneshot-only read; adding zio
scheduler APIs; pinning via a fork symbol.

## Mechanical acceptance (all axes; any failure fails the build)

The laziest passing outputs, and the check that kills each:

| Laziest pass | Kill it with |
|---|---|
| Leave `/sse` on `zio.sleep`; add actor `sleep(.zero)` every turn | Required 1-exec flood test (below) fails on a `waitUntil` that is `Io.sleep`. Mixed oneshot-only must stay hot. Source: `/sse` must not contain `zio.sleep` / positive `io.sleep`. |
| `waitUntil` = `io.sleep(deadline - now)` | Same 1-exec flood test. Positive sleep arms a home-loop timer the hog never polls. |
| Heap only consulted in `waitForActivity` (idle) | 1-exec flood never parks; test fails. Fire-due is on **every** turn. |
| Yield every turn while `live_task_handlers > 0` | Oneshot-only p50 / rps must stay in the hot band (below). Fire-only donation. |
| 1 µs / 50 µs actor or handler sleep | Forbidden durations. Also oneshot p50 band. |
| Actor writes the SSE bytes itself | Token test: client body must contain handler-chosen bytes, not an actor stamp. |
| One shared deadline for all streams | Two-stream test: 10 ms and 50 ms; the 10 ms token arrives first; the 50 ms token is absent at T+20 ms. |
| Reuse `space_events` for time | Occupancy I4 + waitUntil-under-cap must both pass. Separate Event. |
| Broadcast-only; `/sse` still sleeps | Mixed inspect uses `/sse`, not `/sse-broadcast`. |
| 16k SSE by dropping oneshot to ~12k | Mixed oneshot must stay in the hot band. |
| 16k by changing STREAMS / INTERVAL / SECONDS / executors | Fixed I/O contract below. |
| Retarget zio / skip I4 | `./zb build test` includes I4; zon stays `489f31f`. |
| Skip-receipt / fold AckDrainer | Pipeline brief still applies; `records/response < 0.4`. |
| `ReadPump` yield on oneshot-only | Oneshot-only hot band; gate the yield on live task handlers. |
| Claim “already covered by waitForActivity” | Cite the fire-due call site on the **hot** path (the loop that `tryGet`s a chunk). Recompute: that site must run when `read_ch` is ready. |

### Required new tests (you write these; not optional)

**T1 — 1 executor, ingest stays ready, `waitUntil` must fire.**
`zio.Runtime.init` `{ .executors = .exact(1), .enable_task_migration = false }`.
h2c is enough. Routes: task `/sse-token` (`startSse`, `waitUntil(now+20ms)`,
`writeAll("data: token-t1\n\n")`, finish) and complete `/`. Client opens
`/sse-token`, then floods `GET /` on the **same connection** for 100 ms
(or until the token arrives). Assert: the token is in a DATA payload within
100 ms; at least one oneshot completed in that window (ingest did not park).
Timeout fail, not hang.

A `waitUntil` that calls `Io.sleep` / `zio.sleep` with a positive duration
**must fail T1**. If your T1 passes on a stub that only sleeps, T1 is wrong
— stop and fix the test.

**T2 — two deadlines, order.**
One connection, two task streams, `waitUntil(+10ms)` writes `token-early`,
`waitUntil(+50ms)` writes `token-late`. At T+20 ms: early present, late
absent. Then wait out the late. Cancel/RST of one stream must not strand
the other heap entry.

**T3 — terminal during wait.**
Handler in `waitUntil` far in the future; client RST or server shutdown.
`waitUntil` returns a terminal `ResponseError`; heap entry gone;
`live_handlers == 0`; DebugAllocator clean if lifecycle-style.

HEAD (`2612a13`) cannot pass T1 if T1 is implemented against `zio.sleep`.
The production `/sse` path must use the same `waitUntil` T1 uses.

Existing gates that must stay green: `./zb build test -j1`, `tls-smoke`,
`test-lifecycle`, `test-exact`. Do not weaken h2spec exclusions. I4 large
SSE occupancy must stay green (separate Event from `space_events`).

### Measurement (not a substitute for the tests)

Independent oracle: the Go client in `tools/sse_bench/` (shares no server
code). Do **not** grade on `g_cadence.loops` / `writes` alone — those are
server self-reports. `curl /sse-cadence` is supporting evidence only.

Fixed I/O:

```
STREAMS=32  INTERVAL=10  SECONDS=5  WARMUP=1
ONESHOT_WORKERS=8  STARH2_EXECUTORS=2
url=/sse   oneshot=/   TLS  testdata/cert.pem
```

Quiet machine. **Oneshot-only first.** Abort mixed if oneshot-only rps
< **33000** or p50 > **250 µs** (load-shift regime; mixed then reprints
16k for the wrong reason). Re-derive; do not cite this file’s history.

Inspect bar (all must hold on the same binary, same run after a hot
oneshot-only):

| Axis | Pass |
|---|---|
| Mixed SSE events | **16000** (client count; warmup discarded by the client) |
| Mixed delivering | 32/32 |
| Mixed oneshot | rps ≥ **0.85 ×** that run’s oneshot-only, p50 ≤ **300 µs** |
| Stall | 31/32 delivering |
| SSE-only | 16000 (proves idle `waitForActivity` still ticks the heap) |
| Packing | `tools/oneshot-phase-trace.sh` `records/response < 0.4` (exit 9) |

`tools/sse_bench/mixed.sh` does **not** fail on a short SSE count. You
inspect. A silent drop is a fail.

After a real pass: update `tools/oneshot-gap.md` from **that** run only.
Do not invent numbers. Do not claim 16k from a 12.5k-oneshot run.

## Tripwire (do this first, then stop if it fails)

```sh
git rev-list --count HEAD..master
git log -1 --oneline
```

Expected: **0**, HEAD is `2612a13` or a descendant that still contains the
receipt pipeline and this brief. If master is ahead, STOP and report.

## Commits

Commit per milestone (a killed run resumes from the last commit):

1. Heap + `Response.waitUntil` + fire-due on the hot actor path + T1 red.
2. T1/T2/T3 green. `/sse` switched off `zio.sleep`. Comments: cadence is a
   heap entry, not a handler timer; `waitForActivity` is the idle arm of the
   same heap.
3. Gates + hot mixed inspect + `oneshot-gap.md`. If measurement rejects the
   cut, keep the tests if they fail on a sleep-stub; STOP without claiming
   16k.

Do not regenerate goldens. Do not touch `captures/`, `DIAG_BUILD_SPEC.md`,
`HANDOFF.md`. Do not push.

## Zig

Zig 0.16. `./zb` not a guessed `zig`. Do not guess 0.16 APIs — `zigstd`.
`std.testing.FailingAllocator` is not thread-safe. `Io.sleep(io, duration,
clock)` is `io.sleep(.zero, .awake)` for the fire-only donation.
`std.Io.Duration.zero` is `{ .nanoseconds = 0 }`.
