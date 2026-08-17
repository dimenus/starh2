# TLS stall worklog

Live debug log for the intermittent h2load TLS stall. Meant to survive
compaction and to become the commit message when a fix lands. Not a design
doc. Do not treat `DIAG_BUILD_SPEC.md` as this tree.

Harness: `STARH2_DIAG=1 tools/tls-stall-delta.sh NAME --task-migration --rounds N`
Build: `./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix <dir>`

## Isolated cause (measured, not a hypothesis)

At the 30.15s signature the actor parks with scheduler work that can never emit:

```
pending=10 bytes=130 ordinary=0 framed=0 live=0 refilled=false
conn_win≈1073738742
eligible=0 missing=10 tomb_noerr=0 tomb_rst=10 pend_slots=0 rst0=2 intents=0
acc=0 complete=false
```

- 10×13-byte `"Hello, World!"` still in FairScheduler slabs (`-m 10`).
- Those stream IDs are gone from `Session.streams`.
- Tombstones are RST, code **2 = `internal_error`**, not `.no_error`.
- No handler slots, Session intent queue empty.
- `FairScheduler.emitOneData` sees `streamSendAvailable == 0` for RST
  tombstones and `continue`s forever.
- Drain “exhausts” with pending left → actor parks with `!sched_refilled` →
  only the 30s `slow_consumer_timeout_ns` wakes it.

`acc=0` refutes leftover TLS ciphertext for this signature. Kernel Recv-Q/Send-Q
were already 0. `checkSlowConsumers` uses `.cancel` (8), so it is how the park
*ends*, not who wrote `rst0=2`.

26 failed is not the stranded slab count. In-flight at park is 10. 26 is how
many h2load requests were still on that connection when slow-consumer killed
it. 52/78 look like 2–3 stalled clients. Do not overfit 26.

TLS-only / migration: timing. Encrypt holds `session_mu` longer; migration
raises interleaving. Same Session/scheduler path exists on h2c; it has not
reproduced.

## What is still unproven

Who `emitRst(..., .internal_error)` on those 10 streams, and how DATA is still
in the scheduler after RST cleanup with no slot.

Local `internal_error` RST sites in `connection.zig`:

- `runHandlerJob` catch after committed response
- `compressOrAbort` after headers committed (bench hello path does not compress)

Code holes that match the snapshot:

1. `sendCb`/`writeCb`/`startCb` check `getCause()` **before** `session_mu`, then
   `respond_headers` + `enqueuePending` with no recheck. `cmdHeaders` does not
   require a live stream, so HEADERS+DATA can land after `cancelHandler`.
2. `emitRst` queues RST frame then `stream_reset`. `materializeIntents` on
   `enqueueControl` PoolFull/QueueFull returns immediately. `errdefer` frees the
   `stream_reset` intent **without** `cancelHandler`. Tombstone already exists.
   Handler `processIntents() catch {}` swallows that OOM, so the connection
   stays up. Client never sees RST or DATA → waits until slow-consumer.

Dropping pending without RST on the wire (or a connection close) hangs forever.
That is why `dropResetPending` and a bare `refuseResetStream` were reverted.

## Fixes that failed

| arm | change | result |
|---|---|---|
| fix1 | treat missing `.no_error` as send credit | 4/20 stalls, still `tomb_rst=10` |
| fix2 | `dropResetPending` after drain | infinite hang; no snapshot if pending=0 |
| fix3 | `cancelHandler` no-slot still `wakeHandlerWaiters` | 1/40 still 26-fail 30s |
| fix4 | + `materializeIntents` before `drainEmit` | 4/40, same snapshot |
| fix5 | + dropResetPending + refuseResetStream | infinite hang |
| fix6 | refuseResetStream, no sweep | hang with `pending=1 tomb_rst=1 rst0=2` |

Kept (correct, incomplete): no_error framing, cancelHandler no-slot drop,
materialize before drain, `--diag` park snapshot.

## Do not

- Start by changing zio.
- Touch unrelated dirty files in this worktree.
- Drop scheduler pending unless RST is already queued/on the wire **or** the
  connection fail-closes (`writer_failed`).
- Fold the `not-started` class (`started < total`) into this stall.
- Cite old harness counts as proof; re-derive with the current script.

## Landed in this tree (not yet proven under the harness)

- `cmdHeaders` refuses a missing stream (`error.StreamClosed`).
- After `session_mu`: `refuseIfStreamDead` (cause, writer_failed, RST tombstone,
  missing map entry) before HEADERS/DATA.
- `materializeIntents`: on control/framed enqueue fail, still run remaining
  `stream_reset` intents (`cancelHandler` drops pending), then return OOM.
- That OOM fail-closes (`handleWriterFailed`) in the actor and in handler
  `processIntents` / `sendCb` — no more `catch {}`.
- Park snapshot: `rst_h` / `rst_c` / `ctrl_fail` / `rst_after_fail`, and
  throttled empty parks (the infinite-hang signature).

Harness: **0 stalls in 60 rounds** (12+20+40) of `-n 100000 -c 50 -m 10 -t 4 --task-migration`.
Park log on the 20-round arm: `rst_h` rose to 56 (the `runHandlerJob` internal_error
site), `ctrl_fail` to 59, `rst_after_fail` to 3, **`tomb_rst` stayed 0**. The RST
source fires; pending DATA is no longer stranded on a RST tombstone.

not-started remains (~7/40, ~1686 failed ≈ one of 50 clients). Do not fold it
into this stall. Terminal enqueue fail still fail-closes that connection.

Landed as `57359b7`. `./zb build ci` passed on that commit. `not-started`
remains t-761; do not fold it into this stall.
