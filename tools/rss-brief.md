# Design review: per-connection memory (RSS) under multiplexed load

You are one of four reviewers on a design panel; the others are from other
vendors and you will not see their answers. Your working directory is the
starh2 repository (Zig 0.17, HTTP/2 server on the zio runtime) at the commit
named in your prompt. Read the code. Nothing here is built yet.

## Premises — treat these as settled

A finding that depends on the negation of one of these is out of scope.

- P1. The stack is not in real use and has no external consumers. Breaking
  the handler API (including what `Request.arena` promises), `Limits`
  defaults and `Connection` internals is allowed. Do not weigh backward
  compatibility.
- P2. Each connection is owned by one actor task; `session_mu` covers
  Session and FairScheduler. Do not propose sharing per-connection state
  across connections behind a lock unless it is the point of your design,
  and then say what it costs in contention.
- P3. No allocation on the WRITE path stays: pending DATA lives in slabs
  rented from a pool sized at boot (`src/edge/fair_scheduler.zig` module
  comment, `src/edge/slab_pool.zig`). `Limits.resourceUpperBound`
  (`src/core/limits.zig`) is the stated worst case and `Server.init` refuses
  a configuration that exceeds it.
- P4. zio's stack pool is not the problem: its `committed_size` is a
  reservation, not resident memory (measured: 16 KiB vs 256 KiB committed
  changed RSS by nothing; all coroutine stacks together were ~17-58 MB
  resident at 1024 connections).

## What was measured (nachos, Ryzen 9900X, Linux, 2026-10-23)

Workload: an HttpArena-shaped server built on starh2 (`/json/{count}?m=`
serialized per request with `std.json` into `Request.arena`), 12 executors,
`h2load -p h2c -c 1024 -m 32` rotating seven URIs (1-50 items, responses up
to ~12 KiB). Defaults otherwise (`max_streams_per_connection = 256`).

- RSS: idle 8-10 MB; json load 0.5-1.96 GB across runs; baseline-h2c
  (1-byte bodies, -m 100) 0.6-0.8 GB. ~10-13 resident mappings per
  connection, mostly 256 KiB-1 MiB.
- `perf record -e page-faults -c 1 --call-graph fp` during the load ramp
  (324,743 samples, none lost; RSS 8 MB -> 644 MB). Page faults are first
  touches, so this attributes RSS growth. Inclusive shares:

      39.3%  main.json -> std.json.Stringify -> memcpy   (into Request.arena)
      18.0%  edge.connection.Connection.init             (86% of it at connection.zig:1658)
      20.4%  FairScheduler.drain / emitOneData -> ...
      14.5%  Connection.queueWire / sendAccountedWire
      12.8%  drainEmitReceipt.ReceiptSink.flushBatch
      43.0%  compiler_rt memcpy (self; reached through the paths above)

  (Shares overlap: they are inclusive, so a fault in memcpy under
  queueWire counts in both.)

Code the numbers point at:

- `Request.arena` is the per-handler-slot `HandlerJob.arena`
  (`connection.zig:1057`), reset with `.retain_capacity` at
  `connection.zig:3570`, `:3592`, `:5153`, `:5377`. A slot keeps the
  largest arena it ever grew to.
- `connection.zig:1655-1662`: `Connection.init` writes every one of
  `max_streams_per_connection` `HandlerJob` entries at connection start;
  slots are claimed in `allocSlot`/`claimSlot` (`:3509`, `:3523`) and
  released in `releaseSlot` (`:3618`).
- The outbound path: `sendAccountedWire` (`:4966`), `queueWire` (`:5041`),
  the two `flushBatch` (`:4320`, `:4508`). Which buffers these first-touch
  has NOT been traced; that is part of your job.

## The open question you must decide: which memory goal?

- G1. Static bound first (TigerStyle). Every byte a connection can use is
  reserved at startup and counted in `resourceUpperBound`. RSS is lowered by
  choosing smaller bounds, never by freeing and reallocating after init. A
  handler that needs more than its budget fails cleanly.
- G2. Low typical RSS first. Bounded post-init allocation and reclaim are
  acceptable if measured; the worst case is still bounded but not reserved.

Argue which one this stack should follow, using the workload it is built for
as the deciding case: long-lived Datastar SSE streams beside one-shot page
loads on the same connection, often thousands of mostly idle connections,
each occasionally rendering a large HTML fragment. Say what the other goal
would cost on that workload.

## What to produce

1. Your pick between G1 and G2, with the argument.
2. Per-request memory (the 39%): a concrete design under your goal. Options
   on the table, none decided: (a) cap retention
   (`reset(.{ .retain_with_limit = N })`); (b) a fixed per-slot request
   budget reserved at boot (a fixed buffer per slot, size in `Limits`,
   counted in `resourceUpperBound`, overflow fails the request); (c) a
   per-executor or per-connection pool of arenas handed to slots on claim
   and returned on release; (d) something better. State what
   `Request.arena` then promises a handler, and what a handler that renders
   a large response does.
3. The `Connection.init` loop (the 18%): is it redundant? Read
   `allocSlot`/`claimSlot`/`releaseSlot` and say whether every field of a
   `HandlerJob` is set before use. Quote the lines. If not, what must be
   added so the loop can go.
4. The outbound path (~27%): trace which buffers `sendAccountedWire`,
   `queueWire` and the `flushBatch` functions copy into, who allocates them,
   their size in terms of `Limits`, and whether they are per-connection
   reserved memory touched under load or pooled. Propose a change if one is
   warranted under your goal.
5. Defaults: is `max_streams_per_connection = 256` (and any other default
   you find load-bearing for RSS) right for the workload above? Propose
   values with the per-connection memory they imply.
6. What your design understates: the case where it is worse than today.

Rules:

- Label each finding **[LOGIC]** (holds regardless of any deployment fact)
  or **[PREMISE]** (depends on one; name it and what must be true for it to
  bite). Unlabelled findings are discarded.
- Quote at least two lines from `src/edge/connection.zig` that are not
  quoted in this brief, so it is clear you read the code.
- Do not edit any file.
