# mem-BIO oneshot wedge worklog

Live debug log for the open oneshot wedge on the memory-BIO cut (302c0d7,
t-843 step 1). Meant to survive compaction and to become the commit message
when the fix lands. Not a design doc.

Reproduce: server `--mode tls --executors 2` (migration default on), then
the mixed.sh client sequence — 1-worker probe (healthy, ~52k rps), then
`sse-client -streams 0 -oneshot-workers 8` on one connection. Wedges to 0
rps most rounds, ~0.7 s in, after a fast burst. h2load shapes never wedge.

## Captured at the wedge (nachos 2026-08-19, evidence in captures/membio-wedge/)

- Kernel: `Recv-Q 0, Send-Q 0` both sides; ~38,400 data segments moved
  before the stop; `lastsnd/lastrcv` frozen. All bytes are in userspace.
  (`kernel-t8.txt`, identical at t14.)
- All executor threads idle in `io_cqring_wait`; nothing runnable.
- STARH2_PARK snapshots (server2.log): actor parked with
  `pending=0 bytes=0 framed=0 intents=0 refilled=false rst*=0 tomb*=0`,
  and `live=1` — one handler alive at park.
- Counter imbalances at full wedge (trace-t14.json; pairs net zero on
  healthy runs):
  - `read_take_n = ingest_n = 84176` — inbound fully consumed.
  - `write_chunks = pump_write_chunk_sum = 83846` — wire fully drained.
  - `ingest_n - jobs = 274` — ingested, never dispatched.
  - `jobs - handoffs = 67` — dispatched, never emitted.
  - `pump_select = 0` throughout — the Select-free structure held.

## Reading (inference, marked)

The wedge is NOT in the new pump wire path (drained) and NOT the pump's
Event wait (its reset-then-recheck at src/edge/tls.zig:1048-1066 is
ordered correctly, and /trace connections' pumps run fine while one
connection is wedged). The actor parked while holding 274 undispatched
ingests and 67 unemitted jobs: the lost wake is on the ACTOR's
ingest-to-dispatch path under the 8-worker burst shape. `live=1` says one
handler never finished; the 630 ms trickle seen once (mixed-fix-6 round 1,
64 reqs at p50 630 ms) is some periodic tick partially unsticking it —
that cadence names the wait once identified.

## Next (for the fix round)

1. Establish exact semantics of `ingest_n`, `jobs`, `handoffs` counters at
   302c0d7 (they may count frames vs requests differently; the 274/67 gaps
   need units before they become claims).
2. The wake invariant audit from TLS_STALL_BRIEF.md applies verbatim: for
   each actor_wake producer on the new-code paths (CipherRead post,
   complete-batch receipts, pump completions), name the durable state that
   survives a set-before-reset race and that the actor re-checks before
   parking. The mem-BIO rewrite added producers; the park snapshot shows
   the actor's recheck missing at least one of them.
3. A deterministic regression test that pauses the actor between reset and
   park, publishes from the suspect producer, and proves processing.
4. Do not fix with a timeout or poll; the actor deadline that made the old
   stall a 30 s park instead of forever is not a fix here either.

## Second specimen: h2load triggers it too (2026-08-19 EOD)

An EOD perf run at 302c0d7 wedged inside tools/oneshot-phase-trace.sh
itself: h2load `-n 400000 -c 50 -m 10` parked in futex while ONE of its 50
connections' server side sat in io_cqring_wait. Counters at the wedge
(captures/membio-wedge/trace-h2load-wedge.json): jobs=391679
writes=391678 of 400000 — one connection wedged holding its last ~8.3k
requests; 49 connections completed. So the trigger is NOT the Go client
shape specifically: any sufficiently hot pipelined connection can park,
with per-connection probability low enough that a lucky phase-trace run
passes. The fix session's "h2load shapes run fine" was a passing sample
(the validity trap, again).

COUNTER UNITS, now calibrated from this specimen: `jobs` counts REQUESTS;
`ingest_n`/`read_take_n` count INGEST TURNS (~10 requests each at -m 10);
`handoffs`/`tickets` count drain-turn receipts, not requests. The first
specimen's "274 ingested-undispatched" therefore means ~274 ingest turns
(~thousands of requests) held undispatched — direction unchanged, scale
larger.

CONSEQUENCE: no perf number from a 302c0d7-lineage arm is trustworthy on
ANY axis until this closes — a run that completes may have been one lucky
draw. The t-866 fix gates all further measurement of this arm.

## Third round: rates, falsifications, and live wedge timelines (2026-08-19, verifier session)

Quantification (Darwin, M3 Pro, sha 806137a unless noted):

- h2load specimen rate: 5/30 rounds wedge under a 60 s watchdog (healthy
  round: ~0.9 s). Tool: `tools/h2load-wedge-rate.sh`. Evidence bundles under
  `/tmp/starh2-h2load-wedge-rate*/wedge-*/`.
- Go-client probe (WORKERS=2): baseline 2/4 wedges; `--no-task-migration`
  1/6; `--executors 1` 2/6 (instrumented tree). The wedge SURVIVES all three.

FALSIFIED as sole mechanisms, by A/B or by direct state capture:

1. zio task migration / searcher / steal protocol (survives migration off).
2. `sleep(.zero)` arming loop timers at this pin (patched the vendored zio
   with upstream f5f5d28, zero-sleep = yield: wedge persists).
3. Multi-executor announce races (survives `--executors 1`).
4. A pump-only lost event edge: dirty flags + put-then-set are in place in
   the instrumented tree, and the wedge persists with the flags CONSUMED.
5. A hard resource cycle: external `futexWake` re-kicks (OS-thread sweeper)
   and any periodic scheduler activity advance the system by roughly one
   stage per tick, so the state is re-drivable, not a cycle.

DIRECTLY OBSERVED, with `--diag` timelines (raw timestamped stderr, task
state tags from a locally patched zio; see `scratchpad diag-run` captures):

- A parked task with its wake already published: pump task tag=waiting with
  `tls_pump_wake=is_set` and `pump_dirty=true` for many seconds (twice), and
  the actor parked with `actor_wake=is_set` once. One lost `futexWake` is
  then self-sealing on that event: `Event.set` on an `.is_set` event skips
  the wake syscall, so every later producer notify is a silent no-op.
- The 6 s quiet stretch of one wedge showed CipherRead parked INSIDE the
  socket read (ciph_site=3, tag=waiting) while the client had pipelined
  requests outstanding; the client's FIN at teardown delivered everything
  at once. That implicates loop completion delivery (kqueue backend), not
  the futex layer. Upstream zio d0c4d79 ("non-parking tryRead that keeps
  kqueue edge accounting") touches this exact area one commit after the pin.
- A trickle mode is real and reproduces: with a 500 ms in-runtime watchdog
  task present, the wedged connection advances ~one stage per tick. Any
  periodic task nudges the scheduler into delivering one more stranded
  wake. This is the 630 ms trickle of the first specimen, generalized: the
  cadence is whatever periodic task happens to exist.

WORKING MODEL (hypothesis, one level down from the previous round): a wake
delivery in zio is lost between `Waiter.signal`/loop completion and the
task actually running - the victim is arbitrary (pump event, actor event,
CipherRead socket read, watchdog timer), it reproduces on kqueue and
io_uring, with 1 or 2 executors, with and without migration. The two
sharpest single observations for a zio-level minimal repro are (a) a task
parked in `futexWait` while its Event is `.is_set`, and (b) a socket read
never completing while bytes are queued and the loop is in `kevent`.

Instrumentation added (all `--diag`-gated, committed on this branch):

- `STARH2_TLSQ`/`STARH2_PARK` raw timestamped snapshots with conn id, pump
  site (1 run, 2 wake.wait, 3 writeChunks, 4 ingest, 9-11 yield sites),
  CipherRead site (2 index, 3 socket read, 4 post), both Event states, and
  (with the local zio patch) task state tags.
- An OS-thread sweeper in the bench server (`STARH2_SWEEP` every 500 ms)
  that also re-fires `futexWake` on the wedge signatures (`STARH2_REKICK*`).
  It must be an OS thread: near the wedge, in-runtime watchdog tasks are
  themselves victims. `std.debug.print` cannot be used off-runtime (stderr
  lock) and interleaves non-chronologically (positional writes); use the
  raw writers in this instrumentation only.
- `tools/h2load-wedge-rate.sh` and a SERVER_ARGS passthrough in
  `tools/wedge-probe.sh` for A/B arms.

Machine-local (NOT committed; zig-pkg is gitignored): the vendored zio at
`zig-pkg/zio-0.17.0-xHbVVMdhJwAj...` carries (a) upstream f5f5d28
(zero-sleep = yield) and (b) `debugCurrentTaskHandle`/`debugTaskStateByte`
diag exports. `bench_server` guards the hook with `@hasDecl`, so a
pristine pin still builds. Re-apply by hand when reproducing task tags.

## Fourth round: the standalone repro is RED (same day, later)

`tools/zio-wedge-repro` (37c79cc) reproduces the defect class with no
starh2 code, seconds per run. The unlock: clients must be RAW OS THREADS;
in-runtime zio clients keep the executors busy and mask the window. The
window is an idle runtime with executors parked in kevent.

Shape `--connections 1 --pipeline 2 --requests 30000 --rounds 40`, Darwin:

- default (2 exec, migration on): 7/9 stall - listening=true,
  client_connects=1, accepts=0. The kernel completed the TCP handshake;
  zio's accept never returns.
- --no-migration: 6/6 - accepts=1 but the spawned connection task never
  ran (zero registrations).
- --executors 1: 6/6.
- PRISTINE UPSTREAM zio HEAD d0c4d79 (path dep at ~/Source/oss/zio,
  task_tags=false in the banner proves the unpatched build): 5/6. The bug
  is live upstream, not an artifact of our pin or local patches.

Oracle (b) is retired: live netstat sampling during a starh2 wedge shows
Recv-Q/Send-Q zero on both sides throughout (matching the nachos capture),
so nothing ever sat in the kernel; the CipherRead quiet stretch was
innocent (nothing to read - the responses were the missing side).

CONSEQUENCE: this is an upstream zio scheduling/wake-delivery bug. A task
made runnable (an accept completion, a fresh spawn, a futexWake) is never
run while the executors sit parked. starh2's own churn usually rescues the
stranded task, which is why the in-app rate is low and why any periodic
task produced the trickle. The upstream report should come from Ryan and
can ship `tools/zio-wedge-repro` verbatim with the rates table.

## RETRACTION of the fourth round's standalone-repro claims (2026-08-19, later)

The red repro was a HARNESS ANTI-PATTERN, not a zio bug. zio-wedge-repro
called std.Thread.spawn/join per round from inside a zio task. An OS join
blocks the executor thread outright; a socket is pinned to the loop that
registered it (zio #582 documents the placement rule), so the listen
socket's completions starve behind the blocked loop and the harness
deadlocks itself: client waits on server, accept waits on the blocked
loop, the joining task waits on the client. Stall probability was
placement luck.

Proof both ways: a minimal accept repro (clients spawned/joined only from
main, outside the runtime) passes 6/6 x 200 rounds; the same red shape
goes 6/6 GREEN after moving client thread management out of the zio task.

RETRACTED: the repro's 5/6-6/6 red rates, the accept/spawn victim shapes,
the pristine-upstream-HEAD confirmation, and the "OS-thread clients
unlock / rescue-by-churn masks the repro" framing. The rule that survives:
NEVER call a blocking OS primitive (Thread.join, nanosleep, blocking
reads) from inside a zio task; the loop that owns your sockets stops.

STILL STANDING, independent of the repro: every in-app starh2 capture
(rounds 1-3), including the pump parked in futexWait with its Event
.is_set, and the starh2-side A/B falsifications. The starh2 wedge shows
all executors PARKED IN KEVENT at 0% CPU, which is not the blocked-thread
shape, and starh2 has no OS joins in tasks. The wedge is real and remains
unexplained; it is no longer reproduced standalone.

This is the #446 precedent caught on our own instrument before filing.
Nothing goes upstream until a mechanism is named in a clean harness.

## ROOT CAUSE FOUND AND FIXED (2026-08-20): ciphertext stranded in BIO-in

zio is INNOCENT. The wedge was starh2's: the pump's read gates used
`pendingPlaintext()` (SSL_pending), which counts processed-record
PLAINTEXT only and is blind to whole unread ciphertext records still
sitting in the BIO pair's inbound side. Under a pipelined burst, a cipher
chunk feeds several TLS records into BIO-in, the read loop stops at a
record boundary when SSL_pending hits zero, and the pump parks on top of
the client's next requests. Nothing re-wakes it for them: CipherRead only
posts NEW socket bytes, and the client sends nothing more until answered.
Permanent, per-connection, probabilistic on record-vs-chunk boundaries.

The confirming instrument (ledgers, all balanced, then the tell):

    STARH2_SWEEP2 ... tres=323 tcomp=323 tdrop_free=0 tdrop_mm=0
                      bio_in=296 bio_out=0 ssl_pend=0

296 bytes of unread inbound ciphertext with every task ledger balanced
and every task parked clean. `BIO_ctrl_pending(ssl_bio)` sees what
SSL_pending cannot.

THE FIX (src/edge/tls.zig): keep an alias of the SSL-side BIO
(`Conn.ssl_in_bio`), expose `pendingInboundCiphertext()`, and include it
in BOTH pump read gates (the mid-loop gate and the pre-park gate). The
pre-park gate attempts `readOne` and parks only on `.want` (a partial
record - only new socket bytes can finish it, and CipherRead wakes the
pump when they arrive), so the widened condition cannot spin.

Why every earlier theory died on this bug:

- It survives every scheduler A/B (migration, executors=1, zero-sleep,
  timer executor removed at the zio merge-base) because it is not a
  scheduler bug.
- The "task parked with its Event .is_set" observations were transient
  sweep-read races, never a sustained state - the wake-ledger (hop
  stamps in a patched zio) showed all tasks parking cleanly with no wake
  owed at the terminal state.
- The trickle: any later inbound byte or write path re-runs SSL_read and
  releases a few more buried records. Cadence = whatever periodic
  traffic exists.
- h2c never wedges: no BIO pair.

GRADED (Darwin, M3 Pro, pristine fork-pin zio restored for grading):

- wedge-probe WORKERS=2: 9/9 PASS at ~20.9k rps (band was 8-14k with
  intermittent 0s before the fix).
- wedge-probe WORKERS=8: 6/6 PASS at ~42.8k rps (was ~14k and 1/3 red).
- tools/h2load-wedge-rate.sh: 0/30 (was 5/30).
- zig build test green; tls-smoke PASS.
- MUTATION-PROVEN: reverting both gate terms brings the wedge back
  (2/6 red, band collapses to 8-14k); reverting only the pre-park term
  stays green (the mid-loop gate is the load-bearing one; the pre-park
  term is defense in depth).

The fix also RECOVERS THROUGHPUT: the "healthy" pre-fix band was partly
made of sub-second stalls that the averages hid.

Deferred: a deterministic unit test that feeds two application records
in one cipher chunk and asserts the pump does not park with BIO-in
occupancy (filed in the task store). The probe and the h2load rate gate
cover the behavior until then.

## Prior art in the zio tracker (searched 2026-08-19, before any filing)

The bug is NOT already reported. Upstream is lalinsky/zio (the dimenus fork
has issues disabled). All ~100 issues scanned plus keyword searches; the
neighbors, each checked and distinct:

- #446 (closed 2026-07-09): "cross-executor cancel of fibers parked in
  std.Io.Select" - same symptom family (all executors parked in kevent,
  runnable fiber lost) but the mechanism was a Zig STDLIB Condition.wait
  swallowing error.Canceled plus one-shot Future.cancel misuse. Our repro
  has no cancellation in the hot path and the accept shape has no Select.
- #530 (closed): "IOCP: intermittent lost wakeup" - root-caused to a
  Windows-only accept-completion use-after-free (one AcceptEx, two
  completion packets). Different backend, different mechanism.
- #347, #340, #384: closed long before our pin; config error and old
  RwLock hangs respectively.
- #685 (Ryan's, closed): timer starvation when workers never wait -
  different mechanism (no parked executors).
- #582/#574 (closed): spawn delivery and stealing churn - performance
  shape, not correctness; useful background for the report.

The report should cite #446 and #530 as checked-and-distinct so the
maintainer does not close it as a duplicate on the symptom alone.

## Next (fifth round)

1. Bisect INSIDE zio with the red repro (delegated): the kqueue backend's
   completion delivery and the park protocol in runtime.zig - the
   wake_requested swap in Loop.poll, drainDispatched, processCleanup, the
   idle_mask/searcher fences. State which zio the bisect ran against (the
   local vendored copy carries an f5f5d28 backport and two debug exports).
2. Confirm the same red shape on nachos/io_uring to decide backend-shared
   vs backend-specific before the upstream report.
3. If a zio fix lands, grade with: probe 3/3 at WORKERS=2 and 8,
   `h2load-wedge-rate.sh` 0/30, then the full t-843 grading lines.

## Do not

- Re-litigate the pump wire path or reintroduce a Select; both are
  measured clean.
- Fold this into t-850 (deadline-heap flake) or the closed 57359b7 stall;
  same family, different producer.
- Conclude from the 274/67 numbers before establishing counter units.
