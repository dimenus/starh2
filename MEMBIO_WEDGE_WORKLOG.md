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

## Do not

- Re-litigate the pump wire path or reintroduce a Select; both are
  measured clean.
- Fold this into t-850 (deadline-heap flake) or the closed 57359b7 stall;
  same family, different producer.
- Conclude from the 274/67 numbers before establishing counter units.
