# Brief: t-3514, one TLS connection dies early under the mixed shape

You are the investigator. The orchestrator reruns every number you report,
and a reviewer from another vendor reads any fix. Do not tell the
orchestrator that something passes; it recomputes it. Do not prompt, message
or read any other agent's pane.

## The defect

Under the TLS "mixed" shape (complete and task handlers on one connection
set), some requests fail in some runs. Both arms of every A/B show it, so it
is older than the current work.

Observed (orchestrator's runs, all ReleaseFast bench server, `--mode tls`):

- nachos, master 1b4e459 and later, `h2load -t 12 -c 64 -m 16 -n 500000
  /fragment?kb=4 /fragment-task?kb=64`: about 1 run in 6 has 5,000 to 8,300
  failed requests; the rest have 0.
- nachos, one failing run with `--diag`: 8,234 failed = ONE h2load client of
  8,333 requests that ended after about 99. Server diag: that connection
  ended at +160 ms with `TLSERR cqrecv_eof`, that is, the CLIENT closed the
  socket; the other 63 connections ended normally. h2load printed no error
  at its default verbosity.
- Mac, 2026-10-25, the same shape scaled (`-t 8 -n 250000`, `--executors 8`),
  master 913b395 and the slot-pool branch: 3 of 12 runs failed (1,043, 31 and
  3,312 failed requests). One h2load client owns 3,906 requests here, so
  1,043 and 3,312 fit "one client dies part way"; 31 does not obviously fit.
- Mac, an earlier shape (the t-3517 script: task routes kb=4 to 1024 beside a complete load): 1 failure in 7 runs.
  An even earlier Mac series had 0 failures in 118 runs.

Mac TCP settings right now (the owner changed them for an RSS A/B; they reset
on reboot): `net.inet.tcp.msl=1000` (TIME_WAIT 2 s instead of 30 s) and
`net.inet.ip.portrange.first=10000`. The 3-of-12 rate was measured under
them. Treat them as a possible confound: say whether your result depends on
them, but you cannot change them (sudo needs the owner).

## What is not known (do not assume any of it)

- Why the client closes. h2load closes a connection on a protocol error it
  sees (a bad frame, a flow-control violation, a stream error it treats as
  fatal, a TLS record error), and it may close on a timeout. Which one is the
  question.
- Whether h2c shows it. Nobody has run the mixed shape on h2c. That control
  separates "TLS layer" from "HTTP/2 layer".
- Whether the task route matters, or only concurrency.

## Your job, in order

1. **A repro with a rate.** Make a script (absolute paths; the Mac's
   `/bin/bash` is 3.2: no `declare -A`) that runs the mixed shape N times and
   prints failed per run. Measure the base rate on this Mac with n large
   enough to tell 25 % from 5 % (say 20 runs). Then the h2c control at the
   same shape, same n. Every run must show its work (requests started > 0)
   before its failed count counts.
2. **Make the client tell you why it closed.** Options, cheapest first;
   check each against its source before you rely on it:
   - h2load's own output for the failing client: read nghttp2's h2load
     source (brew's version) for what it logs on a connection error, and
     whether `-v` or `--log-file` records it without drowning the run.
   - A per-connection ring in the server of the last N inbound and outbound
     frame headers (type, flags, stream id, length) and TLS record events,
     dumped ONLY when the peer closes before the server did. Cheap, and it
     answers "what was the last thing each side sent". Keep it under a
     comptime flag or the existing `-Dobserve` gate so release builds pay
     nothing.
   - Decrypted capture: whether h2load (OpenSSL) honours `SSLKEYLOGFILE`.
     Verify by trying it, not by reading docs.
   Do not use `--diag`: it writes about 1 GB per run.
3. **Name the cause with a reproduction chain**: the exact commands and the
   observation that shows it, defensible if pushed on. A hypothesis is
   labelled as one. If you find the cause is in h2load, say so with the
   h2load source line.
4. **Stop and report before you fix**, unless the fix is under about 30
   lines and obviously local. A larger fix is a design decision for the
   orchestrator and the owner.

## Premises (settled)

- Read `AGENTS.md` first. zio owns concurrency; `std.Io` abstractions are
  banned. zio and tls.zig are vendored under `zig-pkg/`.
- No external users; breaking changes are free.
- Do not use nachos: the owner is using it.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-3514` (branch
  `carmack/t-3514`, from master 913b395). Never touch
  `/Users/rsaunderson/Source/mine/starh2` or any other worktree.
- Base tripwire, before any work: `git log -1 --format=%s` is the commit that
  added this brief, and its parent's subject starts with "Deliver the GOAWAY
  before closing". If not, STOP and report `STATUS: blocked`.
- `testdata/cert.pem` and `key.pem` are in the worktree.
- Kill only processes you started, by pid. Never `pkill -f`.
- Commit per milestone (an instrument, a test, a fix). Each message is the
  decision record: problem, changes, alternatives rejected, what it depends
  on; markdown; plain ESL English. End it with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file
  under "To file".
- Report in `tools/t3514-report.md` (not committed): the repro script path,
  the rates with n, the h2c control, what the failing client and the server
  each saw last, the cause or the strongest hypothesis with its evidence,
  and what you could not verify. End with `STATUS: done t14a`.

## Round 2 (after the report on b2ef4d0)

The orchestrator rebuilt dbf09eb and b2ef4d0 and ran your rate script, 3
alternating rounds of 10 TLS runs per arm: base 8 of 30 runs failed (365 to
2,843 requests each), fix 0 of 30, every run with work done. Your cause is
accepted.

The owner chose the structural fix. Build it:

1. **HEADERS before DATA, by construction.** A stream's DATA is not
   eligible to be emitted until that stream's response HEADERS has been
   handed to the sink. One per-stream state in `FairScheduler` (set when the
   HEADERS control is emitted, cleared when the stream's slot is released),
   checked where DATA is chosen (`emitOneData`, the forced path in the
   ordinary loop, and any other site that emits DATA). A stream with DATA
   but no HEADERS yet is skipped without losing its place in round robin,
   and without spinning: if nothing else is eligible, `drain` returns as it
   does for a window-blocked stream. Trailers (HEADERS after DATA) must not
   be affected.
2. Keep or drop the b2ef4d0 guard: decide, and say why in the commit. If you
   keep it, say what it still protects that item 1 does not.
3. **Tests:** the race test from b2ef4d0, and the anti-starvation case (64
   other controls, then HEADERS and a body for stream 7; DATA 7 must not
   leave before HEADERS 7), both failing without item 1. A test that a
   stream skipped for missing HEADERS is served once its HEADERS leaves, and
   that a connection with only such streams does not spin.
4. **Mutations** (break the code, full `./zb build test`): do not set the
   state on HEADERS emit; do not check it on the forced path; do not check
   it in `emitOneData`; clear it too early. Record which test fails.
5. **Rate:** your script, TLS, 30 runs on the new tree, and h2c 30 runs.
   Also run the `CONTROL_BEFORE_DATA` case at a shape that reaches it, if
   you can find one with h2load; if not, say so.
6. Throughput: run the TLS complete kb=4 shape and the mixed shape 6 times
   each, alternating with b2ef4d0, and report the ranges. The orchestrator
   reruns them.

Same rules. Do not touch `h1.zig`. Add a "Round 2" section to
`tools/t3514-report.md` (keep round 1 in it). End with `STATUS: done t14b`.

## Round 3 (after review of 735b148)

The orchestrator ran `./zb build test` and `./zb build tls-smoke` on 735b148
on Linux (nachos): both pass. A cross-vendor review found two defects.
For each, first write a test that FAILS on 735b148, then fix:

1. **A reset can leak a `started` entry.** A response's non-END_STREAM
   HEADERS is queued (ordinary queue) while controls are paused; the peer
   resets the stream; `applyStreamResetIntent` calls `forgetStream` (nothing
   to forget yet) and `cancelHandler` drops the DATA slot; a later drain
   emits the stale HEADERS and `noteHeadersOut` inserts the id; nothing
   removes it. Repeated `max_streams_per_connection` times, `drain` returns
   `StreamCap` and fails the connection. Decide the fix and say why: purge
   the stream's queued HEADERS on reset (with its control accounting and
   tickets), or make `noteHeadersOut` refuse a stream the scheduler already
   forgot (a tombstone). Also check: is sending a HEADERS for a stream the
   peer already reset a protocol problem in itself? Test: reset before the
   queued HEADERS reaches the sink, repeated past the set's capacity.
2. **Informational statuses.** `Response.send` and `Response.start` accept
   100-199, but the scheduler treats every HEADERS as the final one, so
   DATA can follow a 1xx and no final HEADERS can follow it. Reject status
   below 200 in `send`, `start` and `startSse` with an error (decided:
   1xx needs its own API; a 1xx with a body is wrong under HTTP anyway).
   Check first whether anything in starh2 emits 1xx itself (for example an
   `Expect: 100-continue` path on HTTP/2) and say what you found.

Mutations: remove each fix, full `./zb build test`, record which test fails.
Same rules. Add a "Round 3" section to the report. End with
`STATUS: done t14c`.

## Round 4 (landing): rebase onto master e7cf701

The second review of round 3 found nothing ("land"). The orchestrator's gates
on 3ad17b2 (Mac, clean cache) pass. Master has moved to e7cf701 (pooled
connection slots: every connection boots from a fixed buffer sized by
`Limits.slotBootBytes`; read its commit message). Rebase this branch onto
e7cf701:

1. `git rebase e7cf701` (keep every commit; resolve conflicts by hand).
2. The `started` key set and anything else this branch allocates in
   `FairScheduler.init` must also be in `FairScheduler.planBoot`, in the
   same order, so the per-boot exact-fit assert holds; use the boot
   allocator for it, not the GPA. Check every other allocation this branch
   added (`bound_shapes.pendingWriteMapBytes`, the wire ring under
   `-Dwire-ring`) against the slot plan and `resourceUpperBound`.
3. Gates on the rebased tree: `./zb build test` (fresh cache), `./zb build
   ci`, `./zb build tls-smoke`, and `-Dwire-ring=true` builds.
4. The rate script on the rebased tree: TLS 30 runs, h2c 30 runs (Mac).

Do not change behaviour beyond what the rebase needs. Add a "Round 4"
section to the report. End with `STATUS: done t14d`.
