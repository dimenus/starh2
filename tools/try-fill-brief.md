# Brief: the zio try-fill (t-843 step 1)

Plan of record: t-843 revision 4 ("D entered through B"). This is step 1:
give the TLS pump a way to take bytes the kernel already holds without a
park/unpark round trip, as a small additive patch in the dimenus/zio fork,
consumed by starh2's `Pump`.

Two sides, two owners:

- **Fork side (zio):** owned by the session rooted in the fork checkout. It
  knows the fork's recent bug fixes; this brief states the CONTRACT it must
  expose, not the implementation.
- **starh2 side + grading:** the consumer change and the A-B-A grade, per
  this brief, in this repo.

## 0. Phase 0 — re-derive the prize, STOP if it is gone

The ~25k rps prize was measured in the MIGRATION-OFF era (the MSG_DONTWAIT
experiment, commit 6881e51: 123-127k vs the 95k band). Migration now
defaults ON (d120f17) and the on-band baseline is ~124.9k — the park cost
and the placement cost may overlap.

Before any fork work: build the current starh2 HEAD with `-Dobserve=true`
(ReleaseFast) and run the mixed recipe's oneshot-only shape on nachos.
Read the pump counters (they exist since 4675825: `pump_select`,
`pump_select_peek`, `pump_turns`; printed via `--trace` `/trace`).

- If peek-wins per request are still ~0.2 or higher, the pump still parks
  per record and the prize stands: proceed.
- If they collapsed, STOP and report: migration-on already collected this
  win, and step 1 is dead. That is a finding, not a failure.

## 1. The fork-side contract (what zio must expose)

A non-parking receive attempt on a stream/socket that:

1. **Returns immediately** whether or not bytes are ready — bytes copied,
   or a distinct "nothing available now" result. No suspension, no
   completion wait, on the caller's execution context.
2. **Consumes readiness state correctly.** A subsequent blocking read/peek
   on the same stream MUST still wake on later data. This is the whole
   difficulty: a recv done beside zio consumed the kqueue EV_CLEAR edge and
   `waitPeek` then parked past WINDOW_UPDATE (proven twice: commits 5ce4f91
   `posix.read`, 6881e51 `MSG_DONTWAIT`; the second failed Darwin
   `tls-smoke` on `/big` round 2). The primitive must live INSIDE zio's
   loop bookkeeping so the edge accounting stays true.
3. **Works on both backends we deploy** — kqueue (Darwin) and io_uring
   (Linux, with its epoll fallback). Say explicitly how each backend
   satisfies (2); they differ.
4. **Is additive.** No change to existing zio semantics; upstream
   (lalinsky/zio) moves daily and this must rebase mechanically. Base on
   fork HEAD — the fork owner's session landed bug fixes recently; do not
   duplicate or revert them, and read the fork's recent log before writing.

Shape is the fork session's choice (a `tryRead`/`tryFill` on the stream, a
flag on the recv op, a reader-level non-blocking fill — whichever fits
zio's grain). The zio test suite must pass on both backends, and the new
primitive needs its own test that proves property (2): try-read to empty,
then assert a blocking read still wakes on new data, on each backend.

## 2. The starh2 side

- `src/edge/tls.zig` `Pump.readOne` / the quiet-turn path: replace the
  `FIONREAD`-then-`peekGreedy` fill with the new primitive. The `FIONREAD`
  ioctl goes away if the primitive subsumes it.
- `src/edge/tls_async.zig` BIO read path: unchanged contract (WANT_READ on
  empty), but the fill that feeds `tcp_reader` may now use the primitive.
- Update `build.zig.zon` to the fork commit. The pin flow for an unpushed
  fork commit is in project memory (`zig-pin-hash-before-push`): `git
  archive` + `zig fetch` computes the content hash before the push; do not
  guess the hash. `tools/lock.json` moves with it.
- No other production change. The chip lists in t-842/t-843 stay closed.

## 3. Grading — A-B-A on nachos, all axes at once

Control arm = current starh2 HEAD (pre-pin-bump). Fix arm = the pin bump +
consumer change. Both built from their own worktrees on nachos; the
existing worktree flow (push to `t845-story`-style branch, `checkout
--detach`, confirm the SHA) applies. Harnesses take the bench lock
themselves; never run two benches on one box.

Hard lines, every one required:

1. Mixed SSE at the offered count every fix round (report exact numbers;
   ±2 counting jitter is known and shared with the Go arm).
2. Stall 31/32 delivering, every fix run.
3. Mixed oneshot: every fix round within 5% of the same-session control
   band, or above it.
4. Oneshot-only: pre-registered success = every fix round beats every
   same-session control round AND the median gain is at least half of
   whatever phase 0 says the remaining park cost is worth. Report the
   number honestly if it lands short; do not tune the recipe.
5. Server CPU (mixed.sh line) within +10% of control.
6. Packing oracle exit 0 (`tools/oneshot-phase-trace.sh`, redirect not
   pipe for the exit code).
7. `./zb build ci` on the fix arm, Darwin, including `tls-smoke` — this is
   the EV_CLEAR regression catcher (`/big` is the round that dies when an
   edge is stolen). A Linux-only green is not a pass.
8. The zio fork's own suite green on both backends (fork session's gate).
9. Re-run the phase-0 counters on the fix arm: peek-wins per request must
   actually DROP. A throughput win with unchanged counters is measuring
   something else; report it as such.

## 4. Degenerate builds this brief refuses

- A try-fill that spins: bounded attempts only; CPU line is graded.
- A primitive that reads outside the loop's bookkeeping and passes on
  io_uring while stealing edges on kqueue (line 7 exists for this).
- Buying oneshot-only by parking SSE or starving reads (lines 1-3).
- A starh2-side workaround that avoids the fork change (that is the
  current FIONREAD state; step 1 exists because it was not enough).
- Closing t-843 — it is the plan of record for steps 2-3; step 1 landing
  is a milestone note on it, not a close.

## 5. Report

Phase-0 counter table and the go/no-go call it forced; the fork commit(s)
and their tests' output on both backends; the starh2 diff summary and pin
hash; per-round grading lines verbatim with log paths and binary sha256;
the eight-plus-one hard-line results one per line; what you could not do
and why. Every claim cites a file the grader can recompute from.
