# Brief: the server SETTINGS must be the first frame on a TLS connection (t-3511)

You are the implementer. A separate reviewer from another vendor will read
your diff, and the orchestrator will rerun every test and count every number
itself. Do not tell the orchestrator that the tests pass; it recomputes them.
Findings reach you only through the orchestrator. Do not prompt, message or
read any other agent's pane.

## Premises: treat these as settled

- The stack is not in real use and has no external consumers. Do not weigh
  backward compatibility, and do not keep the old order beside the new one.
- Read `AGENTS.md` first. Its premises and its TLS section apply. One line of
  it changes in this work (see "What to change").
- Never guess a dependency's contract: zio is vendored at `zig-pkg/zio/src`
  and tls.zig at `zig-pkg/tls/src`. Read the function you call.

## The defect

RFC 9113 §3.4: the server connection preface is a SETTINGS frame, and it MUST
be the first frame the server sends. On a TLS connection starh2 can send
RST_STREAM (or GOAWAY) first. A client then fails the whole connection (h2load:
GOAWAY PROTOCOL_ERROR "SETTINGS expected").

What is known, and how:

- FACT (code): `Connection.run`, TLS branch (`src/edge/connection.zig`, near
  the `tls_leftover` ingest, about line 3160) ingests the client's first
  plaintext flight (preface, SETTINGS, and any pipelined HEADERS) into
  Session BEFORE the first flush of session intents (about line 3248,
  "Emit after the CQ is armed so queueWire can encrypt").
- FACT (code): `Session.init` already queued the server SETTINGS intent.
  `FairScheduler.classifyControl` puts RST_STREAM and GOAWAY in the `terminal`
  class and SETTINGS in `ordinary`, and `FairScheduler.drain` emits every
  terminal control before any ordinary one.
- So: if the first flight makes Session queue RST_STREAM (a refused stream:
  more streams than the server will accept) or GOAWAY (a connection error),
  that frame is emitted before the server SETTINGS. INFERENCE until your test
  shows it.
- OBSERVED (t-3511, nachos): `h2load -c 256 -m 32 -n 512000 /fragment?kb=4`
  over TLS (8192 streams against `max_streams_per_server` 4096) made h2load
  send GOAWAY "SETTINGS expected" on about 60 of 256 connections. A probe of
  each connection's first pushed chunk showed type 3 (RST_STREAM). h2c at the
  same shape: 0 failures. h2c flushes the preface before it reads anything.
- History: commit 511af4c (t-538) fixed the same class once. SETTINGS ACK was
  terminal and went out first; it moved to `ordinary`. That commit rejected
  "gating the scheduler on preface sent" as a second place that knows about
  preface order. Read its message (`git show 511af4c`).

## What to change (decided; do not reopen)

Remove the class by order of operations, not by another classification rule:

1. On TLS, flush the server preface to the write path BEFORE ingesting the
   handshake leftover plaintext. Then ingest the leftover, then flush again.
   After this, nothing the client sends can be queued ahead of the server
   SETTINGS, because the SETTINGS already left the scheduler.
2. Check that the input order is unchanged: the leftover plaintext must still
   be ingested before any byte the TLS `Pump` reads from the socket after it.
   The `Pump` is driven by the actor (it is not a task), so arming it should
   not read anything by itself. Verify that by reading `src/edge/tls.zig` and
   the arm code, and state what you found in the commit message, quoting the
   lines.
3. Keep the existing early exit (`if (self.session.terminal != .none) return
   error.ConnectionClosed;`) semantics correct: a first flight that is a
   connection error must now produce SETTINGS and then GOAWAY on the wire, if
   the write path allows it. If it cannot, say why in the commit message.
4. Add an invariant at the one place every production frame passes (the
   `FairScheduler` sink path into the write chunk, see AGENTS.md "All
   production wire output passes through FairScheduler's sink"): the first
   frame a connection writes is a SETTINGS frame without the ACK flag. A
   violation panics in safe builds (`std.debug.assert` or an explicit
   `@panic` with the frame type). Cover h2c and TLS. It must cost nothing in
   ReleaseFast beyond one predictable branch.
5. Update the AGENTS.md line "Handshake leftover plaintext (a pipelined
   preface) is ingested on the actor before the queue is armed." to the new
   order. Protocol comments beside the code move with the code.

## Tests (required)

- A TLS test whose first flight (pipelined into, or right after, the
  handshake, the way curl and browsers send it) makes the server refuse a
  stream. Use a small `Limits.max_streams_per_connection` or
  `max_streams_per_server` so the first flight exceeds it. Assert the first
  frame the client receives is SETTINGS without ACK, and that the refused
  stream gets RST_STREAM REFUSED_STREAM after it. Look at
  `tests/lifecycle.zig` "the first wire frame is the server's own SETTINGS"
  (t-538) and the TLS tests in `tests/` for existing helpers. Run it in a
  loop of at least 20 connections in one test, because the order depends on
  what lands in the first read.
- A TLS test whose first flight contains a connection error (for example a
  frame on stream 0 that is illegal there). Assert SETTINGS first, then
  GOAWAY.
- The test must FAIL on the current code. Prove it: run the new tests on the
  base commit (with only the tests added) and record the failure output. If
  it does not fail on the base, the test does not reach the defect; stop and
  report `STATUS: blocked` with what you saw.

## Mutation tests (required; report each one)

Break the CODE, not the test. For each, run the full suite and record which
test fails:

1. Restore the old order (ingest leftover before the first flush).
2. Remove the first-frame invariant.
3. Make the invariant accept SETTINGS with ACK.

Do not regenerate or loosen any existing test expectation. A changed
expectation in an existing test is a finding the reviewer will ask about.

## Gates

- `./zb build test` and `./zb build ci`. On this Mac, `ci` always fails one
  step, `macos-libcxx-probe` (environment). Every other step must pass.
  Report the full step list, not a summary.
- `./zb build tls-smoke` passes.

## Rules

- Work only in your worktree. Never touch `/Users/rsaunderson/Source/mine/starh2`.
- Base tripwire, before any work: `git rev-list --count HEAD..394c1ee` must
  print 0, and `git log -1 --format=%h` must show the commit that added this
  brief. If not, STOP and report `STATUS: blocked`; do not rebase.
- Commit per milestone (tests first, then the fix, then the invariant).
- The commit message is the decision record (see `~/.claude/CLAUDE.md`
  "Commits"): problem, changes, benefits, alternatives rejected, what it
  depends on, statistics, in markdown, plain ESL English. End it with
  `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Do not push. Do not close or edit tasks.
- Write a short report to `tools/preface-first-report.md` in your worktree
  (not committed): commits, the base-failure output, each mutation and the
  test that caught it, anything you could not verify.

## Round 2 (from the orchestrator; supersedes "What to change" item 3)

The orchestrator reran your work: the new tests fail on 8afb273 in 3 of 3
runs (four RST_STREAM ahead of SETTINGS; NoFrames for the connection error),
`./zb build test` at 9bd1445 is 418/419 with 1 skip, and mutations 1, 2 and 3
are each caught. A reviewer from another vendor then read the diff. The
orchestrator checked each finding below against the code; all four hold.

1. `Session.terminal` is now read outside `session_mu`
   (`if (self.session.terminal != .none)` after the locked block). Ingest can
   spawn task handlers, and they mutate `terminal` under that mutex (see the
   "Both checks stay INSIDE this lock" comment in the main loop). Read it
   inside the locked block into a local.
2. Remove `writeStashBeforeClose` from this change. It converts
   `error.Canceled` from `zio.select` into a plain return (`catch return`),
   so `run` loses `zio.recancel()`. More important, it is a second TLS drive
   loop for one case of a general defect: a connection error drops its
   GOAWAY at any time, not only in the first flight. That gets one fix, in
   the general teardown path, as t-3519, in a later round. Not here.
   The connection-error test stays, but it asserts only the order this change
   owns: every frame the client receives, if any, starts with the server
   SETTINGS (non-ACK). Write in the test, beside the assertion, that GOAWAY
   delivery is t-3519 and the assertion tightens there. Do not delete it.
3. The tests do not prove the leftover path ran. The client coalesces
   Finished and the HTTP/2 flight in one write, but the server may read only
   Finished. Add a test-only counter (same style as `test_preface_checks`) of
   connections that started with a non-empty `tls_leftover`, and assert in
   both TLS tests that every tested connection took it. If the counter shows
   that some connections did not, make the client shape force it, or report
   it.
4. The `## Startup sequence` doc comment on `run` (about line 3093) still
   says ingest, then arm, then flush. Update it to the new order.

Amend the commits or add new ones, your choice; the commit messages must
describe the final state, not the path. Same gates and report as before;
add a "Round 2" section to the report.
