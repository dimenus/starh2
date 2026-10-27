# Brief: t-3567 triage, TLS bare flush barriers and a full stash

You are the investigator. Do not fix anything. The orchestrator reruns every
test and measures everything itself; do not tell it that tests pass. Do not
prompt, message or read any other agent's pane.

## The question

Can bare flush barriers on TLS (a handler's `Body.flush` with nothing
pending, a zero-length ticketed write) fill the TLS stash and fail the
connection? If yes, how often, under what shape, and what is the smallest
correct fix?

## What is known (from reading; each line is to be checked, not trusted)

- On h2c, a bare flush barrier waits outside `session_mu` for room in the
  write-ack channel and the write queue (`waitForAckRoom`, `barrierRoom`,
  `connection.zig` near 5283). That wait is armed only on the h2c start path:
  `self.ack_room_armed = true` near 4063. On TLS `waitForAckRoom` returns at
  once.
- On TLS, `pushWriteChunk` (near 6437) puts the chunk in the stash with
  `pump.push`. A full stash counts `tls_write_overflow` and returns
  `WriteFailed`. Its comment says the scheduler's `pause` / `pause_control`
  hooks stop the drain before the stash fills, "so trySend is the
  last-resort fail-close". A bare barrier is sent by `sendFlushTicket` (near
  6399): check whether it goes through those pause hooks at all.
- `tests/outbound_memory.zig` has "bare flush barriers against a non-reading
  peer wait outside the lock..." (near 853). Check which transport it runs and
  why it does not catch this on TLS, if it does not.
- Option A (a70a088) landed the TLS test holds `test_hold_writes` on the TLS
  pump and `test_hold_stash`; use them so the stash fills without depending
  on socket buffer sizes.

## What to do

1. Read the TLS barrier path end to end and write down, with quotes, which
   check (if any) stops a barrier before a full stash. Say whether a full
   stash on this path fails only the stream or the whole connection.
2. Build a reproduction: a real TLS server, the raw client in
   `tests/wire_client.zig`, many streams whose handlers flush with nothing
   pending while the stash is held full. Count `tls_write_overflow`,
   connections ended by the server, and failed streams. Run it n>=10 times
   and give the rate. Also run the SAME test on h2c as the control, where
   the wait is armed: it must not fail. If the TLS case does not reproduce,
   prove the test reached a full stash (a counter, not a guess) before you
   call it refuted.
3. If it reproduces: give two or three fix options with a recommendation.
   Consider at least: (a) give TLS the same wait as h2c (`waitForAckRoom`
   with a stash-room predicate; the TLS actor posts no write acks, so say
   what wakes the waiter); (b) send barriers through the scheduler so the
   pause hooks cover them. For each, say what the lock order is, what wakes
   a waiter, and what an inline (complete) handler does, since the actor
   must not wait for itself.
4. Commit the reproduction test on this branch as a test that is EXPECTED to
   fail today, gated so `./zb build test` stays green (a separate build step,
   for example `zb build test-t3567`), and say how to run it.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-3567` (branch
  `carmack/t-3567`, from master a70a088). Never touch any other worktree,
  including the one your pane started in.
- Base tripwire before any work: `git -C
  /Users/rsaunderson/Source/mine/starh2-wt-3567 log --format=%s -2` shows,
  newest first, "Add the triage brief for t-3567" and "Park a writer when the
  connection's outbound budget is full...". If not, STOP and report
  `STATUS: blocked`.
- `testdata/cert.pem` and `key.pem` are already copied (gitignored).
- Kill only processes you started, by pid. Never `pkill -f`.
- Never present inference as fact: label each claim as measured (with the
  command) or read (with the quote) or inferred.
- Commit messages: problem, changes, what it depends on, statistics;
  markdown; plain ESL English. End with
  `Co-Authored-By: <your model> <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file.
- Report in `tools/t3567-report.md` (not committed). End with
  `STATUS: done t67a`.
