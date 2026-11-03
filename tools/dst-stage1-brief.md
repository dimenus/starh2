# Brief: DST stage 0 and stage 1 for starh2 (test-race fix, HTTP/1.1 TLS failure witness)

You are the implementer of the witness track. Worktree
`/Users/rsaunderson/Source/mine/starh2-wt-dst`, branch `carmack/dst-witness`,
based on `d33e615` (t-3759 step 7: HTTP/1.1 onto links, `h1AckSink`). Tripwire
before any work: `git rev-parse HEAD~1` must print `d33e615...` (HEAD is the
commit that adds this brief). If not, STOP.

Never touch any other worktree or branch. A second implementer works on the
zio simulator port in `starh2-wt-dst-port` and `zio-wt-sim-port`; you do not
talk to it. Reviewers (Fable, gpt-6-astra) read your commits; their findings
reach you only through the orchestrator. Do not prompt any other agent.

## Why (read these first)

`AGENTS.md`. Then the design debate that produced this plan, read in full:
`/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/dst-brainstorm-brief.md`
and its rounds, and the answers in
`/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/dst-r1/`, `dst-r2/`,
`dst-r3/` (above all `dst-r3/dst3-astra.md` section 1 and "One joint plan").
The three designers signed: a narrow, deterministic witness first; the zio
simulator port in parallel; no hooks in the actor.

## Premises (settled)

- No external users; breaking changes are free.
- zio owns concurrency; `./zb build std-io-gate` must stay green.
- No scheduling hooks or pause points inside the connection actor. Mutants are
  compiled out of release builds (`src/edge/test_hook.zig` shape: a comptime
  flag, `void` outside test builds). Never a runtime env var in production code.
- Every claimed capability needs a failing witness, an automatic oracle, and a
  replay check. A clean sweep against a mutant is a FAIL.

## Milestone 0: make t3567's idle table measure what it claims (1 commit)

`tests/t3567_tls_barriers.zig` `runIdleOp` joins the client, then reads
`ops_done` before the handler is known to have resumed (`:777-834`); the
handler increments it only after `send` returns (`:135-139`, `:241-242`). So
`ops=0` can mean "not resumed yet", not "send failed". This test has failed in
about 1 of 3 full-ci runs on Linux.

- Each idle handler records its terminal call's result at return, before
  anything can suspend: not returned / ok / the error name.
- `runIdleOp` waits for that record (bounded, with a named failure if it never
  arrives) and judges the record, not a counter read at an unsynchronised
  moment. "Not returned" is never classified as an error.
- Keep every existing assertion's meaning; only fix the synchronisation.
- Validity: a mutant that makes the handler's `send` return an error after the
  response is delivered must fail the test with the error name. Show it.
- The orchestrator is measuring the old test under full-ci load right now;
  you do not need that result to do this.

## Milestone 1: the HTTP/1.1 TLS failure witness (3-5 days)

The bug it must catch: in an earlier version of step 7, the HTTP/1.1 TLS pump
posted failure acks into a 4-slot channel nothing read, and `Pump.post` panics
on overflow. One client resetting at the right moment crashed the server. The
fix wired `.ack_apply = h1AckSink` (`src/edge/h1.zig:411-433,469-471`). The
end-to-end regression is `tests/goaway_delivery.zig:2795-2894`; it is not
deterministic.

Build:

1. **Shared production construction.** Factor the HTTP/1.1 TLS pump
   construction and failure/finalisation out of `H1Conn.startTlsPump`
   (`h1.zig:440-483`) so production and the witness use the same function and
   the same ack wiring. Keep `Pump.start`'s final-address rule
   (`tls.zig:1625-1658`). A fixture that supplies its own ack wiring does not
   count.
2. **The driver** (`tests/dst_tls_lifecycle.zig` or similar). Drive the real
   pump's completion handlers (`Pump.onCqComplete`, `tls.zig:1120-1132`) with
   explicitly modelled completed results. Enumerate (exhaustigen style, small
   bounds): send error vs receive error vs EOF; zero vs one borrowed chunk;
   every legal teardown position. Stop normal driving on `.exit`
   (`h1.zig:783-804`). Never fabricate acks and never submit synthetic
   descriptors to a kernel. Add a seeded permutation of the same actions:
   seeds 0-63 plus 16 seeds derived from the commit hash, each run twice.
3. **The oracle**, checked after every step by the driver: no notification
   left in the unread channel (fail with a named invariant such as
   `UnconsumedTlsAck` on the FIRST abandoned one, not at overflow); at
   finalisation every borrowed chunk returned exactly once; a terminal result
   and a completed teardown actually happened in each case (a case that did no
   work FAILS, it does not pass). Print per-run counts of each fault injected
   and fail if a configured fault never fired (fault-rate floor).
4. **The mutant**: removing the `h1AckSink` wiring in the shared constructor,
   as a compiled-out test flag. The ci step must build the mutant, require the
   named invariant failure, and require the same case to fail identically
   twice (same trace, same invariant).
5. **ci**: `./zb build dst-transport` (control + mutant), wired into
   `./zb build ci`. Budget about 30 s excluding compilation; print the
   completed-case count; finishing under the corpus minimum fails. Every
   failure prints seed, case, build identity, the event trace, the invariant
   and the one-line replay command.

Scope line, printed by the step: this witness proves the HTTP/1.1 TLS
transport cannot regain an unread failure channel. It does not reproduce the
HTTPS crash end to end, and it says nothing about HTTP/2, the scheduler, or
real threads.

## Gates and evidence

- Mac: `./zb build ci` on a `git archive` export with a fresh `--cache-dir`.
  Known failures: the two `macos-libcxx-probe` steps (t-3540), occasional
  `deadlines T2/T3`, `compression I4`, `t3567`, `outbound_budget order (h2c)`.
- nachos (Linux): only in a new directory `~/src/dst-witness` (copy `zig-pkg`
  and `testdata/*.pem` from `~/src/s-3759-spike`; `git archive <sha> | ssh
  nachos 'tar -x -C ~/src/dst-witness'`; `export
  PATH="$HOME/.local/share/mise/shims:$PATH"` first). Touch no other
  directory, change no system setting, kill only what you start, by exact PID.
- Wrap every build and run in a time limit (`perl -e 'alarm shift; exec @ARGV'
  <s> ...` on the Mac, `timeout` on nachos). Build each test binary with its
  own `--cache-dir` and check there is exactly one binary before running it;
  `zig build` caches passing test runs, so run binaries directly to repeat.
- Run the new steps at least 5 times on each machine. Mutation-test every new
  check by breaking the CODE, not the test, and read why each mutant fails.
- Do not regenerate any golden or expected-output file.
- Do not tell the orchestrator the tests pass; the orchestrator recomputes.

## Commits and report

One commit per milestone, each with a full decision-record message (problem,
changes, benefits, alternatives rejected, what it depends on, statistics).
Never push. Write `tools/dst-stage1-report.md` (not committed): per milestone,
the commit, each test's result before and after, the mutation table, ci on both
machines, every corner cut, and anything not measured labelled as inference.
Stop after milestone 0 and report; the orchestrator will tell you to continue.
