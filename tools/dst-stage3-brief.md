# Brief: DST stage 3, one HTTP/2 connection inside the simulator

You implement stage 3 of the plan the three designers signed
(`/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/dst-r3/`, each answer's
"joint plan"; round 3's disputes 2, 3, 6 are the ones that shape this stage).
Worktree `/Users/rsaunderson/Source/mine/starh2-wt-dst-port`, branch
`carmack/dst-zio-port`. Tripwire: HEAD is the commit adding this brief, and its
first parents include the zio pin move to `031b15c` and the `dst-zio` ci
wiring. If not, STOP. The zio fork worktree `zio-wt-sim-port`
(`sim/dst-m2`) is yours too, for sim-side changes; push nothing.

## Goal

Run one real h2c `Connection` (real actor, real read and write pumps, real
ticket table, real handlers) inside the zio simulator, against an in-sim peer,
so that every interleaving of the actor, the pumps, the handler and the peer is
chosen by one seed and replays from it.

## Premises (settled)

- Entry: use the existing production path below the accept loop,
  `Connection.serveAccepted` (or `connEntry`'s post-accept body if admission
  is in scope). Do not add a public `Server.serveConnected`. If a sim pipe end
  cannot be passed where a `std.Io.net.Stream` goes, STOP and report the shape
  you would need.
- The simulator runs `.pinned` scheduling, 1-2 logical executors. Production
  places a connection's pumps with `connPlacement()` (`.local` under pinned)
  and `simCoopYield` skips its own executor. So more executors do not by
  themselves create actor/pump interleavings. Prove with coverage marks which
  interleavings each run actually reached (below); never claim one from the
  executor count.
- No scheduling hooks or pause points in the actor. Any new seam is at the
  transport or in zio, `void` outside test/sim builds. Mutants are compiled-out
  flags, never runtime env vars.
- Never randomise which ready select arm wins; randomise readiness timing and
  task picks only.

## The peer and the oracle

- The peer is a zio task in the same simulation, speaking HTTP/2 through
  `src/h2_client.zig`'s builders, with `tests/h2_oracle.zig` checking every
  frame it receives (do not write a second oracle).
- Each handler records its terminal call's result at return, before anything
  can suspend (the shape of `874ce91` on `carmack/dst-witness`): not returned,
  ok, or the error. Never classify "not returned" as an error.
- The per-stream rule, judged after the run settles:
  - clean history (no injected fault, no reset, no shutdown, peer reads to the
    end): `send` ok and the peer saw the complete response with END_STREAM;
  - a ticket that settled ok must make the wait return ok, even if a cancel or
    a close arrives before the handler resumes;
  - an error must match an injected failure or a terminal cause;
  - "send returned an error, peer saw the handler's complete response" is
    forbidden, EXCEPT when an injected write or flush failure hit a batch that
    held that response (the h2c pump fails the whole batch today,
    `wire_pump.zig:576-590`; task t-3794 will change that, and its fix will
    remove this exception).
- Two phases per run (TigerBeetle VOPR shape): a safety phase with faults,
  then faults stop, the peer reads everything, cooperative handlers are
  released, and every stream, handler, ticket and slot must retire by a
  simulated deadline. A stall the injected faults do not explain fails.
- After every run: the connection's existing `deinit` conservation checks, and
  `sim` deadlock detection.

## Workload and faults

Seeded, swarm style (each seed enables a subset): streams 1-8; empty, small and
multi-frame bodies; window credit 0, 1 byte and small increments; peer resets
(RST_STREAM) at chosen points; peer half-close and close; `requestShutdown`
mid-flight; injected short writes and write errors at the sim pipe. Fault-rate
floor: print per-run counts of each fault and mark, and fail a sweep in which a
configured fault or a required mark never fired. Mark at least: a ticket
settled ok before a cancel arrived; a cancel arrived while a send waited; a
reset raced a response; teardown with a handler still running.

## Validity

- The mutant: swap the two checks at `src/edge/ticket_table.zig:305-306` (the
  pre-t-537 order, cause before ok), as a compiled-out flag. The ci step must
  build it and require the per-stream rule to fail on a named seed, with the
  same failure on a replay of that seed.
- A second mutant of your choice in the pumps or the actor that the oracle must
  catch; read why it fails.

## ci and evidence

- `./zb build dst-conn`: regression-seed file first, then seeds from the commit
  hash plus a fixed range, to a budget of about 60 s, with a completed-seed
  floor; determinism checked by running a sample of seeds twice and comparing
  trace hashes. Wire it into `ci` once its wall time is measured on both
  machines.
- Every failure prints seed, build identity, invariant, trace hash and the
  one-line replay command.
- Scope line, printed by the step: what is simulated, and that real threads,
  work stealing, TLS and HTTP/1.1 are not.
- Time limits on every build and run; fresh `--cache-dir` per binary; run
  binaries directly to repeat. nachos only in `~/src/dst-zio-port`. Never write
  the shared Zig global cache by hand. Do not tell the orchestrator the tests
  pass; the orchestrator recomputes.

## Commits and report

One or more commits, each with a decision-record message. Never push. Append a
"Stage 3" section to `tools/dst-port-report.md`: commits, results, the
mutation table, marks reached per sweep, wall time on both machines, every
place the plan did not fit, and anything not measured labelled as inference.
Stop and report on any design question.
