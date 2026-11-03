# Brainstorm brief: deterministic simulation testing for starh2, TigerStyle

You are one of three independent designers (different vendors). The owner wants
proposals for how starh2 can find timing bugs (bugs that depend on the order
in which tasks, I/O completions, timers and threads interleave) on purpose,
reproducibly, instead of by luck. Think in the style of TigerBeetle's VOPR and
"TigerStyle": everything nondeterministic behind one seam, a seeded PRNG drives
every choice, time is simulated and compressed, faults are injected, and every
failure replays from its seed.

## Read first

- This repo: `/Users/rsaunderson/Source/mine/starh2-wt-3737` (branch
  `carmack/t-3737`). `AGENTS.md` first. Then `src/edge/server.zig`,
  `src/edge/connection.zig`, `src/edge/h1.zig`, `src/edge/tls.zig`,
  `src/edge/gate.zig` if present, and the test harness pieces under `tests/`
  (`wire_client.zig`, `serve_deadline.zig`, the h2 oracle). Note how the code
  reaches the outside world: `self.io` (Zig 0.17 `std.Io`, a vtable), zio
  primitives (tasks, channels, select, timers, events), sockets.
- The concurrency runtime: zio, vendored under `zig-pkg/zio-*/src`
  (executors, work stealing, the completion queue, io_uring/kqueue backends).
- A simulator already started: `tools/zio-dst` in this repo (its own
  `build.zig`, sources under `tools/zio-dst/src`), and `git log -- tools/zio-dst`
  for why it exists. Read what it does and does not cover before you propose
  anything. If you cannot open it, say so plainly; do not guess its contents.

Quote at least one line from each of the three places above that only the
code shows, so the owner can see you read them.

## Premises (settled; ideas that depend on their negation are out of scope)

- No external users. Breaking changes are free. Rework to get the primitive
  right is acceptable cost.
- zio owns concurrency in starh2. `std.Io` concurrency types (Event, Queue,
  Group, Future, Select, Mutex) are banned by a ci gate; `std.Io` the vtable is
  fine. The owner maintains a fork of zio and can change it.
- Owner's rule: grow the harness and the oracles (a transport seam, the h2
  oracle), not test hooks inside the server's actor code.
- Every learning should end in a tool that cannot forget: a check that fails
  the build beats a document.
- Production runs multi-threaded (work stealing across executors) on Linux
  (io_uring) and macOS (kqueue). A simulator that only covers one thread is
  still useful, but say what it cannot find.

## The bugs this must catch (the evidence)

Each of these was found late, by luck or by a human reading code:

1. A test fails in about 1 of 3 full-ci runs on Linux and never alone (0 of
   80 under 8 parallel copies, 0 of 30 under 24-core CPU load): an h2c task
   handler's `send` of an empty-body response; the client sees the 204 and
   END_STREAM, but the handler's `send` apparently returns an error. Inference:
   a race between the send's completion and a cancel posted when the stream
   ends.
2. A segfault once in about 54 full-ci runs, 0 in 600 isolated runs: zio
   `registerTask` reads `current_executor.runtime` from invalid memory while
   `Server.acceptLoop` spawns a connection task (server.zig ~853), apparently
   during test teardown.
3. A process-wide panic reachable by one HTTPS client that resets at the right
   moment: acks posted into a 4-slot channel nobody read. Found only by a
   reviewer reading code; reproduced afterwards with 40 resetting clients.
4. Earlier history (see git log): a lost wake with ciphertext stranded in a
   BIO; zio `select` dropping a claimed item on cancel; zio timers backdated or
   early on a busy loop; a TLS stall needing lldb on live state.

## What to propose

1. **The seam.** Where should nondeterminism be cut so one seed decides every
   interleaving: a simulated `std.Io` implementation, a simulated zio backend
   (completion queue / event loop), a simulated scheduler, a simulated network
   (loss, delay, reorder, partial writes, resets), simulated time, or a
   combination? What does each cut make reproducible, and what stays outside
   (real threads, atomics, OS behaviour)?
2. **The oracle.** A simulator without strong invariants finds only crashes.
   Which invariants should run on every step (accounting balances, every
   handler joined or orphaned exactly once, no slot reused while reachable,
   bytes the client saw equal bytes the handler sent, liveness: everything
   ends by a simulated deadline)? Where do they live so they cannot be skipped?
3. **The workload.** Swarm testing, fault mixes, client behaviours (slow
   readers, resets, half-close, pipelining, shutdown mid-flight), and how a
   seed maps to a scenario.
4. **Multi-thread coverage.** How to find bugs that need real parallelism
   (bug 2 above) when a deterministic simulator runs one thread. Options
   include a deterministic multi-executor scheduler, PCT-style priority
   schedules, or a separate jitter mode on real threads (random yields at zio
   suspend points). Say which you would build and why.
5. **A staged plan.** The first stage must be small (days, not weeks) and must
   be able to find at least one of bugs 1-3 above. For each stage: what it
   costs, what it would have caught from the list, and how it runs in ci
   (time budget, seeds per run, how a failure is reported and replayed).
6. **Your pick.** Recommend one plan decisively. Then say what your own plan
   understates or gets wrong, and what the other likely plans (a pure jitter
   mode; a full VOPR-style rewrite behind a simulated Io) understate.

Label every claim about this code or zio **[CODE]** with file:line, and every
claim about what a technique would find **[INFERENCE]**. Unlabelled claims are
discarded. Plain English, short sentences. No code changes: this is design
only.
