# Brief: DST stage 2, port the zio simulator onto starh2's zio pin

You are the implementer of the port track. Two worktrees are yours:

- zio: `/Users/rsaunderson/Source/mine/zio-wt-sim-port`, branch `sim/dst-m2`,
  based on `dbf1beb` (starh2's current zio pin, from branch
  `fix/iouring-send-eagain`).
- starh2: `/Users/rsaunderson/Source/mine/starh2-wt-dst-port`, branch
  `carmack/dst-zio-port`, based on `d33e615` (HEAD is the commit adding this
  brief), for repointing `tools/zio-dst` and its ci step only.

Tripwire before any work: `git -C <zio wt> rev-parse HEAD` prints `dbf1beb...`
and `git -C <starh2 wt> rev-parse HEAD~1` prints `d33e615...`. If not, STOP.
Never touch any other worktree, branch or the main zio clone's checked-out
branch. Never push. A second implementer builds a transport witness in
`starh2-wt-dst`; you do not talk to it. Reviewers' findings reach you only
through the orchestrator. Do not prompt any other agent.

## What exists

- The simulator: 20 commits on `dimenus/sim/dst-m1`, tip `913ffda` (also merged
  into `dimenus/main` at `cd0d5fc`). Merge base with the pin: `5124ea7`. Since
  that base: sim branch 20 commits, 27 files, +1,969/-103; pin 157 commits,
  97 files, +10,966/-1,981.
- The harness: `tools/zio-dst` in starh2 (its own `build.zig`, pins `913ffda`),
  with checks D1-D5 (determinism over 20 seeds twice, at least 100 distinct
  traces over 1000 seeds, mutants found, real clock/futex/backend poll panic,
  a scope report with no empty unsimulated line). Read `git log --
  tools/zio-dst` for why.

Read first: `AGENTS.md`; the debate that chose this plan,
`/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/dst-brainstorm-brief.md`
and the answers in `.../tools/dst-r1/`, `dst-r2/`, `dst-r3/` (above all
section 2 of each round-3 answer, which names the conflict sites). For Zig 0.17
read `~/.claude/skills/zig/SKILL.md`.

## Premises (settled)

- Move the 20 sim commits onto `dbf1beb`. Do not move starh2 back onto the old
  sim line (it would lose 157 commits of runtime fixes). Do not rebuild the
  simulator from scratch (the coop-yield points and the mutants encode real
  bugs).
- This is a semantic transplant, not a merge measured by compiling. Where the
  pin changed a mechanism, keep the PIN's behaviour and re-fit the sim hook to
  it. The round-3 answers name the sites; at least:
  - `zio_options`: the sim imports `@import("zio_options")` as build options;
    on the pin that name is root-declared (`src/options.zig`). Give `sim` and
    `sim_mutant` a home in the pin's options shape.
  - `Loop.poll`: the pin announces `sleeping`, polls, then takes requests that
    arrived during the poll. Keep that ordering when re-splitting the sim's
    `waitForEvents`.
  - `Runtime` init/shutdown: the pin has start and stop barriers
    (`startWorkers`, `workers_stopped`); the sim's logical-worker init and
    teardown must respect them.
  - `CompletionQueue`: the sim routes every driver-entry consumer through one
    function and has a source-invariant test for it; the pin still has
    separate sites. Port the ownership rule and its test, not just the yield.
  - `getNextTask`: keep the pin's pinned-overflow refill; add the seeded pick.
  - `ResetEvent` vs the pin's merged `Event`.
- Keep the executor cap at 1-2. Raising it is out of scope.
- Never randomise which ready select arm wins (zio's documented order,
  `select.zig:262-263`). Randomise readiness timing and task picks only.
- Native (non-sim) builds must be unchanged in behaviour: every sim branch is
  `if (comptime ...sim)`.

## Done means

1. On `sim/dst-m2`: the pin's own zio test suite passes natively (Mac and
   nachos) with sim off, unchanged from `dbf1beb`'s result (measure
   `dbf1beb` first and report both).
2. `tools/zio-dst` repointed at your zio commit (a local path or `git+file`
   URL during development; say which), and D1-D5 green with sim on, plus every
   existing mutant (`arm_timer_stale` and the others in the harness) FOUND.
   A clean mutant sweep is a fail.
3. A ci step in starh2 (`./zb build dst-zio`) that runs D1-D5 and the mutants
   in under about 60 s, not yet wired into `ci` (the orchestrator decides after
   measuring).
4. Your commits squashed into a few reviewable commits on `sim/dst-m2`, each
   with a decision-record message: what changed vs `913ffda`, which pin
   mechanism each hook was re-fitted to and why.

**Time it.** Record the hours spent per conflict site in the report. The
designers estimated 5-10 days; the measurement is part of the deliverable.

## Gates and evidence

- Wrap every build and run in a time limit (`perl -e 'alarm shift; exec @ARGV'
  <s> ...` on the Mac, `timeout` on nachos). Fresh `--cache-dir` per binary;
  run binaries directly to repeat (`zig build` caches passing runs).
- nachos only in a new directory `~/src/dst-zio-port` (`export
  PATH="$HOME/.local/share/mise/shims:$PATH"` first). Touch nothing else,
  change no system setting, kill only your own PIDs.
- Mutation-test any new check by breaking the CODE, and read why it fails.
- Do not tell the orchestrator the tests pass; the orchestrator recomputes.

## Report

`/Users/rsaunderson/Source/mine/starh2-wt-dst-port/tools/dst-port-report.md`
(not committed): commits, the native suite result on `dbf1beb` and on your tip
(both machines), D1-D5 and mutant results, time per conflict site, every
decision where the pin and the sim disagreed and what you chose, and what
remains unsimulated. Label anything not measured as inference.
