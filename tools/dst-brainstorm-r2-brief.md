# Brainstorm brief, round 2: deterministic simulation for starh2

Round 1 is `tools/dst-brainstorm-brief.md`. Its premises, the three bugs and the
labelling rule ([CODE] with file:line, [INFERENCE] otherwise; unlabelled claims
are discarded) are unchanged. Repo: `/Users/rsaunderson/Source/mine/starh2-wt-3737`.

## New inputs (in `tools/dst-r1/`, untracked, read them all)

- The three round-1 answers: `dst-fable.md`, `dst-astra.md`, `dst-wonder.md`.
  One of them is yours. Read the other two in full.
- `tigerbeetle-survey.md`: a source-cited survey of TigerBeetle's VOPR and
  fuzzers (clone at `/Users/rsaunderson/Source/oss/tigerbeetle`, commit
  0450c32b2; open it to check any claim you lean on).
- `zio-sim-913ffda/`: the zio simulator source at `913ffda` (branch
  `sim/dst-m1` on dimenus/zio), which nobody could read in round 1. Start with
  `src/sim.zig`, `src/runtime.zig`, `src/ev/`.

## Facts measured since round 1

- starh2 pins zio `dbf1beb`. The sim branch tip is `913ffda`. Their merge base
  is `5124ea7`. Since that base, the sim branch has 20 commits (27 files,
  +1,969 / -103 lines); starh2's pin has 157 commits (97 files, +10,966 /
  -1,981 lines). Measured with `git log` and `git diff --stat` in the zio clone.
- Bug 2 (the `registerTask` segfault) did not reproduce in 600 isolated runs
  of its test binary (200 on each of three trees, 8 in parallel). Bug 1 did not
  reproduce in 30 runs under 24-core CPU load (stress-ng), nor in 8 full-ci
  runs with error logging added (expected about 2-3 at the earlier rate).
- In bug 1's test, a 300 ms sleep added after `resp.send` returned in under
  1 ms: the handler is cancelled right after its response ends.

## What to answer

1. **Revise or defend your first stage** against the other two answers and
   against TigerBeetle. The owner wants a first stage of days, with a validity
   mutant that proves it fires. Say what you changed and why.
2. **The disagreements.** Take a side on each, with reasons:
   - Seam: a seeded schedule mode on real threads first, a single-thread sim
     backend first, or a harness that drives one connection's events first?
   - The bug-1 oracle: Fable proposes "send returned ok iff the peer saw
     END_STREAM"; astra says socket acceptance is not delivery and wants a set
     of allowed histories under resets. What exact rule, and for which
     scenarios?
   - Bug 2: ordering bug (a sim can catch it) or data race (only TSan,
     real-thread stress, or a model check of spawn/steal/teardown)? What
     evidence would decide it cheaply?
3. **What TigerBeetle changes in your plan.** Name the techniques you adopt
   (e.g. two-phase safety-then-liveness runs, fault-rate floors, coverage
   marks, seed = commit hash, exhaustigen-style small-bound enumeration, a
   completion-level fake IO under the real transport) and the ones you reject,
   each with a reason specific to starh2.
4. **The port.** Having read the sim source and the measured divergence: port
   the sim onto `dbf1beb`, rebase starh2 onto the sim line, or rebuild the sim
   on current zio from scratch? Estimate in days and say what the estimate
   rests on.
5. **Final pick.** One staged plan, the first stage concrete enough to brief an
   implementer: files touched, the ci step, the seeds and time budget, the
   validity mutant, and the bug it must catch.

Plain English, short sentences. Design only: no code changes.
