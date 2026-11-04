# Brief: carry the DST branches into the t-3759 series

The t-3759 series on `carmack/t-3737` (steps 1-4, 7, t-3763, t-3831) and three
side branches were built in parallel from `d33e615`. The side branches hold
fixes and gates the series must have before it lands. Carry them in, as a
bisectable series, every commit green on its own.

Worktree `/Users/rsaunderson/Source/mine/starh2-wt-3737`, branch
`carmack/t-3737`. Tripwire: going down first parents from HEAD you reach
`9d21242` (t-3831) with only brief commits ("Add the ...") between. If not,
STOP. Never touch the side branches themselves or their worktrees; read them
with `git show` / `git log`; cherry-pick from them. Never push. Untracked
`tools/dst-r*/`, `tools/pcap-r1/` and report files are not yours.

## What to carry, in this order (on top of 9d21242, below the brief commits)

1. **The zio pin to `5d4b0a5`** (dimenus/zio `sim/dst-m2`, pushed): the zio
   races the TSan lane found (t-3832 union copy, t-3836 work-stealing ring,
   t-3835 sockreg double completion). Root `build.zig.zon`; the hash is in
   `0bfb1aa` on `carmack/dst-tsan`. One commit; its message cites `0bfb1aa`
   and the three zio commits.
2. **From `carmack/dst-witness`**, in order: `874ce91` (t3567 judges the
   handler's own result), `418fdef` (the HTTP/1.1 TLS transport witness,
   `H1Tls`, `Pump.stopped`, `dst-transport` in ci), `8a8d4e9` (t-3794: h2c
   settles each chunk by accepted bytes), `e8055fd` (t-3807 tests).
   `418fdef` and steps 3/7 both changed `src/edge/h1.zig`: expect conflicts.
   Resolve them so both intents hold (the H1Tls construction and teardown
   ownership AND step 3's accounts / endpoint fields / step 7's links), and
   say in each commit message what you resolved and how.
3. **From `carmack/dst-zio-port`**: `2ac4128` + `aa42b62` (`tools/zio-dst`
   repointed and `dst-zio` in ci; point `tools/zio-dst` at `5d4b0a5` too, not
   at the older commit those name), `17cc1d5` (GOAWAY on shutdown during the
   h2c preface wait), `d84780e` + `21ccadc` + `dffd776` (`dst-conn` and its
   regression seed, in ci). Skip that branch's pin commits (`4ee9096`,
   `f0ee898`, `9a73c51`): the series pins past them.
4. **From `carmack/dst-tsan`**: `a7bb805` (the TSan lane), `f686a27`
   (`slotIndex` reads slots through a pointer), `7d99329` (Mac report
   grouping). Skip its pin commit (item 1 replaces it).
5. **The brief commits** of the three branches go on top with the series'
   own briefs, unchanged, so the record is in one history.

Each carried commit keeps its author, date and message; add a short
"Carried into the series" note to a message only when you had to change the
code to resolve a conflict.

## Make the TSan lane green

After item 4, `./zb build tsan` on nachos must report nothing. Known:
- t-3831 is fixed in `9d21242` (verify it stays gone).
- **t-3844**: a zio thread-pool worker never joined (`h1_battery`, via
  `dirReadImpl` -> `submitFileOpToThreadPool`). Decide from zio's source
  whether this is a zio defect (the pool does not join or detach its workers
  on deinit) or the test's runtime teardown. If zio: fix it on `sim/dst-m2`
  in `/Users/rsaunderson/Source/mine/zio-wt-sim-port` with a zio test that
  fails first, push the branch, move the pin (a commit after item 1's, or fold
  into item 1 if nothing depends on the order). If the test: fix the test.
- Anything else the lane reports: STOP and report it with its triage; no
  suppressions without a written happens-before edge.

## Gates

- After every carried commit: it builds (`./zb build` and the test binaries)
  on the Mac. Bisectability is the point; say if a commit had to be split.
- At the end: Mac and nachos `./zb build ci` once each on `git archive`
  exports (ci now runs `dst-transport`, `dst-zio`, `dst-conn`); the TSan lane 3
  runs on nachos, zero reports; t3567 direct 5x per machine.
- h2c, TLS, h1c bench pairs (6 each) on nachos and the Mac against
  `9d21242`, `failed == 0` first.
- nachos only in a new `~/src/s-3759-integrate`. Time limits; fresh
  `--cache-dir`; never `rm -rf` a path built from variables (not even one that
  does not exist yet); never write the shared Zig cache by hand. Do not tell
  the orchestrator anything passes.

## Report

`tools/integrate-report.md` (not committed): the final commit list with each
carried commit's source hash, every conflict and its resolution, the lane runs,
ci, bench. Stop and report on any design question.
