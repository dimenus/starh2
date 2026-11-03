# Brief: DST stage 4, a ThreadSanitizer lane for real-thread races

The simulator (stages 2-3) runs one OS thread and cannot run work stealing, so
it cannot find races that need real parallelism. The zio `registerTask`
use-after-free (lalinsky/zio#875, fixed on our fork in `2f727e3`) was one: it
showed up once in about 54 full ci runs as a segfault. ThreadSanitizer would
report that class the first time the bad order happens, crash or not. Build
the lane that does that.

Worktree `/Users/rsaunderson/Source/mine/starh2-wt-dst-tsan`, branch
`carmack/dst-tsan`, based on `9a73c51` (zio pin `2f727e3`, the fix). Tripwire:
HEAD is the commit adding this brief, first parent `9a73c51`. If not, STOP.
Never touch other worktrees or branches; never push. Reviewers' findings reach
you only through the orchestrator; do not message any other agent.

## Read first

`AGENTS.md`; the DST debate's agreed plan
(`/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/dst-r3/`, each answer's
section on bug 2 and the native lane: a TSan arm needs a known-positive
control, because zio's TSan fiber glue may crash on a fiber that never ran);
the bug-2 report `/Users/rsaunderson/Source/mine/starh2-wt-dst/tools/bug2-core-report.md`;
zio's TSan hooks (`zig-pkg/zio-*/src/coro/tsan.zig`) and how
`-fsanitize-thread` is wired in Zig 0.17 (`~/.claude/skills/zig/SKILL.md`,
then the Zig source).

## Build

1. **A positive control first.** The zio-only churn reproducer from the bug-2
   investigation is on nachos at
   `~/src/dst-witness/bug2/tree-stock/tests/bug2_churn.zig` (2 executors, 1M
   group spawns, 640-byte contexts; copy it, do not modify that directory).
   Build it with TSan against zio `182b0f6` (before the fix) and show TSan
   reports the race on `task.runtime` (a data race on freed memory or a
   use-after-free report, with the stacks of the read in `registerTask` and of
   the free). Then against `2f727e3` (the fix): no report. If TSan cannot run
   zio at all (fiber glue crash, missing runtime), STOP and report what you
   measured; do not work around it quietly.
2. **The lane**: a build step (`./zb build tsan`, not in `ci`) that builds the
   starh2 test binaries that exercise concurrency (at least goaway_delivery,
   shutdown, lifecycle, slot_reuse, accounts, admission, placement, h1_battery)
   and the bug-2 churn test with `-fsanitize-thread`, runs them with a time
   limit, and fails on any TSan report. Suppressions only with a written
   reason per entry, in a committed file, and each one shown to be a true
   false positive (cite the happens-before edge TSan cannot see).
3. **Triage what it finds** on the current tree. For each report: a real race
   (file it as a task, `--for starh2` or `--for zio`, with the TSan output) or
   a false positive (suppression with the reason). Do not fix production races
   in this stage unless the fix is one line and obviously right; report them.

## Evidence

- nachos only in a new directory `~/src/dst-tsan` (`export
  PATH="$HOME/.local/share/mise/shims:$PATH"` first). TSan is Linux-first; say
  whether the Mac works and do not force it.
- The positive control's result before and after the fix, with TSan's output.
- The lane's wall time, the number of binaries and runs, and every report with
  its triage.
- Time limits on every build and run; fresh `--cache-dir` per binary; never
  `rm -rf` a path built from variables; never write the shared Zig cache by
  hand. Do not tell the orchestrator anything passes; the orchestrator
  recomputes.

## Report

`tools/dst-stage4-report.md` (not committed). Commits with decision-record
messages. Stop and report on any design question.
