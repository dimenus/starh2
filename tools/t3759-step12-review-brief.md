# Review brief: t-3759 steps 1 and 2 (af05327, 8370b4d)

Review two commits on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`:

- `af05327` step 1: request body clock at HEADERS, minimum body and response
  rates, accept loop never exits on an error, RLIMIT_NOFILE boot check.
- `8370b4d` step 2: `serve` never returns while a handler still runs and
  reports what it waits on; `Server.deinit` and `GlobalAccounting` checks run
  in every build; per-test `serve` deadline helper.

Read the committed objects (`git show <sha>`, `git show <sha>:<path>`), not the
working tree. The design is `tools/hardening-design.md` (v3, at 686d695); the
implementer's brief is `tools/t3759-step12-brief.md`. Read the commit messages:
they hold the decision record. Read dependency source under `zig-pkg/zio-*/src`
before making a claim about zio.

## Premises (settled; findings that depend on their negation are out of scope)

- No external users. Breaking changes are free; no compatibility shims.
- zio owns concurrency. `std.Io` concurrency types are banned by a gate.
- zio is cooperative: `AnyTask.cancel` sets a flag and wakes; a handler that
  never checks cancel cannot be stopped. `serve` must never return while a
  handler still runs (owner's decision), and must log what it waits on.
- Shares, per-client accounting and the HTTP/1.1 link model are later steps.
  Do not report their absence.

## What to look for

Correctness defects only: a deadline that can fire on an honest client (above
all a sparse SSE stream, a slow but steady upload, a body paused because the
server owes credit), a deadline that an attacker can keep from firing, a clock
that is never cleared or never armed on some path, a shutdown path where `serve`
can return while a handler runs or can wait without logging, a `deinit` path
that frees memory a live handler can reach, a counter that can still wrap, an
accept error path that can spin or exit, and tests that would pass with the
defence removed.

Label every finding **[LOGIC]** (holds regardless of deployment) or
**[PREMISE]** (name the deployment fact it rests on). Unlabelled findings are
discarded. For each, give file:line in the commit, a concrete failure scenario,
and quote the code that shows it. Say "no finding" for an area you checked and
found sound, so coverage is visible.
