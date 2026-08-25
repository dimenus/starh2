# Brief: producer-claims protocol for zio's CompletionQueue under select

One-shot work brief for a fresh session (task references this file; the
file is the record). Deliverable: a PR-ready branch on dimenus/zio that
implements the protocol the zio maintainer sketched on upstream PR #712,
plus its evidence. The maintainer's sketch is the DESIGN CONSTRAINT:

> "the producer claiming a specific completion to a specific waiter
> before signaling, so a wake always carries an item nobody else can
> take, plus reworking close/cancelAll/cancel around that."

Deviations from that shape are findings to report back, not decisions to
make. The PR must be reviewable by the maintainer in his own style; he
rewrote our previous protocol (#702 -> his #709), and that is fine if
the tests ride along.

## Context (read these before any code)

- Upstream PR #712 thread (gh pr view 712 --repo lalinsky/zio --comments):
  the bug, the maintainer's single-driver walk, his two-driver
  counterexample, and the exact interleaving that breaks the current
  protocol (ownerCallback publishes to `completed` and wakes in two
  non-atomic halves; the driver's own `next()` consumes in the gap; the
  stale wake claims a later select's arm).
- The reproducer: `examples/cq_spurious_select_repro.zig` on the branch
  `fix/cq-spurious-select-win` (dimenus/zio).
- The gate: `tools/zio-pin-gate/gate.sh` in starh2 (this repo).
- starh2 memory: `zio-select-conservation` (our #702-era producer-claims
  prior art: commits 84bb04c, b4fabaa on old fork history).

## Tripwires — run FIRST, stop and report on any mismatch

1. Base: branch from upstream 4f831ea
   (`git merge-base --is-ancestor 4f831ea HEAD` on your branch; the
   branch must rebase clean onto upstream main).
2. `git -C <zio> diff 4f831ea --stat -- examples/cq_spurious_select_repro.zig`
   must stay EMPTY on your branch relative to the repro as it exists on
   `fix/cq-spurious-select-win`. The reproducer and the two conservation
   repros in starh2 (`tools/select-cancel-repro/`, `tools/cq-spurious-repro/`)
   are READ-ONLY fixtures. Any edit to them is an automatic fail, however
   good the reason looks. If a fixture seems wrong, that is a finding.
3. Blast radius: changes limited to `src/completion_queue.zig`,
   `src/select.zig`, `src/common.zig`, `src/sync/Futex.zig`,
   `src/ev/completion.zig`, plus NEW test files. A needed change anywhere
   else is a report-first, not an edit.

## Acceptance — all axes at once, any one fails the build

Every claim below must cite the log path that proves it, and the
acceptor re-runs the gate independently; self-reported numbers are
never accepted.

A. **Race closed** (the point): `zig build examples
   -Dexample=cq-spurious-select-repro` then 10 consecutive runs of
   `cq-spurious-select-repro 8 20000 64` exit 0, on BOTH
   macOS/kqueue and Linux/io_uring (nachos), in BOTH Debug and
   ReleaseSafe. Baseline for contrast: on 4f831ea the same runs abort
   in under a second.

B. **Misuse still caught** (paired axis; closing our race must not
   legalize the maintainer's): write a two-drivers-one-queue test in
   his PR-comment shape. It must FAIL LOUDLY (error or assert) under
   the new protocol. If the new protocol makes multi-driver silently
   "work", that is a design deviation to report, not a bonus.

C. **Conservation kept**: `select-cancel-repro` (200k iterations) and
   `select-clobber-repro` (2M) clean against the branch.

D. **Suite green without test surgery**: `zig build test` all pass.
   No existing test's assertions may change; a test that the new
   protocol genuinely obsoletes is a report-first. NEW tests are
   required for at least: claim-racing-close, claim-racing-cancelAll,
   claim-to-a-waiter-whose-select-was-canceled (the delivered-item
   path), and the teardown drain (close/cancelAll/next-to-null must
   guarantee callback quiescence — the unlock-to-wake UAF window named
   in the #712 thread is in scope: close it or document precisely why
   it is out of scope for this PR).

E. **End-to-end**: `tools/zio-pin-gate/gate.sh <clone> <your-sha>`
   prints **PASS-CLEAN** (not PASS-WITH-WARN: the WARN tier exists
   only for the tolerance-mitigation era). This runs h2spec, the
   collapse probe on nachos, everything.

F. **Perf floor** (the degenerate-optimum guard: a global lock around
   publish+wake+claim passes A-E and kills throughput):
   `tools/zio-arm-ab.sh` with two arms — the current starh2 pin and
   your branch — on nachos. Candidate must hold: oneshot-e2 and
   oneshot-e8 medians >= 97% of the current pin's, sse500 p50 <=
   1.25x the current pin's, in the same run. Report the summary table.

The laziest output that passes A alone is serializing everything; F
kills it. The laziest that passes A+F is weakening a test; D's
no-test-surgery rule kills it. The laziest that passes A+D+F is
"multi-driver now silently allowed"; B kills it.

## Deliverable shape

- Branch `feat/cq-producer-claims` on dimenus/zio, based on 4f831ea.
- Commit structure: protocol change; new tests; (optional) teardown
  fix. House commit-message format (problem / change / alternatives /
  verification with the numbers).
- A report back containing: every acceptance letter with its log path,
  the design deviations list (empty is a valid answer), and what the
  next reviewer should attack first (per the fixes-are-the-next-bugs
  rule).
- Do NOT open the upstream PR — the maintainer conversation stays with
  the main session; the branch and the evidence are the deliverable.
