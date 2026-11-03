# Review brief: DST stage 0 and 1 (874ce91, fd72bd5)

Review two commits on branch `carmack/dst-witness` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-dst`:

- `874ce91`: t3567's idle table judges the handler's own recorded result
  instead of a counter read before the handler resumed (test only).
- `fd72bd5`: the HTTP/1.1 TLS transport witness: `h1.H1Tls` (shared production
  construction of the pump), a submission seam `Pump.submitOp` (observe builds
  only; `void` in release), `tests/dst_tls_lifecycle.zig` (enumerated and
  seeded driver, oracle, floors), `tools/dst/transport.sh` and the
  `dst-transport` ci step.

Read the committed objects (`git show <sha>`, `git show <sha>:<path>`), not the
working tree. Read both commit messages in full. The implementer's brief is
`tools/dst-stage1-brief.md` (with its Handoff section). The design debate that
chose this plan is in `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/`
(`dst-brainstorm-*.md`, answers in `dst-r2/`, `dst-r3/`).

## Premises (settled; findings that depend on their negation are out of scope)

- No external users; breaking changes are free.
- No scheduling hooks or pause points in the connection actor. A transport
  submission seam that is `void` outside test builds is accepted.
- This witness proves only that the HTTP/1.1 TLS transport cannot regain an
  unread failure channel. Not modelled, by decision: a receive that delivers
  data, encrypt failure, oversized records, HTTP/2, real threads.

## What to look for

Correctness defects only:
- **Production changes.** The `H1Tls` refactor of `H1Conn` and the `tls.zig`
  changes: any behaviour change for a real connection (teardown order,
  `failDrain` timing, a buffer freed while the pump can reach it, the
  final-address rule of `Pump.start`, the CQ shutdown order). Anything the
  seam changes in a release build.
- **A witness that cannot fail.** Any invariant that is vacuous, checked at
  the wrong moment, or satisfied by the driver itself rather than by the pump;
  legality rules that exclude a sequence the real `H1Conn` can produce; a
  floor that a broken pump could still meet; a replay check that compares the
  wrong thing.
- **The t3567 change.** Does the record really happen before anything that can
  suspend? Can "not returned" ever be counted as success or as an error?
- **ci.** Can `dst-transport` pass when it ran nothing, or when the mutant
  failed for a different reason?

Label every finding **[LOGIC]** (holds regardless of deployment) or
**[PREMISE]** (name the deployment fact it rests on). Unlabelled findings are
discarded. For each: file:line in the commit, a concrete failure scenario, and
the quoted code. Say "no finding" for each area you checked and found sound.
