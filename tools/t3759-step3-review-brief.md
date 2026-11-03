# Review brief: t-3759 step 3 (8eb220f)

Review `8eb220f` on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`. Its parent is `d33e615`.
Read the committed objects (`git show 8eb220f`, `git show 8eb220f:<path>`), not
the working tree. Read the commit message in full: it is the decision record.
The design is `tools/hardening-design.md` (v3), sections "The rule everything
follows", "The client table", "Client identity (Q1)". The implementer's brief
is `tools/t3759-step3-brief.md`. Read zio source under `zig-pkg/zio-*/src`
before any claim about zio.

## Premises (settled; findings that depend on their negation are out of scope)

- No external users; breaking changes are free.
- This step CHARGES every resource to connection, client, aggregate and server
  accounts; it does not REFUSE by share (step 4). Rate buckets and rate-only
  records are step 4; route accounts are step 5.
- zio is cooperative; a handler that never yields cannot be stopped. `serve`
  never returns while a handler runs.
- Identity rules are the design's Q1 section exactly (IPv4 per address, IPv6
  /64 plus a /48 aggregate, PROXY v2 only with trusted sources, `.none` on Unix
  sockets).

## What to look for

Correctness defects only:
- **Conservation.** Every charge released exactly once on every path: refusal
  undo, orphaned handlers, cancelled `serve`, startup failure, TLS handshake
  failure, PROXY header failure, slot quarantine, HTTP/1.1 and HTTP/2. A record
  freed or reused (generation bump) while any holder can still release to it.
- **The table's concurrency.** Lookup-or-insert and release-to-zero under the
  mutex; lock-free charges on a referenced record; backward-shift deletion;
  the generation check. A race where two holders see one record removed and
  re-inserted, or a charge lands on a record that was just reused.
- **The admission deadlock fix** (`admission_mu`, `wakeAdmission`): is the fix
  complete on every path that releases while holding the mutex? Can the accept
  loop now miss a wake and park for ever?
- **PROXY v2 parsing**: bounds, the size cap, a header split across reads, v4
  and v6 address extraction, `LOCAL`, a trusted source check that can be
  bypassed.
- **Identity keys**: v4-mapped addresses, /64 and /48 masking, `unknown`.
- **The `.peer = config.peer` change**: the request's peer now comes from the
  connection instead of a `getpeername` per request. Any case where they
  differ (PROXY endpoints: which address must a handler see?).
- **Tests that would pass with the defence removed.**

Label every finding **[LOGIC]** or **[PREMISE]** (name the deployment fact).
Unlabelled findings are discarded. For each: file:line in the commit, a
concrete failure scenario, and the quoted code. Say "no finding" for each area
you checked and found sound.
