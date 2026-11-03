# Review brief: t-3759 step 7, HTTP/1.1 onto links (f206fd3)

Review one commit, `f206fd3`, on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`. Its parent is `6ea2fcf`.
Read the committed objects (`git show f206fd3`, `git show f206fd3:<path>`), not
the working tree. Read the commit message in full: it holds the decision
record, including every cross-scope pointer it keeps and why. The brief the
implementer worked from is `tools/t3759-h1links-brief.md`. Read zio source
under `zig-pkg/zio-*/src` before any claim about zio.

## Premises (settled; findings that depend on their negation are out of scope)

- No external users. Breaking changes are free; no compatibility shims.
- zio owns concurrency; `std.Io` concurrency types are banned by a gate.
- zio is cooperative; a handler that never checks cancel cannot be stopped.
  A connection must never wait for such a handler. `serve` must never return
  while a handler still runs, and must log what it waits on.
- Owner's lifetime rule: a handler's world is its own per-request arena.
  Nothing a handler holds may point into connection-owned memory, except
  through the gate while the gate is open.
- Steps 1 and 2 (deadlines, write-byte timeout, serve waits for handlers)
  were reviewed in four rounds and are out of scope except where this commit
  changes them. Accounts, shares and route shares are later steps.

## What to look for

Correctness defects only:
- **Teardown races.** A handler entering the gate while it closes; a handler
  that reports just as teardown starts (join or orphan: can it be both, or
  neither?); the orphan count and the task-handler token on every exit path,
  including TLS, startup failure and a cancelled `serve`. Can a slot be reused
  while a handler can still reach it?
- **Lifetime.** Any pointer a handler can still follow into connection memory
  after the gate closes: request fields, headers, body, trailers, the TLS
  write channels, the response writer, a flush or SSE path, `waitUntilCb`.
- **One link model.** `LinkCore` shared by `StreamLink` and `H1Link`: does a
  shared path assume HTTP/2 facts (stream ids, RST, per-stream windows) that
  are false for HTTP/1.1, or the reverse? Did the HTTP/2 path change behaviour?
- **HTTP/1.1 behaviour a client sees.** Keep-alive, pipelined heads,
  `Expect: 100-continue`, chunked and close-delimited framing, the 408 and
  503 answers, TLS. Anything that changed other than the new 503 when the
  request pool refuses.
- **Accounting.** Request-pool and connection charges returned exactly once,
  on every exit, including an orphan that ends after its connection is gone.
- **Tests that would pass with the defence removed.**

Label every finding **[LOGIC]** (holds regardless of deployment) or
**[PREMISE]** (name the deployment fact it rests on). Unlabelled findings are
discarded. For each: file:line in the commit, a concrete failure scenario, and
the quoted code. Say "no finding" for each area you checked and found sound.
