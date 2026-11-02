# Review brief, round 2: t-3759 steps 1 and 2 (93107af, 124cfee)

Review two commits on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`. Round 1 reviewed an earlier
version (af05327, 8370b4d); its findings are in the round-1 brief's answers and
were fixed in these commits. Read the committed objects (`git show <sha>`,
`git show <sha>:<path>`), not the working tree. Read both commit messages in
full: they hold the decision record. Read zio source under `zig-pkg/zio-*/src`
before any claim about zio, and Go's
`/opt/homebrew/Cellar/go/1.27.3/libexec/src/net/http/internal/http2/http2.go`
(`bufferedWriterTimeoutWriter`) for the rule this copies.

## Premises (settled; findings that depend on their negation are out of scope)

- No external users. Breaking changes are free; no compatibility shims.
- zio owns concurrency; `std.Io` concurrency types are banned by a gate.
- zio is cooperative; a handler that never checks cancel cannot be stopped.
  `serve` must never return while a handler still runs, and must log what it
  waits on. The wait is a shielded join by design.
- Owner's decision: a connection whose socket takes no byte for
  `write_byte_timeout_ns` (default 30 s, 0 = off) while bytes are waiting is
  closed, on h2c, TLS and HTTP/1.1, as Go's `WriteByteTimeout` does but on by
  default. Progress is per byte the socket accepts. The per-stream response
  clocks (stream window, connection window, rate) stay.
- Owner's decision: response grace 5 s; request body 240 B/s after 5 s.
- Shares, per-client accounting and the HTTP/1.1 link model are later steps.

## What to look for

Correctness defects only, above all in what changed since round 1:
- The write-byte clock: any path where bytes wait but the clock is not armed
  (h2c pump, TLS send completions, HTTP/1.1 timed send, HTTP/1.1 over TLS), a
  path where it never restarts on progress, or where an honest reader slower
  than the writer is closed while it is still taking bytes. Partial writes.
- The body-clock pause on credit that is owed or queued but not on the wire
  (`window_unsent`, `conn_unsent`, `creditOnWire`): can a peer hold a body
  paused for ever now, and does the counting ever go wrong (underflow, a credit
  counted twice, never cleared on reset)?
- Shutdown: the pre-spawned canceller, the per-connection cancel count and the
  report timing; every exit of `serve` (including startup failure) joins every
  handler; `deinit` with live work.
- Tests that would pass with the defence removed.

Label every finding **[LOGIC]** (holds regardless of deployment) or
**[PREMISE]** (name the deployment fact it rests on). Unlabelled findings are
discarded. For each: file:line in the commit, a concrete failure scenario, and
the quoted code. Say "no finding" for each area you checked and found sound.
