# Review brief, round 3: t-3759 steps 1 and 2 (d332346, d01817f)

Review two commits (d332346 step 1, d01817f step 2) on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`. Rounds 1 and 2 reviewed earlier versions (af05327/8370b4d, then 93107af/124cfee); their findings were fixed in these commits. Read the committed objects (`git show <sha>`,
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
  `write_byte_timeout_ns` (default 60 s, as nginx `send_timeout`; 0 = off) while bytes are waiting is
  closed, on h2c, TLS and HTTP/1.1, as Go's `WriteByteTimeout` does but on by
  default. Progress is per byte the socket accepts. The per-stream response
  clocks (stream window, connection window, rate) stay.
- Owner's decision: response grace 5 s; request body 240 B/s after 5 s.
- Shares, per-client accounting and the HTTP/1.1 link model are later steps.

- Owner's decision, accepted cost: on Linux over a fast path, a reader slower
  than about 30 kB/s can lose its connection at 60 s (kernel writer-wakeup
  steps). Do not report that cost; do report anything that makes it worse.
- Implementer's choice in round 2: queued WINDOW_UPDATE credit counts as the
  peer's after one `request_body_idle_timeout_ns` (a cap), not per-frame
  tracking. Judge whether the cap is sound; a cheaper exact form is a finding.

## What to look for

Above all the round-2 changes: the HTTP/1.1 handler join on every exit of
`h1.serve` (shielded, charges moved to the slot); the queued-credit cap and
`refuseWindowUpdate`; progress dated at socket acceptance (`accepted_ns`,
`Pump.sent_ns`); the HTTP/1.1-over-TLS cause mapping; the `serving` flag in
`deinit`; the bare END_STREAM sent on a zero window (RFC 9113 6.9.1) and its
reset on connection-window expiry; absolute `.deadline` timers. Then:

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
