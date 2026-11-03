# Review brief, round 2: DST stage 1 (b36c624)

Review `b36c624` on branch `carmack/dst-witness` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-dst`. It is `fd72bd5` amended with
the round-1 fixes; read `git diff fd72bd5 b36c624` first, then the full commit
message (its "Review round 1" section). Read committed objects only. The
round-1 brief, `tools/dst-stage1-review-brief.md`, holds the premises; they are
unchanged.

## What to look for

Above all the production changes in this round:

- **`Pump.stopped`** (set when the pump posts its shutdown): `writeChunks`
  refuses, `submitOp` refuses as a closed CQ, and `stageCloseNotify` and
  `drivePending` return false. The TLS pump is shared with HTTP/2. Read every
  HTTP/2 caller in `src/edge/connection.zig` and `src/edge/tls.zig` that can run
  after the pump posted its shutdown: can any of them now hang (waiting for a
  submission or an ack that will never come), drop a chunk without returning it
  to the pool, leave a ticket unsettled, or skip a close_notify a healthy path
  needed? Does every refused chunk still reach its owner exactly once?
- **`H1Tls`** now owns the teardown decision (`armed`, `running()`,
  `close()`, `deinit` panics on a running pump). Any path in `H1Conn` where
  `close()` is not reached before `deinit` (errors in `initConn`, a cancelled
  `serve`, startup failure)?
- The witness's new write-after-exit offer and the Debug pinning: anything
  that makes the gate vacuous.

Correctness defects only. Label every finding **[LOGIC]** or **[PREMISE]**
(name the deployment fact). Unlabelled findings are discarded. For each:
file:line in the commit, a concrete failure scenario, and the quoted code. Say
"no finding" for each area you checked and found sound.
