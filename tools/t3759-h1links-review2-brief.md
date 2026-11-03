# Review brief, round 2: t-3759 step 7, HTTP/1.1 onto links (b3c1885)

Review commit `b3c1885` on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`. Its parent is `6ea2fcf`.
Round 1 reviewed an earlier version, `f206fd3`. Its findings were fixed in
`b3c1885`: the complete-handler pool refusal answering 500; a TLS send to a
closed channel returning `WriteFailed`; no test of a shielded handler parked in
`waitUntil`; `recv_buf` poisoned before the receive was drained; and a missing
request pool failing silently. The fix round also found and fixed a write chunk
left in the TLS pump at `deinit` (`Pump.failDrain`), and moved the cause before
the failed ack on a TLS send failure.

Read the committed objects (`git show b3c1885`, `git diff f206fd3 b3c1885`,
`git show b3c1885:<path>`), not the working tree. Read the commit message in
full. Read zio source under `zig-pkg/zio-*/src` before any claim about zio.
The round-1 brief, `tools/t3759-h1links-review-brief.md`, holds the premises
and the areas; they are unchanged.

## What to look for

Above all the round-1 changes (`git diff f206fd3 b3c1885`): is each fix
complete, on every path that has the same shape (h1c and TLS, complete and task
handlers, peer close, shutdown, cancelled `serve`)? Does `Pump.failDrain` at
`deinit` return every chunk exactly once, and can it race with a send still in
flight? Does the new cause ordering hold on every TLS failure path? Then the
round-1 areas again, for anything the fixes changed.

Correctness defects only. Label every finding **[LOGIC]** or **[PREMISE]**
(name the deployment fact). Unlabelled findings are discarded. For each:
file:line in the commit, a concrete failure scenario, and the quoted code. Say
"no finding" for each area you checked and found sound.
