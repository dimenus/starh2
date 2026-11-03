# Review brief: t-3763 (adfee9c, 9d3fb98, 0df9779, 6e91aa8)

Review four commits on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`: `adfee9c` (zio.Group),
`9d3fb98` (zio.Event), `0df9779` (zio.Mutex), `6e91aa8` (zio.Channel for the
h2c write queue and the read free list). Parent of the first: `0fdbc59`. Read
the committed objects only, and each commit message in full: each lists the
semantic differences between the std.Io type and the zio type and how each
call site was checked. The brief was `tools/t3763-brief.md`. Read zio source
under `zig-pkg/zio-*/src` (the pinned version) before any claim about zio.

## Premises (settled)

- zio owns concurrency in starh2; std.Io the vtable stays.
- No behaviour a client or a handler can see may change, except where the
  commit message names it.
- The t-3759 shutdown contract: `serve` never returns while a handler runs and
  logs while it waits; `zio.Group.cancel` waits shielded with no limit and no
  logging, so it must not replace the logging wait.

## What to look for

Correctness defects only, per swap:
- **Group**: cancel and wait order at shutdown and startup failure; whether
  any wait can now hang unlogged; spawn failure paths; the two zio
  use-after-free fixes in the pin (t-3818) still on the path.
- **Event**: set/reset/wait semantics, cancel while waiting, timeouts, a wait
  that was uncancelable before and is cancelable now or the reverse.
- **Mutex**: `session_mu` lock/unlock under cancel; lockUncancelable sites; a
  lock held across a suspension that was safe with std.Io and is not with zio.
- **Channel**: capacity, blocking versus `trySend`, close semantics, what a
  closed or full channel returns; the read free list's panic on a full send
  (is it truly unreachable?); the write queue's new `WriteFailed` on closed
  and full.
- Anything deleted as dead (`read_free`, `TicketTable.io`): was it really
  unreachable?

Label every finding **[LOGIC]** or **[PREMISE]** (name the deployment fact).
Unlabelled findings are discarded. For each: file:line in the commit, a
concrete failure scenario, and the quoted code. Say "no finding" for each area
you checked and found sound.
