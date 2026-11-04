# Brief: t-3831, the h2c actor reads FairScheduler and Session state without session_mu

Worktree `/Users/rsaunderson/Source/mine/starh2-wt-3737`, branch
`carmack/t-3737`. Tripwire: going down first parents from HEAD you reach
`e2c2c7e` (t-3759 step 4, 3 of 3) with only brief commits ("Add the ...")
between. If not, STOP. Untracked `tools/dst-r*/`, `tools/pcap-r1/` and report
files are not yours.

## The race (found by the TSan lane; the full task note is t-3831)

`Connection.runBody`'s locked block (`src/edge/connection.zig` ~4757-4860)
ends before `waitForActivity` (~4883). `waitForActivity` -> `nextDeadlineNs`
(~3963) calls `self.sched.nextStallDeadlineNs` (`fair_scheduler.zig` 911-912:
reads `conn_stall`, `live[0..active_pending]`, `pending_slots[i].stall`) and
`session.nextIdleDeadlineNs` / `session.unsentDeadlineNs` with no lock, while a
handler task writes the scheduler under `session_mu` (`writeCb` ->
`enqueuePending` -> `FairScheduler.enqueueDataBytes` -> `allocPending`,
`fair_scheduler.zig` 883-884; `DataPending.clear` :161). `AGENTS.md` says
`session_mu` covers Session and FairScheduler. Line numbers are from the TSan
branch; find the same code in this series.

TSan pair (nachos): write by `FairScheduler.allocPending fair_scheduler.zig:884`
< `enqueueDataBytes` < `Connection.enqueuePending` < `writeCb` <
`Response.writeBody`; previous read by `FairScheduler.nextStallDeadlineNs
fair_scheduler.zig:911` < `Connection.nextDeadlineNs` < `waitForActivity` <
`runBody`.

## Do

1. Read the actor loop and decide the shape. The task suggests computing the
   park deadline inside the locked block (so the actor parks on a value read
   under the lock) or having the actor own the stall state. Whatever you
   choose: no read of Session or FairScheduler state outside `session_mu`
   remains on the actor's park path, and a handler's change to a deadline
   still wakes the actor in time (say how: the doorbell ring after
   `unlockSession`, or a lock-protected recompute). If the fix needs a design
   change (for example a new ownership rule for the scheduler), STOP and
   report the options.
2. **The oracle is the TSan lane.** It lives on branch `carmack/dst-tsan`
   (`tools/tsan/`, `./zb build tsan`), not in this series yet. Copy that lane
   into a scratch `git archive` export of this series (do not commit it here),
   and show the three signatures (`allocPending` vs `nextStallDeadlineNs`, and
   the Session deadline reads) present before your fix and absent after, in at
   least 3 lane runs each on nachos. Any other report the lane shows on this
   series: list it, do not fix it, unless it is the same race.
3. A behavioural test if one can be made without hooks in the actor (a
   response-rate grace deadline that a handler's write moves, and the actor
   wakes at the moved deadline). Say if it cannot be made and why.
4. Gates: Mac and nachos ci once each on `git archive` exports; the
   deadlines, goaway_delivery, lifecycle, placement and shares binaries 5x
   direct per machine; h2c and TLS bench pairs (6 each) on nachos against
   `e2c2c7e`, because the park path is on every turn. nachos only in a new
   `~/src/s-3759-t3831`. Time limits; fresh `--cache-dir`; never `rm -rf` a path
   built from variables, not even one that does not exist yet; never write the
   shared Zig cache by hand. Do not tell the orchestrator anything passes.

## Commit and report

One commit on top of `e2c2c7e`, below the brief commits (rebase them on top,
unchanged), with a full decision-record message. Never push. Report to
`tools/t3831-report.md` (not committed). Stop and report on any design
question.
