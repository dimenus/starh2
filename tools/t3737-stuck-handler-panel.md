# Panel: what should starh2 do about a handler that never stops?

You are one of several independent reviewers. Answer the question below with a
decisive pick. The code is in this repository: branch `carmack/t-3737`, commit
`839dd69` (the spike). Your cwd is that worktree. Read the code; do not answer
from this brief alone.

## Premises — settled. A finding that depends on their negation is out of scope.

1. starh2 is an HTTP/2 server in Zig on the zio runtime. It has no external
   users. Breaking changes are free. Do not weigh backward compatibility.
2. zio is cooperative. A task can be asked to cancel (`AnyTask.cancel`: set a
   flag and wake it), never forced. A handler that never yields, shields
   itself, or swallows `error.Canceled` keeps running. There is no way to kill
   one task. This is not up for change.
3. Memory a running task can reach must not be freed or reused.
4. The spike's design is accepted: an HTTP/2 task handler reaches only its
   `StreamLink` (in its request arena, from the server-wide request pool) and
   enters the connection only through a gate. When the connection ends, the
   gate closes, a cancel is requested, and the connection's slot is freed at
   once. A handler still running then is an "orphan".
5. Before the spike, any such handler made the whole process panic 5 s after
   its connection ended. Going back to a process-wide panic is out of scope.

## Facts (read them in the code; tell us if one is wrong)

- An orphan holds: its stack, its `StreamLink`, its request arena blocks, and
  one server-wide task token (`GlobalAccounting.tryReserveReaper`,
  `Limits.cancellation_reaper_jobs`, default 4096). The token is released when
  the link is freed. `GlobalAccounting.orphaned_handlers` counts orphans.
- `Limits.max_streams_per_connection` is 256 and `max_connections` is 1024.
  So by arithmetic, 16 connections each with 256 orphans hold all 4096 tokens.
  Not tested. When no token is free, a new task handler is refused.
- `Server.serve` waits for `orphaned_handlers == 0` with no limit (1 ms poll)
  before it returns, because `deinit` frees the request pool.
- A handler becomes an orphan only through application code that ignores
  cancellation. A normal handler (`try zio.sleep`, a cancellable wait) ends
  when teardown's cancel arrives; measured: 0 ms, h2c and TLS.

## The question

What should the policy be for orphans: their admission cost, their limit, how
they are reported, and what server shutdown does with them?

Options we have seen. You may pick one, combine them, or propose another.

1. Keep the token as it is. Log every orphan with its route, and expose the
   orphan count. Orphans are honest memory accounting; the log names the bad
   handler for its owner.
2. A separate, smaller cap for orphans. When it is full, new task handlers are
   refused (or: new task handlers on the route that made the orphans).
3. A per-peer cap on orphans, so one client cannot use up the server's tokens.
4. Shutdown: wait with no limit; or wait with a limit and then exit the
   process without `deinit`; or something else.

## What we want back

- Your pick, in one sentence, then the reasons.
- What each option understates or gets wrong.
- The concrete failure scenario for your pick: inputs, state, what breaks.
- Anything in the code that contradicts the facts above. Quote the line.

Label every finding:
- **[LOGIC]** holds whatever the deployment.
- **[PREMISE]** depends on a deployment fact; name the fact.
Unlabelled findings are discarded. Be short. Do not edit any file.
