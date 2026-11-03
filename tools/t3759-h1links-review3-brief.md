# Review brief, round 3: t-3759 step 7, HTTP/1.1 onto links (836a84e)

Review commit `836a84e` on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`. Its parent is `6ea2fcf`.
Round 2 reviewed `b3c1885`. Its findings were fixed in `836a84e`:

- The HTTP/1.1 TLS pump posted acks into a 4-slot channel nobody read, and
  `Pump.post` panics when it is full. A client reset could crash the server.
  Now `h1AckSink` applies acks on the posting task, as HTTP/2 does.
  `driveTlsUntilIdle` and `writeRawTlsOwner` honour `.exit`. A failed
  `writeChunks` ends the connection.
- `offload_tls_io` was cleared before the gate drained on a cancelled `serve`.
  The flag is gone. The write path is chosen by the calling task (`onActor`, a
  zio task-local).
- The multi-chunk test could pass without its trigger. It now checks the
  client and handler are live at the trigger, and checks the recorded cause.

Read the committed objects (`git show 836a84e`, `git diff b3c1885 836a84e`,
`git show 836a84e:<path>`), not the working tree. Read the commit message in
full. Read zio source under `zig-pkg/zio-*/src` and `src/edge/tls.zig` before
any claim about them. The premises and areas of
`tools/t3759-h1links-review-brief.md` are unchanged.

## What to look for

Above all `git diff b3c1885 836a84e`:
- `h1AckSink` drops every completion and panics if one carries accounting.
  Is it true that HTTP/1.1 completions never carry an outbound release, a
  control-pool release, a ticket or a receipt? Can any client input reach the
  panic? Does anything the old channel's receiver would have done (nothing
  received it) now need doing?
- `onActor` as a zio task-local: is it set for the whole of `serve` on every
  path, on the right task, also when the connection task migrates between
  executors? Can a handler task ever read it as true? Does anything else read
  task-locals in a way this breaks?
- `.exit` now ends the connection on the owner path, and `readSomeTls` returns
  end of stream. Can that change client-visible behaviour on a healthy
  connection, or drop a response that would have been delivered?
- Any remaining post into a bounded channel, on HTTP/1.1 or HTTP/2, that a
  client can push past its capacity.

Correctness defects only. Label every finding **[LOGIC]** or **[PREMISE]**
(name the deployment fact). Unlabelled findings are discarded. For each:
file:line in the commit, a concrete failure scenario, and the quoted code. Say
"no finding" for each area you checked and found sound.
