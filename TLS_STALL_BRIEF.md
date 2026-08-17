# Brief: isolate and fix the intermittent TLS connection stall

**Status.** The 30s stall this brief isolates landed in `57359b7`
(`Don't park a TLS connection on DATA that a RST already killed.`).
Do not re-solve it. Keep the file as the diagnostic method: how the stall
was separated from `not-started`, which measurements refute which cases,
and what not to "fix". The next park of this shape should start here, not
from a fresh reading of the actor loop.

`not-started` (`started < total`) remains open as t-761.

## Goal

Find the smallest Starh2 boundary that strands an otherwise-live TLS
connection, prove the cause with state captured at the stall, fix that cause,
and add a deterministic regression test.

Do not begin by changing zio. The current evidence no longer supports a claim
that this is only a zio task-migration defect.

## Repository protocol

Read `AGENTS.md` and `~/.claude/skills/zig/SKILL.md` before you edit. Use
`zigstd` for Zig 0.16 standard-library APIs. Do not guess them.

These premises are settled:

- This stack is malleable and has no external consumers. Make the breaking
  change if that is the clean fix. Do not add compatibility machinery.
- Prefer share-nothing ownership. A new mutex is evidence that state probably
  belongs to the wrong task.
- Use the grain of `std.Io` and zio: task ownership, channels and events,
  buffered writers, explicit flush, and cancellation. Do not build a parallel
  runtime.

The working tree contains unrelated changes in progress. Do not revert,
rewrite, stage, or commit them.

## What is known

### The stall signature is request accounting, not duration

This is a correction. An earlier version of this brief said the connection
parks for about the configured 30-second deadline. The captured rounds
disagree. Six captures carry an identical `requests:` line and three different
durations:

| capture | duration | requests line |
|---|---|---|
| `/tmp/starh2-stall-tls-mig-100.round2.h2load` | 30.14s | 100000 total, 100000 started, 100000 done, 99974 succeeded, 26 failed, 26 errored, 0 timeout |
| `…round80`, `…round81` | 30.3s | identical |
| `…round86` | 64.74s | identical |
| `…round85` | 137.47s | identical |
| `…round61` | 30.13s | same shape, 30 failed |

So the signature is:

- TLS only. h2c has not reproduced it.
- `started == total` and `done == total`. Every request reached the server.
- `failed == errored`, and that number is small and nearly fixed: 26 in five
  captures, 30 in one. `timeout == 0`.
- The server process stays alive and sleeping.
- The duration is a consequence, not the signature. A 25-second or 30-second
  threshold would have missed two of the six captures.

At auto executor width, task migration raises the reproduction rate greatly,
and a migration-off setting has been a useful operational workaround.

Migration is **not necessary**. One-executor, migration-off runs have produced
the same accounting signature.

Therefore the working claim is narrower: migration changes the timing of a
Starh2 TLS liveness defect. Nobody has proved that zio loses socket readiness.

### Measured at the stall: both kernel queues are empty

Captured on 2026-08-16 with the hardened harness, binary sha `713066228ead506c`,
auto executors, migration on, 60 rounds. Three rounds stalled (17, 27, 60). The
socket samples are in `/tmp/starh2-stall-arm-stall2/base.round{17,27,60}.sockets`.

Every stalled round shows the same picture at t=5s, t=15s and t=25s:

- **Exactly one connection remains established.** The same client port appears
  in all three samples, so one connection parks for the whole window.
- **`Recv-Q` and `Send-Q` are 0 on the server row and on the client row.**

This refutes case 1 below. No kernel receive data waits for a parked ReadPump.
The client has sent everything and the server has read it. The server has sent
everything it wrote and the client has read it. Both peers wait, and the
unconsumed work sits inside the Starh2 process.

**The limit of this measurement:** `netstat` reports kernel socket buffers
only. It cannot see bytes held in the TLS receive accumulator, in `read_ch`, in
a scheduler slab, or in any other userspace structure. That is exactly why the
diagnostic pass below is still required. The measurement narrows the search to
cases 2, 3 and 4. It does not choose between them.

### Measured at the stall: nothing in the server is runnable

Captured twice with `tools/stall-thread-sample.sh`, which samples every thread
of the live server and of h2load with `sample(1)` and changes no code. Both
captures caught the exact 26-failure, 30.1-second signature.

Across a 3-second window at the stall, sorted by top of stack:

```
kevent          (in libsystem_kernel.dylib)   26652
__ulock_wait    (in libsystem_kernel.dylib)    2221
```

That is everything. All 12 executor threads sat in `kevent` inside
`ev.loop.Loop.poll` for **100% of the samples**. No starh2 frame appeared on
any thread stack. The one remaining thread, which is not an executor, waited in
`__ulock_wait`. h2load meanwhile sat in `ev_run`.

Three failure modes are refuted by this, and each would have needed a different
fix:

- **Lost socket readiness below starh2 is refuted.** No thread waits in a
  socket read, and there is no kernel data to deliver.
- **Lock contention or a deadlock on `session_mu` is refuted.** No executor
  thread waits on a lock. This was the obvious suspect, because `session_mu`
  covers three domains.
- **A spin or a busy loop is refuted.** No thread burns CPU in starh2 code.

**What remains:** both processes are idle, no bytes wait in either kernel
buffer, one connection stays open, and 26 requests are never answered. Nothing
in the server is runnable, so no task will publish the event that another task
waits for. That is a lost wakeup, or work stranded in a queue whose consumer
has already parked. It is cases 2, 3 and 4 below, and nothing else.

**The limit of this measurement:** a suspended coroutine sits on no thread
stack. The sample names the state of the runtime; it cannot name the await
point of a parked task, or say which queue holds the stranded work. That is
what the diagnostic pass below is for.

### A second, different defect: requests that never start

`started < total` is a separate class. The client never submitted the rest of
the requests. It is fast, it loses thousands of requests, and it happens with
migration both on and off.

It is also far easier to reproduce than the stall, which makes it useful:

```sh
tools/tls-stall-delta.sh notstarted --mode tls --rounds 3 \
  -n 1000000 -c 50 -m 10 -t 4
```

That command produced `NOT-STARTED` in 3 of 3 rounds on a migration-off
server. The `-c 10` case is the same class.

**Instruction change from the earlier brief.** Do not treat this class as
forbidden evidence. Spend a bounded first pass on it, because it is
deterministic and the stall is not. Then state whether the two share a cause.
If they do not, file it and continue. A deterministic reproducer is worth more
than a 7.5% one, even when it turns out to be a different bug.

The harness never folds the two together. It classifies every round and counts
the classes separately.

### The plaintext zio reproducer

The zio-only three-task example under `tools/zio-migration-repro/` stays green.
Do not report it upstream as a reproducer.

## The instrument

`tools/tls-stall-delta.sh` is the harness. Read its header comment. It now
enforces the things that a human previously had to remember:

- **It always binds port 0** and reads the real port from the server ready
  line. It rejects `--port`. A leftover server can no longer answer for the
  build under test.
- **It prints the sha256, size and mtime of every arm binary.** A missing
  binary fails. A binary older than any file under `src/`, `examples/`,
  `build.zig` or `build.zig.zon` fails, unless you pass `--allow-stale`.
- **Two arms that resolve to one file is a hard failure.**
- **It classifies each round** as `ok`, `stall`, `not-started` or `other`, with
  an orthogonal `slow` flag. The classifier was validated against the ten
  captured rounds above and reproduced their known labels.
- **It samples sockets and labels each row `server` or `client`** by the owning
  pid from `netstat` field 11. It counts the rows, prints per-side Recv-Q and
  Send-Q totals, and says `SAMPLE-EMPTY` when it finds nothing. The earlier
  version truncated the sample at 20 rows and could not tell the sides apart.
- **It has a compare mode.** `--bin-b` alternates two binaries round by round
  against two servers in one session.

Build an arm like this:

```sh
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix /tmp/starh2-base
```

**The case that reproduces most reliably today**, measured on 2026-08-16 at 3
stalls and 2 not-started rounds in 60:

```sh
tools/tls-stall-delta.sh tls-auto \
  --mode tls --task-migration --rounds 60 \
  -n 100000 -c 50 -m 10 -t 4 --sample-after 5 --sample-every 10
```

The two-executor case gave 0 in 40 rounds in the same session, so prefer the
auto width. Start there:

```sh
tools/tls-stall-delta.sh tls-two \
  --mode tls --task-migration --executors 2 --rounds 40 \
  -n 100000 -c 50 -m 10 -t 4
```

Run one case at a time. Each case writes to `/tmp/starh2-stall-<name>/`.

`-N 2` is an acceptable accelerator at auto executor width. It is not
acceptable with one executor, because it creates overload failures in the
migration-off control. The unaccelerated signature is authoritative.

## First question to answer

At the moment the actor parks, where is the unconsumed work?

Separate these cases mechanically. Case 1 is refuted twice over, by the socket
measurement and by the thread sample. Re-confirm it in your own capture, then
work through the rest.

1. Kernel receive data remains while ReadPump is parked in socket I/O.
   Investigate zio readiness. **Refuted.** The server `Recv-Q` is zero in three
   captures, and no thread waits in a socket read in either thread sample.
2. ReadPump has accepted bytes, but `read_ch` or TLS buffered input still holds
   them while the actor is parked. Investigate the actor wake predicate.
3. Outbound chunks or write completions remain while their owning task is
   parked. Investigate that queue and wake boundary.
4. No transport work remains, but handler completion or scheduler work does.
   Investigate the reset-then-recheck producer census in the actor.

Do not infer queue state from kernel queue sizes alone. Quote the labelled
socket rows from `<outdir>/<arm>.round<N>.sockets`, and say which side each row
came from. That file is the only place the direction is recorded.

## Questions the root-cause statement must answer

A mechanism that cannot answer these is the wrong mechanism.

1. **Why 26?** Five captures lost exactly 26 requests, one lost 30, and an
   earlier note records 104, which is 4 x 26. Derive that number from the
   mechanism, or name the quantity it depends on and show why it is not
   derivable. This is the cheapest independent oracle in the problem.
2. **Why does the park end at all?** Two captures ran to 64.74s and 137.47s and
   then completed. A mechanism that strands work forever does not explain a
   connection that recovers.
3. **Why TLS only?** h2c has never reproduced it. Name the state that exists in
   the TLS path and not in the h2c path.
4. **Why does migration raise the rate without being necessary?** State the
   timing that migration changes.

## Required diagnostic pass

Add opt-in diagnostics to the benchmark path. The normal production path must
pay no atomic, formatting, allocation, or timer cost.

Capture enough state to locate the owner of the stranded work:

- successful posts and consumes for `read_ch`, `write_ch`, `write_ack_ch` and
  `completion_ch`;
- ReadPump state: waiting for a free lease, waiting in socket read, posting;
- actor state: processing, immediately before `actor_wake.reset`, immediately
  before `waitForActivity`, and resumed;
- WritePump and AckDrainer state;
- TLS receive accumulator length, and whether the decrypt step left plaintext or
  ciphertext pending;
- `sched_refilled`, `writer_failed`, live handler count, scheduler pending
  count, and the armed deadline;
- an increasing actor-wake publication count, split by producer.

Do not read `std.Io.Queue` internals concurrently without synchronization.
Prefer diagnostic producer and consumer sequence counters. If a post can race
its counter update, record started, completed and failed sequences, so that the
snapshot cannot manufacture impossible occupancy.

Emit one bounded snapshot when a connection makes no progress for a diagnostic
threshold. Set that threshold below the shortest observed park, and state the
value you chose and the connection deadline it competes with. 5 seconds is a
safe choice. A snapshot that never fires before the connection dies is useless.

The watchdog may observe and report. It must not wake, retry, close, or heal
the connection.

## Wake invariant to audit

`Connection.run` resets `actor_wake` and then rechecks producer-owned evidence
before it parks. Enumerate every production `actor_wake.set()` caller:

- ReadPump posts;
- normal and reaper handler completion posts;
- scheduler refills;
- writer failure and shutdown;
- server shutdown.

For each producer, name the durable state that survives a set-before-reset race
and that the actor checks after the reset. A comment is not evidence. Add a
deterministic test that pauses the actor between the reset and the park,
publishes from that producer, then proves the actor processes the work with no
unrelated wake.

The test must cover every producer independently. A periodic poll, a timeout,
an extra unconditional wake, or a sleep-based test does not satisfy this
invariant.

**These tests are required work whatever the diagnosis shows. They are not, on
their own, proof of the root cause.** They become the proof only when the
captured snapshot shows case 2 or case 4.

## Fix constraints

Do not fix the symptom by any of these:

- retries, periodic polling, sleeps, or a shorter timeout;
- suppression of failed-request accounting;
- keeping task migration disabled and calling the root cause fixed;
- blocking socket I/O in the actor;
- two tasks that share TLS connection state;
- a mutex around pump-owned state;
- weaker graceful shutdown, ticket completion, or bounded-memory rules.

The fix must preserve:

- ReadPump and WritePump as the sole owners of the socket directions;
- actor ownership of TLS and Session state;
- all production output through `FairScheduler`;
- exactly-once release of chunks, tickets, leases and handler slots;
- the existing resource bounds.

If the evidence proves the defect is below Starh2, stop after you produce a
standalone reproducer that fails without an import of Starh2. Do not file an
upstream claim that rests only on the Starh2 benchmark.

## Acceptance criteria

All of these are required.

1. **A captured snapshot, taken before the fix, identifies the stranded work
   and its owning task.** The count of captured snapshots must be greater than
   zero. The diagnosis must cite exact counters and state, not timing. If no
   snapshot is ever captured, report that and stop. Do not propose a fix from a
   reading of the code.

2. **The fix removes the state that the snapshot captured.** Re-run with
   diagnostics on and show that the same snapshot no longer occurs. A fix whose
   only evidence is a clean run is not accepted.

3. **A deterministic regression test fails when the fix is reverted.** Prove
   the mutation. State the exact revert you applied and the test output.

4. **The stall does not survive, and the control proves the instrument fired.**
   Use the compare mode. It exits 2 as `INCONCLUSIVE` when the control arm
   never stalls, and an `INCONCLUSIVE` run is not a pass.

   ```sh
   tools/tls-stall-delta.sh acc-auto --mode tls --task-migration --rounds 40 \
     -n 100000 -c 50 -m 10 -t 4 \
     --bin /tmp/starh2-base/bin/starh2-bench-server --label-a base \
     --bin-b /tmp/starh2-fix/bin/starh2-bench-server --label-b fix

   tools/tls-stall-delta.sh acc-two --mode tls --task-migration --executors 2 \
     --rounds 40 -n 100000 -c 50 -m 10 -t 4 \
     --bin /tmp/starh2-base/bin/starh2-bench-server --label-a base \
     --bin-b /tmp/starh2-fix/bin/starh2-bench-server --label-b fix
   ```

   The alternating arms exist because a fixed arm order has manufactured a
   result in this repository before, and because a machine that stops
   reproducing the bug otherwise looks like a fix.

   **Why the round counts alone are not the proof.** At the measured base rate
   of 3 stalls in 40 rounds, 40 clean rounds happen by chance about 4 times in
   100. At 1 in 20, they happen about 13 times in 100. Criterion 3 carries the
   proof. These runs only support it.

5. Migration-off behaviour stays correct: 20 single-arm rounds with the fixed
   binary, auto executors, migration off, zero failures of any class.

6. h2c control passes 20 rounds.

7. The `not-started` class is either shown to be unaffected, or diagnosed
   separately, or filed with `~/.claude/scripts/taskprobe/task`. It must not be
   folded into this result silently. Quote the per-class counters from the
   `RESULT` lines.

8. `./zb build ci` passes, including `tls-smoke`.

9. With diagnostics disabled, the benchmark-server hot path carries no new
   always-on instrumentation cost. Show how you checked.

Report exact commands, binary sha values from the `BIN` lines, per-class
counters from the `RESULT` lines, and whether you used an inactivity
accelerator. Do not report a probabilistic pass as proof without the
deterministic regression test.

## Deliverables

- A root-cause statement with the failing snapshot, which answers the four
  questions above.
- The minimal production fix.
- The deterministic, mutation-proven regression test.
- The per-producer wake-invariant tests.
- Any opt-in diagnostic support worth keeping. Remove the rest.
- Updated `README.md` and `tools/README.md` claims, if migration is no longer
  the correct explanation.
- Updated `tools/tls-stall-delta.md` with the final measurements and the binary
  sha values that produced them.
