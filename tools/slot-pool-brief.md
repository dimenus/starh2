# Brief: pooled connection slots, stage 1 (boot memory)

You are the implementer. A reviewer from another vendor reads your diff, and
the orchestrator reruns every test and measures RSS itself. Do not tell the
orchestrator that tests pass; it recomputes them. Findings reach you only
through the orchestrator. Do not prompt, message or read any other agent's
pane.

## Why (settled; do not reopen)

Under `std.heap.smp_allocator`, starh2's idle RSS grows without a visible
stop. No memory leaks; the allocator wastes freed blocks because of the
pattern of threads that free them. About 73 % of the growth is in connection
churn: each new connection makes about 58 general-allocator (GPA) calls, about
1.2 MB, and `Connection.init` alone makes 39 calls, about 949 KB. The owner
decided: **the server keeps connection slots and reuses each slot in place;
no GPA calls per connection in steady state; RSS bounded by peak concurrent
connections times the slot footprint.** This is the reference design of
TigerBeetle (`src/message_bus.zig`: arrays at init, `connection.* = .{` on
close).

The work has three stages. **This brief is stage 1 only: boot memory.**
Stage 2 (request arenas and bodies per handler slot) and stage 3 (no GPA
fallback for frames and write chunks, an HPACK ring, response-header scratch)
come later. Do not start them.

## Premises: treat these as settled

- No external users. Breaking changes are free.
- Read `AGENTS.md` first. zio owns concurrency: `std.Io` abstractions are
  banned (Event, Queue, Group, Mutex, Future, Select); the `std.Io` vtable is
  fine; `zio.Mutex` and `zio.Channel` are allowed. zio is vendored under
  `zig-pkg/`; read a function before you rely on it.
- The embedding app chooses the GPA. Slots are allocated from it, once each.

## Stage 1 design (decided; do not reopen)

1. **A slot is a stable heap object** (`ConnSlot`, in `src/edge/`): the
   `Connection` storage (never moved after the first boot: `finishBoot`
   exists because `init` returns by value and a move dangles interior
   pointers) plus ONE boot buffer that a `std.heap.FixedBufferAllocator`
   hands out.
2. **Boot allocations come from the slot buffer.** Everything
   `Connection.init` allocates at boot (the arrays, both `SlabPool`s, the TLS
   buffers, the `FairScheduler` arrays), Session's boot tables in
   `Session.init` (stream map, pending maps, tombstones, credit ids, intent
   storage, preface scratch, header block), and the TLS `tls_edge.Conn`
   (today `gpa.create` in `src/edge/server.zig`). Runtime allocations (the
   request arena, bodies, HPACK, frame and write-chunk fallbacks, response
   headers) stay on the GPA in this stage. Give `Connection.init` and
   `Session.init` an explicit boot allocator parameter; do not hide the
   split behind a global.
3. **Slot-resident hash maps must never grow.** Zig 0.17's `HashMap.rehash`
   is in place (it returns `void` and takes no allocator, `std/hash_map.zig`),
   but `put` calls `grow` when `available` is exhausted, and deletes consume
   `available` until a rehash. Session already rehashes the stream map when
   `stream_removes * 2 >= capacity`. Prove, by reading the code and by an
   assertion, that no slot-resident map can grow. If one can, make it unable
   to (rehash earlier, or `putAssumeCapacity` behind a proven bound) and say
   which.
4. **One number for the slot size.** `Limits.slotBootBytes(mode)` computes
   the boot buffer size. The test that keeps it honest: for h2c and TLS, and
   for at least three different `Limits` (the default, a small one, a large
   one), booting into a buffer of exactly `slotBootBytes` succeeds and
   booting into one byte less fails with `error.OutOfMemory`.
   `resourceUpperBound` counts the slot term from the same function. The
   existing boot terms it replaces are removed, not kept beside it (t-3527
   found the current pending-map term undercounts: 3 maps of `u31 -> usize`
   against 4 real maps with wider values).
5. **The free list is a LIFO stack under a `zio.Mutex`**, owned by `Server`,
   capacity `max_connections`, slots created lazily (so RSS follows the peak,
   not the cap). LIFO reuses the most recently used slot, whose pages are
   warm.
   - Take a slot in `Server.acceptLoop`, after admission, before the spawn.
     On a spawn failure, push it back before admission is released.
   - Return it in `connEntry`'s defer, in this order: reset the slot, push
     it, then `releaseConnection`. Releasing first lets the next accept see
     room and create a duplicate slot.
   - `Server.deinit` frees every slot and asserts the stack holds
     exactly the created count.
6. **Reset in place, not free.** Split `Connection.deinit` into what makes a
   slot reusable (drain the channels, join the pumps and handlers, return
   the server-wide slabs, release stream admission through Session's
   `onRelease` walk, close the socket) and what freed boot memory (now
   nothing: the FBA is rewound when the slot is pushed). Every one-shot state
   must be re-armed on reuse: channels that were closed are re-initialised
   over the same buffers, `dead`, `wire_started`, the read free list
   (`finishBoot`), the preface queue. In safe builds, before the push,
   assert the slot is clean: no live handler or join, both slab pools fully
   returned, scheduler empty, no parked request or lease, accounting zero,
   every data channel empty. A slot that fails the check is not pushed (it is
   leaked, counted, and reported, never reused dirty).
7. **h1c is out of scope.** Its buffers (`src/edge/h1.zig`) stay as they are.

## Tests (required)

- The exact-fit test in item 4.
- **The rule as a test: zero boot GPA calls after warm-up.** With a
  counting allocator as the server GPA, open N connections, close them, then
  open and close N more: the second round makes no GPA call from the boot
  path. (Runtime allocations of a request still happen; count only calls
  from boot, or use a shape where the only runtime work is the preface and
  SETTINGS exchange. Say which you used and why it isolates boot.)
- Reuse correctness: one connection whose slot was used before by a
  connection that ended (a) normally, (b) by a connection error with GOAWAY,
  (c) by a cancel, (d) with streams open at close, must behave exactly like a
  fresh one (the existing lifecycle and goaway tests are the template; run a
  representative set of them on reused slots).
- The safe-build clean check fires: a test that dirties a slot on purpose
  (test-only hook) and asserts it is not pushed.
- Server shutdown with slots in use and slots free: every slot freed once.

## Mutation tests (required; break the CODE, not the tests)

1. Return the slot after `releaseConnection` instead of before.
2. Skip re-initialising one closed channel on reuse.
3. Do not rewind the FBA on push.
4. Remove the clean check.
5. Make `slotBootBytes` one byte short, then one byte long.

For each, run the full `./zb build test` and record which test fails.

## Gates

- `./zb build test` and `./zb build ci` (on this Mac `ci` always fails
  `macos-libcxx-probe`, two runs, environment; every other step must pass)
  and `./zb build tls-smoke`.
- Zig caches a passing test run; change a comment in a test file to force a
  real rerun.
- The orchestrator measures RSS on nachos with `tools/rss-stability.sh`; you
  do not need to. Make sure `--gpa smp` and `--count-gpa` still work in the
  bench server.

## Rules

- Work only in `/Users/rsaunderson/Source/mine/starh2-wt-slot`. Never touch
  `/Users/rsaunderson/Source/mine/starh2`.
- Base tripwire, before any work: `git log -1 --format=%s` is the commit that
  added this brief, and its parent's subject starts with "Deliver the GOAWAY
  before closing". If not, STOP and report `STATUS: blocked`; do not rebase.
- `testdata/cert.pem` and `key.pem` are already in your worktree.
- This is a significant refactor. If you find that a decided item above
  cannot work, STOP and report it with the code that shows why, before
  building around it.
- Commit per milestone. Each message describes its own tree and is the
  decision record (`~/.claude/CLAUDE.md` "Commits": problem, changes,
  benefits, alternatives rejected, what it depends on, statistics; markdown;
  plain ESL English). End it with
  `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.
- Do not push. Do not file, close or edit tasks; list what you would file in
  the report under "To file".
- Report in `tools/slot-pool-report.md` (not committed): commits, the
  exact-fit numbers per mode and Limits, the zero-boot-call test output, each
  mutation and the test that caught it, the full gate summaries, what you
  could not verify.

## Round 2 (after review of 6f916a3)

The orchestrator reran `test`, `ci` and `tls-smoke` on a clean cache: same
result as your report. A cross-vendor review asked for fixes. The deviation
(rebuild through `Connection.init` instead of a re-arm list) is accepted. Fix
these, each with a test or a mutation that shows the test can fail, and say
in the report which mutation killed which fix:

1. **The bound omits slot storage.** `resourceUpperBound` counts the boot
   buffer but not the `ConnSlot` object itself (it holds a whole
   `Connection`) or the free-list array. Count
   `max_connections * @sizeOf(ConnSlot)` and the stack array once.
2. **The quarantine is unbounded and can lose a slot.** A dirty slot is
   appended with `catch {}` and admission is released, so each dirty
   connection makes a new slot. Preallocate the quarantine at
   `max_connections` in `SlotPool.init`, append with assume-capacity, and
   panic when it is full (a server that has leaked `max_connections` slots is
   broken). `deinit` asserts `quarantine.items.len == leaked`.
3. **Unchecked arithmetic in the plan.** `planSlotBoot` and its callees use
   `n_chunks + 1`, `n_chunks * WIRE_CHUNK_SIZE`, `control_entries + max_streams`
   and similar without checks, and some capacities narrow to `u32`.
   `resourceUpperBound` reaches this before it can return `InvalidConfig`.
   Use checked arithmetic everywhere in the plan and return
   `error.InvalidConfig`; add a test with extreme `Limits` values that expects
   `InvalidConfig`, not a panic.
4. **The release-order test depends on a 50 ms sleep.** Replace the timing
   with a check that cannot depend on scheduling: in safe builds, at the
   moment `connEntry` releases admission, assert the connection's slot is
   already back (`state == .empty` and counted in the pool). Rerun mutation 1
   (defer order swapped) with `test_recycle_delay_ms` at 0 and record which
   test fails. Remove the delay hook if nothing needs it any more.
5. **Tests read `SlotPool` fields without its lock** (`len`, `leaked`,
   `created` in `tests/slot_reuse.zig`). Add a locked snapshot accessor and
   use it.
6. **The clean check may be dormant.** `quiesce` already panics on most dirt
   before `recycle` runs, and the only test dirties the slot after `quiesce`.
   List every condition `dirtiness` checks and, for each, whether `quiesce`
   can reach `recycle` with it set. For a condition no real path can reach,
   say so in the commit message. If NO condition is reachable, the check is
   a second net behind `quiesce`: keep it, and say that plainly in the
   `conn_slot.zig` doc instead of promising quarantine for real leaks.
7. **Make every connection an exact-fit check.** In safe builds, right after
   `Connection.init` returns in a slot, assert
   `fba.end_index == limits.slotBootBytes(mode)`. Then every test, under every
   `Limits` it uses, checks that `planBoot` still matches `init`, not only the
   four sets in the exact-fit test. Mutation: add one extra small boot
   allocation to `Session.init` without changing its `planBoot`; record which
   tests fail.

Not in this round: the HTTP/1.1 cancel path (a cancel during an h1 task
handler can reach `H1Conn.deinit` with the handler live). It predates this
branch and is filed separately. Do not change `h1.zig` for it.

Commit per item or per pair; same message rules. Update
`tools/slot-pool-report.md` with a "Round 2" section. End with
`STATUS: done s1r2`.

## Round 3 (after review of 874b759)

The orchestrator reran `test`, `ci` and `tls-smoke` on a clean cache, and
mutations 4 (defers swapped: 93 crashed) and 7 (a 3-byte planned entry not
allocated: 86 failed, under five different slot sizes): all match your
report. The review found two defects, both inside the round-2 fixes:

1. **Quarantined slots break the bound.** A quarantined slot no longer holds
   admission, so up to `max_connections` new slots can be made beside a full
   quarantine: twice what `resourceUpperBound` counts. Decided fix: slots in
   existence, quarantined ones included, never exceed `max_connections`.
   `SlotPool.take` returns an error when `created == stack.len` and the free
   list is empty; the accept loop already closes the connection on a failed
   take (check that path releases the handshake and admission in the right
   order). The "quarantine full" panic then cannot be reached; replace it with
   an assert. Change the test that leaks slots: after `max_connections` leaks
   the next connection is refused, and `created` stays at `max_connections`.
   Mutation: allow `take` to create past the cap; record which test fails.
2. **`alignForward` can still overflow.** `Plan.block` calls
   `std.mem.alignForward(usize, self.end, alignment)`, which asserts when
   `end + alignment - 1` overflows. Use checked addition before aligning, and
   the same in `hashMapBytesForCapacity`. Add a case to the extreme-limits
   test that puts the plan end near `maxInt(usize)` (for example a huge
   `compressed_header_bytes`) and expects `InvalidConfig`. Mutation: restore
   the plain `alignForward`; record which test fails.

Same rules. Add a "Round 3" section to the report. End with
`STATUS: done s1r3`.
