# Panel brief: slot pool stage 3, the last general-allocator calls per connection and per request

You are a design reviewer from a different vendor than the author. Your cwd
is a git worktree of starh2 on branch `carmack/stage3`, at master e3c8186
plus this file. Read the code; do not edit anything; do not run the test
suite.

## Where this stands (settled; do not reopen)

- starh2 is a Zig 0.17 HTTP/2 server on zio. The goal is STABLE RSS under a
  repeating load with any general allocator (GPA) the app picks. Under
  `std.heap.smp_allocator`, a block freed on another thread than the one
  that allocated it lands on that thread's freelist, and the allocator maps
  fresh slabs while those sit unused, so steady per-connection and
  per-request GPA traffic makes RSS grow.
- Stage 1 (e7cf701): every h2c and TLS connection boots from a reusable
  `ConnSlot` (one FixedBufferAllocator buffer, sized by
  `Limits.slotBootBytes`, a plan checked by an exact-fit assert on every
  boot in safe builds). Stage 2 (e3c8186): every HTTP/2 request's headers,
  body and handler memory come from a server-wide `RequestPool` (lock-free
  size classes, a resident cap with reclaim, a 64 MiB retain limit, a bump
  arena, hard caps: over a cap the allocation fails, 500/503 before commit,
  RST after). Read both commit messages.
- Measured on nachos after stage 2 (THP off for the server, smp, 10 cycles,
  n=3): RSS slope 6.4-8.0 MB/cycle, of which load 3.5 and churn 3.5-4.9.
  The pool is flat. A stage-2 test counted the GPA calls left per request
  in the steady state: small ones, 9, 13 and 45 bytes (a frame header, a
  WINDOW_UPDATE, the two preface frames), about 1 per request on average.
- Owner's standing decisions: hard failure over a cap, no GPA fallback on a
  path that has a bound; chunky up-front memory is fine when it buys a
  stated bound; the slot plan and `resourceUpperBound` stay one source of
  truth; HTTP/1.1 moves later (t-3537), out of scope here.

## What stage 3 covers (from the task; verify against the code)

- The two preface frames `Session.init` queues (SETTINGS, WINDOW_UPDATE).
- Outbound frame bytes: `rentFrame` falls back to the GPA when the frame
  pool is empty or a frame is larger than `frame_slab_bytes` (512).
- Write chunks: the write pool's GPA fallback.
- HPACK: the decoder's dynamic table (`indexIntoTable` dupes name and value
  on the decoder's allocator), and anything the encoder allocates.
- Response headers: `appendResponseHeaders` builds header lists on the GPA.
- The TLS leftover copy in `serveTlsThenBranch` (t-3532), once per TLS
  connection that pipelines its preface.

## Questions (answer each; be decisive; a survey with no pick is discarded)

1. **Inventory.** List every GPA call that can happen per connection or per
   request after warm-up on the h2c and TLS paths, with file, line and the
   quoted call. Mark each: per connection, per request, per frame, or rare.
   Include anything the list above missed.
2. **Home for each.** For each one: the slot's boot buffer (fixed at boot,
   planned in `slotBootBytes`), the request's arena (`RequestPool`), a fixed
   per-connection ring or scratch in the slot, or "must stay dynamic" with
   the reason. Quote the bound that makes a fixed home possible (a Limits
   field, a protocol maximum such as SETTINGS_HEADER_TABLE_SIZE).
3. **Frames and write chunks.** What makes `rentFrame` fall back today
   (pool size, frame size above 512 bytes)? Should a frame above the slab
   size go to the request arena, to a larger slab class, or be split? What
   happens to a frame that cannot get memory: backpressure (wait) or
   failure? Where is the wait, and can it deadlock?
4. **HPACK.** The decoder's dynamic table is bounded by
   SETTINGS_HEADER_TABLE_SIZE (4096 by default here). Pick: a ring buffer of
   that size in the slot, or something else. What about a peer that sends a
   larger table size update? Quote the code that enforces the size.
5. **Order and proof.** Which item removes the most of the remaining churn
   and load growth, so it goes first? What A/B (on `tools/rss-stability.sh`
   with `RSS_NO_THP=1`) and which counter prove each item did its work?
6. Anything that would make RSS grow or a bound lie.

## Output

Every finding must carry exactly one label, or it is discarded:

- [LOGIC]: holds regardless of any deployment fact.
- [PREMISE]: depends on a deployment fact; name the fact.

Quote at least one line of code for each claim about the code.
