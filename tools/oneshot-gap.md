# One-shot TLS: packing, gates, remaining gap

How a second HEADERS used to start a new TLS record, how that is gated now,
and which micros are worth running. Re-derive every count; they move with
the machine.

## What changed

On TLS, one FairScheduler drain copies every control and ticketed DATA into
the boot-counted plaintext scratch (16 KiB) and encrypts that as one
`queueWire` input. `WireChunk.control_entries` is how many control-pool
slots that write still holds. Occupancy stays held until the write
completes; do not weaken the pool to buy a record.

A bool `control_entry` on the chunk was the defect. The sink flushed on the
second HEADERS so it could release one slot per chunk. One-shot TLS then
paid one record per response (`records/response = 1.00`) while SSE packed,
because SSE DATA is not a control.

h2c concatenates the same drain-turn into one write chunk. Unticketed DATA
is the immediate per-frame handoff unless the drain holds a complete-handler
receipt (`hold_unticketed`). Complete oneshots share one drain-turn receipt,
so their DATA joins — including after a 16 KiB overflow — and that receipt
rides the last `queueWire`. Joining unticketed DATA merely because the
scratch already held HEADERS hung lifecycle SSE shutdown.

## Live packing oracle

```sh
tools/oneshot-phase-trace.sh
```

Throughput-shaped (use these for the remaining gap, not wall waits):

- `tickets/handoff` — receipts coalesced into one `queueWire`. Complete
  oneshots attach **one receipt per packed write**, so this sits near **1**
  on purpose. Packing authority is `records/response`.
- `tickets/emit` — receipts flushed in one `drainEmit` turn
- `records/response` — TLS records per h2load success. Packed drain turns
  at `-m 10` sit well below **0.4**. **1.00** means a second HEADERS
  flushed the batch again. The script **exits 9** rather than reporting
  that as a result.
- `allocs/request` — counting-allocator calls on the server GPA (`--trace`)

Latency-shaped (overlapped across many concurrent streams; not throughput):
`block` / `hold` / `ack` / `resume`, and dispatch-sampled `spawn` /
`to_send` / `hpack`.

Cross-check `writes` against h2load `succeeded`, not tickets against
`succeeded`. Tickets now track packed writes. `jobs` is a dispatch
counter and includes `/trace`.

Use a dedicated 400k TLS run for packing, not the official interleaved
100k TLS+h2c harness. That harness is noisier than the path under test.

## Unit gates (what a nested-sink rewrite used to hide)

The drain-turn rules live in `src/edge/emit_batch.zig` so they can fail
without standing up a cipher. `tests/transport.zig` imports that module as
the public-surface canary.

| Edge | What fails if it comes back |
|---|---|
| Second HEADERS starts a new TLS record | three HEADERS join; `control_entries == 3` |
| Ticketed DATA counted as a control | HEADERS+DATA+HEADERS → entries 2, not 3 |
| Completion releases a bool's worth of occupancy | `ControlPool.release(60, 1)` after 3 reserves starves ordinary |
| Byte-only release drops no entry | `release(100, 0)` leaves `entries_held == 1` |
| Concat past 16 KiB | `wouldFit` false; flush then a new batch |
| Receipt on a non-last encrypt record | `lastRecordMeta` / `lastRecordFlush` zero tickets and do not flush |
| unticketed DATA batched from empty scratch | `frameIsBatchable` is false |
| oneshot DATA missing a drain-turn receipt | `frameJoinsInProgress` is true only when `hold_unticketed` |
| Packed ticket chain truncated or short | AckDrainer walk canaries (snap `next` before `complete`) |
| Scratch includes the TLS content-type byte | `max_plaintext + 1 == TLS_PLAINTEXT_SCRATCH_SIZE`, asserted in `drainEmit` |

`zig build test` still cannot reach the TLS encrypt loop. `./zb build ci`
includes `tls-smoke`. Packing itself is the phase-trace exit 9.

## Isolated CPU (not the remaining gap)

```sh
./zb build bench-pipeline -Doptimize=ReleaseFast -- -n 1000000 --rounds 5
tools/bench-hendrik-pipeline.sh -n 1000000 --rounds 5
```

`TLS pack 6 HEADERS+DATA` is a few nanoseconds and zero allocs. Do not
spend another round on the memcpy. Isolated Session request+response now
sits close to http2.zig's inline core (~630 ns vs ~567); empty zio
spawn+await is what complete handlers no longer pay.

Huffman is closed: a comptime prefix trie in `huffDecode` matches
http2.zig `decodeBounded` (Darwin Chrome UA 658 vs 653 ns). Mixed HPACK
86 vs 16 ns is required dynamic-table copies vs views (~70 ns), not an
accidental memcpy. Frame 2.7 vs 0.3 ns is not the same job — they parse
the 9-byte header only. Ticket bookkeeping: packed-6 ~154 ns handoff
(~26 ns/req); parked wake ≈ empty spawn (~2 µs) and is the write
self-clock. Re-derive; do not cite a ratio.

Official one-shot:

```sh
./zb build bench -Doptimize=ReleaseFast -- -n 100000 -c 50 -m 10 -t 4 --rounds 3
tools/bench-hendrik.sh -n 100000 -c 50 -m 10 -t 4 --rounds 3
```

Keep `tools/sse_bench/phase-trace.sh` as the paired axis. Drain-turn
coalescing is what paid SSE; do not undo it to buy one-shot.

## Tried and rejected (do not retry without new evidence)

- Skipping the one-shot ticket wait collapsed ~176k → ~1.6k req/s with a
  lock-convoy `block` around 10 ms. The receipt is the self-clock, same as
  SSE.
- Running a complete handler on the actor *while still spawning per
  response and drain-emitting per job* dropped ~176k → ~123k: encode left
  the parallel handler tasks and serialized onto the actor. The landed cut
  is different: complete handlers skip spawn, encode the whole inline
  batch, one `drainEmit`, then wait one drain-turn receipt (not one
  ticket per response, and not a skipped wait). Do not re-try the first
  shape, and do not undo the batch wait.
- More packing micros, or TLS encrypt 6×50 B vs 1×300 B: live already
  packed (oracle exits 9 at `records/response > 0.4`). Packed drain turns
  at `-m 10` sit well below that.
- Starting by changing zio.
- View-decode Huffman plaintext or dynamic-table strings. Encoded bytes
  are not the string; a later block can evict a dynamic entry while a
  request still holds the field. Static-table strings may be borrowed.
- Parsing only the 9-byte frame header. Starh2's `ingestOne` is the
  resumable parser plus payload-lifetime union; that is the production
  job.
- Folding AckDrainer into WritePump. Darwin oneshot `--trace`: ack-drain
  56 µs → 0.2 µs, but TLS fell ~369k → ~307k req/s and resume/write waits
  grew. Completing tickets on the write task delayed the next getOne;
  the third task was overlapping the next write, not serial waste.
- Skipping the completion-channel hop for complete handlers (actor
  `releaseSlot` after the batch wait). Darwin official stayed ~283k TLS /
  ~505k h2c (t-788 was ~288k / ~493k); packing unchanged. The hop is not
  the live gap. A skip keyed on `defer_receipt` stranded SSE shutdown
  (lifecycle hang); do not retry.

## What is left

Live one-shot is no longer an alloc, spawn, HPACK, Huffman, parse,
h2c-unpacked-write, or per-response ticket-wait story. Complete oneshots
wait one drain-turn receipt (`tickets/handoff` near 1; packing is
`records/response`). Re-run `tools/oneshot-phase-trace.sh` and the
official bench; they move with the machine.

After h2c concat, the live gap vs http2.zig is TLS. Isolated
Session/HPACK are tied. Re-derive the official numbers; the shape on
Nachos after this cut was h2c catching TLS’s packing (CPU/req a few
microseconds) while TLS still paid several extra microseconds on top.
Sol Extra High’s “TLS is not the first remaining suspect” applied when
h2c was still unpacked; retire that sentence.

The stacks terminate TLS differently. http2.zig’s bench uses BoringSSL
via `http2-boring`; `serveConnection` gets already-decrypted
reader/writer and coalesces `SSL_write`. starh2 uses patched
`dimenus/tls.zig` — AES-GCM from `std.crypto`, not a homerolled AES —
actor-owned, encrypt under `session_mu`, then memcpy ciphertext into a
write chunk. Do not treat the TLS tax as “Zig AES vs C AES” until
`connectionEncrypt` of a packed oneshot is timed against AES-GCM of the
same bytes and against h2c write of the same bytes. Switching to
BoringSSL would make a third owner of the socket unless it sits behind
the same byte-transform ABI.

Do not name the remaining HTTP/2 residue “one actor per connection” —
http2.zig also has one connection task; theirs is a single
read/dispatch/write/flush loop. Their TLS throughput is a narrower
contract (handler or blocked write stops ingest). We keep the three
starve stories: other connections, handler work vs actor, peer stops
reading.

Reopen HPACK/parse only if an on-CPU h2c profile shows
ingest+HPACK+Session ≥ 1 µs/req or 15% CPU. Sol Extra High picked A on
those terms.

Do not drop scheduler pending without RST-on-wire or fail-close. That
is the TLS stall in `TLS_STALL_BRIEF.md` / `57359b7`, a different
defect. `not-started` (`started < total`) is t-761, also different.
