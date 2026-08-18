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
- `inbound_records/req` — TLS records decrypted (`firstRecord` loop).
  Packed h2load at `-m 10` sits near **0.10**, same shape as outbound.
  **1.00** would mean one inbound record per response.
- `encrypt` / `decrypt` / `sendWire` ns/req — `Clock.awake` around
  `connectionEncrypt`, `connectionDecrypt`, and `sendAccountedWire`.
  Throughput-shaped; compare to isolated cipher rows, not to overlapped
  `queue_ns`.
- `ingest` / `intents` ns/req — `Session.ingest` then `processIntents`
  under `session_mu` (TLS: inside `driveDecrypt`; h2c: the plaintext
  chunk path).
- `accAppend` / `accCompact` ns/req — socket chunk into `tls_recv_acc`,
  then leftover memmove after `firstRecord`. Outside the decrypt clock.
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

The TLS cipher is not the live tax. `bench-pipeline` now times
`std.crypto` AES-GCM and `connectionEncrypt` of a 64-byte inbound record
and a 300-byte packed outbound record. Re-derive; on Darwin both sit in
the low hundreds of nanoseconds, and tls.zig's record wrapper is tens of
nanoseconds over AES-GCM, not microseconds. Packed outbound / 6 is tens
of nanoseconds per response. Do not retry packing 6×50 vs 1×300, and do
not treat BoringSSL vs tls.zig as a cipher-speed story until an on-CPU
TLS profile shows encrypt/decrypt as the slice.

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
  at `-m 10` sit well below that. Inbound is packed the same way
  (`inbound_records/req` ≈ 0.10); do not treat `firstRecord` as a 1:1 tax.
- Treating `connectionEncrypt` / AES-GCM as the several-µs TLS tax.
  Isolated and live clocks are tens of nanoseconds per request.
  `sendAccountedWire` is ~0.2 µs/req on TLS and h2c. Official 100k CPU/req
  was 2048-bit RSA handshake (~17 ms user each, 50 conns) amortized over
  the request budget. ECDSA P-256 testdata is the recipe in
  `tools/README.md`; do not regenerate `rsa:2048` for a throughput run.
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
- Treating the process-global `test_observed_*` / `test_wire_sends` RMWs
  as a 0.10 µs chip. Sol Extra High ranked that first. Nachos ABA at
  `-n 100000 -c 50 -m 10 -t 16 --rounds 3`, ReleaseFast, P-256, counters
  on vs compiled out: TLS CPU/req 4.90 / 4.77 / 4.76 µs (on, off, on
  again); h2c 4.463 / 4.464 / 4.467 µs. The on-vs-off TLS delta is
  0.06 µs and smaller than the on-vs-on-repeat spread (0.14 µs).
  Throughput stayed inside a few percent with no consistent winner.
  The counters still compile out of ReleaseFast (`-Dobserve=true` puts
  them back); they are not the remaining gap.
- Treating `tls_recv_acc` append/compaction as the TLS-minus-h2c residue.
  400k packed `--trace` (Darwin and Nachos): append 8 ns/req (16.3 B),
  compact 0 ns/req (leftover/record ≈ 0 — `firstRecord` consumes the
  whole chunk). Outside the decrypt clock, and two orders below 0.10 µs.
- Treating complete-handler reaper reserve as a 0.10 µs chip. Sol Extra
  High ranked that second (then leftover after #1 and #3). Complete
  oneshots skip `tryReserveReaper`; 404/405 and task handlers still
  take a token. Nachos ABA at `-n 100000 -c 50 -m 10 -t 16 --rounds 3`,
  ReleaseFast, P-256, keep/skip/keep: TLS CPU/req 4.72 / 4.75 / 4.62 µs
  (user 3.56 / 3.61 / 3.40); h2c 4.37 / 4.42 / 4.35 µs. Skip-vs-keep
  TLS is 0.03 µs, smaller than keep-vs-keep (0.11 µs). Throughput had
  no consistent winner (TLS 2.14M / 2.10M / 2.17M). Darwin t=4 was the
  same shape. The skip stays as correctness: complete must not consume
  the cancellation budget SSE needs. The numeric
  `cancellation_reaper_jobs >= max_streams_per_server` bound is
  unchanged; a config cannot promise the mix.

## What is left

Live one-shot is no longer an alloc, spawn, HPACK, Huffman, parse, or
h2c-unpacked-write story. Complete oneshots share one drain-turn receipt
(`tickets/handoff` near 1; packing is `records/response`). The actor no
longer `waitTicket`s that receipt: pending complete-batch receipts are
depth 2, AckDrainer reports, the actor consumes already-complete tickets.
Re-run `tools/oneshot-phase-trace.sh` and the official bench; they move
with the machine.

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
write chunk. Isolated `connectionEncrypt` is tens of nanoseconds over
AES-GCM of the same bytes. Switching to BoringSSL would make a third
owner of the socket unless it sits behind the same byte-transform ABI.

Live `--trace` 400k packs inbound and outbound at ~0.10 records/req on
Darwin and Nachos, so `firstRecord` is not a 1:1 tax. Nachos clocks:
encrypt 28 ns/req, decrypt 30 ns/req, `sendAccountedWire` 111 vs 107 ns
(h2c), ingest 1119 vs 1187 ns. The record-layer cipher is not the tax.

Official 100k CPU/req was handshake. testdata was 2048-bit RSA;
tls.zig spent ~17 ms user per handshake. Fifty connections amortized
over 100k requests is ~8 µs/req — the whole TLS-minus-h2c gap. The same
50 handshakes over 1M requests left ~0.8 µs/req. ECDSA P-256 cut
handshake user from ~16 ms to ~0.6 ms (n=50) and 100k TLS CPU/req from
12.4 µs to 5.2 µs, next to h2c at 4.8 µs. Official Nachos 3-round after
P-256 testdata: TLS ~2.05M at 4.81 µs CPU (user 3.60, sys 1.21) vs h2c
~2.21M at 4.56 µs (user 3.43, sys 1.13). Record-layer residue ~0.25 µs.
p99 is hundreds of µs on both arms. `tools/bench.zig` prints user vs
sys so a future RSA cert cannot hide as “TLS”. The remaining gap vs
http2.zig is Connection lifecycle again, not AES, not packing, and not
handshake.

Do not name the remaining HTTP/2 residue “one actor per connection”.
http2.zig also has one connection task. The oneshot p50 vs Go was **who
waits the write**: keep (`67c96ab`) encoded complete oneshots on the
actor and `waitTicket`ed, so ingest of the next HEADERS waited on
WritePump. That wait moved off the actor (receipt kept; AckDrainer still
only reports). AES, packing, HPACK, and handshake are not that gap.

Darwin, 2026-08-18, interleaved 8 workers / 1 TLS connection /
`STARH2_EXECUTORS=2` / same Go client as `mixed.sh` oneshot-only,
3 s + 1 s warmup, 3 rounds. Keep is the `67c96ab` binary; pipeline is
this working tree. Keep-vs-keep p50 spread was 1 µs.

| arm | oneshot-only p50 | rps | err |
|---|---|---|---|
| keep `67c96ab` | 461–462 µs | 17.3k | 0 |
| pipeline | 205–211 µs | 36.5–37.4k | 1–2 |
| Go net/http | 202–212 µs | 37.1–38.8k | 0 |

`complete_receipt_depth_max` reached **2** on that 8-worker load
(`--trace` `/trace`) and on `tools/oneshot-phase-trace.sh` (h2load
`-n 400000 -c 50 -m 10 -t 4`). Packing: `records/response = 0.10`.
Do not retry a 50 µs actor sleep to buy SSE cadence.

Go `net/http` h2 (`h2_bundle.go` `serverConn.serve`) is the same three-way
split we keep for Datastar, cheaper: `readFrames` only reads; `serve`
never blocks on a handler or on TCP (stdlib comment: handlers wait for
their frames to hit the wire); `go runHandler` per stream. A tiny write
that fits the bufio buffer is inlined on `serve` (`staysWithinBuffer`);
otherwise `writeFrameAsync`. TLS is a `Conn` wrapper on that write, not a
third owner under `session_mu`. SSE `/updates` is a handler goroutine that
`Flush`es; a command is another `runHandler` that 204s. `writeSched.Pop`
skips a zero-window stream. GC is not the advantage.

http2.zig cannot be that opponent. Handler API is `fn(ctx) !Response` /
`setBody` — no flush, no stream to post onto after return, `ProcessStream`
is a documented no-op. A 204 oneshot they can do; a long-lived Datastar
`/updates` bus they cannot. Blocking in the handler to fake SSE would run
on the connection task and park ingest of the 204. Mixed.sh correctly
omits them.

A flow-control stall of one oneshot stream does **not** park their ingest.
Darwin, `a2859f8`, TLS `GET /` (13-byte body), client
`SETTINGS_INITIAL_WINDOW_SIZE=8`, WINDOW_UPDATE withheld on stream 1:
stream 1 stuck at 8 bytes without `END_STREAM`; sibling `GET /` 50/50 in
9 ms, 99/99 in 18 ms (their cap is 100 streams; the stalled one occupies a
slot). Control (WU sent) finishes stream 1 at 13 bytes. Their unit test
`flush_ready_streams does not let a blocked stream stall other responses`
matches the wire. Evented I/O is real; a zero-window stream is skipped.
A client that stops reading the **socket** still fills TCP and blocks
`flush` for every stream on that connection — including Go and us.

`tools/sse_bench/mixed.sh` exercises starve stories 2 and 3 on one TLS
connection (32 SSE streams, 10 ms, 8 oneshot workers; Go opponent).
Darwin, 2026-08-18, `37f04e0` (actor-owned handler deadline heap on
`2612a13`), `STARH2_EXECUTORS=2`, 5 s + 1 s warmup, 3 rounds. Oneshot-only
stayed in the hot band: 35670 / 34753 / 33778 rps (p50 214 / 219 / 223 µs).
Mixed oneshot held 0.85× and p50 ≤ 300 µs: 34603 / 33804 / 32840 rps
(p50 226 / 230 / 234 µs). Mixed delivering 32/32 every round. Mixed SSE
did **not** hit 16,000: 10082 / 10658 / 10784. Go mixed was 16,000 every
round at 38325 / 37533 / 37360 oneshot rps. Stall: starh2 31/32
delivering, 10266 events, oneshot 33364 rps; Go 31/32, 15502 events,
oneshot 38105 rps. SSE-only at the same STREAMS/INTERVAL/SECONDS
(`tools/sse_bench/run.sh`) hit 16000/16000 — idle `waitForActivity`
ticks the heap. Packing after this cut:
`tools/oneshot-phase-trace.sh` `records/response = 0.10`,
`complete_receipt_depth_max = 2`. T1/T2/T3 are green; `/sse` uses
`waitUntil`, not `zio.sleep`. The 10 ms mixed cadence is still the miss,
not a silent drop. Do not claim 16k from this run. Do not satisfy the
remainder with a 50 µs actor sleep, yield-every-turn, zio#687, or by
softening oneshot.

Sol Extra High ranked legal chips inside that split: (1) gate test
counters — measured, below the bar; (3) clock inbound accumulator
append/compaction — 8 ns/req append, compact never on packed oneshot;
(2) skip complete-handler reaper reserve — measured, below the bar,
kept so complete oneshots do not take cancellation tokens. Wakes have
no legal deletion. `session_mu` hold is uncontended on a complete-only
load. Outbound ciphertext copy is already inside `sendAccountedWire`
(~111 ns). Those legal chips are done. A oneshot ABA still cannot price
the split; the mixed run above can.

Reopen HPACK/parse only if an on-CPU h2c profile shows
ingest+HPACK+Session ≥ 1 µs/req or 15% CPU. Sol Extra High picked A on
those terms.

Do not drop scheduler pending without RST-on-wire or fail-close. That
is the TLS stall in `TLS_STALL_BRIEF.md` / `57359b7`, a different
defect. `not-started` (`started < total`) is t-761, also different.
