# Grader / interop commands (local)

Pins live in `tools/lock.json`. Held-out seeds stay outside this repo (`tools/held-out/README.md`).

## Build / unit

```sh
./zb build ci
./zb build test
./zb build starh2-conformance-server example-hello example-datastar-sse
./zb build starh2-conformance-server example-hello example-datastar-sse -Doptimize=ReleaseSafe
./zb build release   # x86_64-linux-musl + aarch64-linux-musl + aarch64-linux-gnu ReleaseSafe
```

## One-shot benchmark against http2.zig

`tools/bench-hendrik.sh` builds
[`hendriknielaender/http2.zig`](https://github.com/hendriknielaender/http2.zig)
at the checkout's current revision, then runs the common h2load harness. It
records that revision, verifies every arm serves the same 13-byte body, alternates
arm order, and rejects client-limited or collapsed runs. Every round reports
request-completion p50/p99 as well as throughput. The summary reports server CPU
per request, total server CPU, peak RSS, and RSS retained immediately before
shutdown.

```sh
# Default checkout: ../../oss/http2-zig-hendrik relative to this repo.
tools/bench-hendrik.sh -n 100000 -c 50 -m 10 -t 4 --rounds 3

# Any other checkout:
HENDRIK_ROOT=/path/to/http2.zig tools/bench-hendrik.sh \
  -n 100000 -c 50 -m 10 -t 4 --rounds 3

# Attribute the opponent in isolation, without producing Starh2 rows:
tools/bench-hendrik.sh -n 100000 -c 50 -m 10 -t 4 --rounds 3 --opponent-only
```

CPU/request covers the reachability probe, reported rounds, and the final
double-client-thread run used to reject client-limited results. The latency
percentiles come from h2load and measure complete one-shot responses; they are
not comparable to the server-timestamp-to-client SSE latency reported by
`tools/sse_bench/run.sh`.

The checkout needs its `boringssl` submodule initialized. The script builds into
an explicit `/tmp` prefix, so a newer Debug or unrelated binary cannot silently
replace the measured ReleaseFast server.

### What the opponent measures

Verified at `http2.zig` revision
`a2859f8c76a05c917e2a33162939721c45ca5d4f`:

- On Darwin, one `std.Io.Kqueue` task owns each accepted connection. That task
  reads and dispatches frames synchronously; handlers are not separate tasks.
- Request-ready stream indices enter a bounded per-connection queue. The
  connection calls a handler returning a complete `Response`, writes HEADERS
  and flow-controlled DATA into one buffered writer, then flushes when idle or
  full.
- Connection slots own fixed storage for 100 streams and use an open-addressed
  stream-id lookup. The benchmark supplies this storage, avoiding per-connection
  stream-storage allocation.
- TLS is outside the HTTP/2 core: the benchmark uses `http2-boring` with
  BoringSSL and requires ALPN `h2`.
- The response API is one-shot. It cannot represent a long-lived streaming
  response, so it is an opponent only for the one-shot benchmark; the SSE
  benchmark continues to use Go `net/http`.

This is not an identical stack comparison: starh2 uses its pure-Zig TLS fork,
while the opponent uses BoringSSL. The starh2 h2c arm remains in the same run so
the result shows whether the gap survives after removing starh2's TLS cost.

## Concurrent SSE benchmark against Go

```sh
STREAMS=200 SECONDS_RUN=10 INTERVAL=1 ROUNDS=3 tools/sse_bench/run.sh
```

The saturated arm multiplexes 200 streams over one TLS connection and offers
200,000 timestamped events/s. It alternates arm order, reports delivery latency
and event count together, reports each server's consumed CPU time per arm, and
samples both peak and retained RSS during the rounds. The client opens every
stream before starting the clock and discards a one-second warm-up by default,
so connection setup and ramp-up are excluded from event and latency results.
Override the discarded interval with `WARMUP=N`. Server CPU includes opening
and warm-up because it is sampled around the complete client invocation.

The script defaults Starh2 to two zio executors for this connection-heavy
streaming shape; override with `STARH2_EXECUTORS=N`. The benchmark server itself
keeps `.auto` when `--executors` is omitted, so the one-shot harness is not
silently constrained by the SSE-specific runtime topology. Set
`STARH2_EXECUTORS=auto` to restore that default here. Go uses its default
`GOMAXPROCS` unless `GO_MAX_PROCS=N` is set; setting both values to the same
number compares equal scheduler widths. Starh2 keeps zio task migration off:
repeated TLS connection churn can otherwise strand a socket task. Pass
`--task-migration` directly to `starh2-bench-server` only when reproducing that
upstream runtime failure.

## Isolated pipeline benchmark

`bench-pipeline` removes the socket, TLS, client, and lock-contention variables
from the one-shot path. It times response HPACK encoding, static and
dynamic-indexed request HPACK decoding, frame parsing, frame-plus-HPACK inbound
work, the deterministic `Session` request/response path, packed ticket
bookkeeping (already-signaled one, packed-6 handoff, parked wake), and an empty
task spawn/join using the same zio runtime shape as the bench server.

```sh
./zb build bench-pipeline -Doptimize=ReleaseFast -- -n 1000000 --rounds 5
tools/bench-hendrik-pipeline.sh -n 1000000 --rounds 5
```

The second command compiles `tools/hendrik_pipeline_bench.zig` directly against
the checkout selected by `HENDRIK_ROOT` (the same default as
`bench-hendrik.sh`). It prints the opponent revision and leaves that checkout
unmodified. Its `inline request core` starts from a parsed HEADERS frame and
includes stream lookup/lifecycle, HPACK decode and validation, direct handler
dispatch, response HPACK/framing, and fixed-buffer output.

Each row reports median ns/op, the full round range, successful allocator calls
per operation, and requested bytes per operation. Setup, dynamic-table seeding,
and warmup are excluded. The empty-task and parked-wake ticket rows use
`n / 100` iterations because they are intentionally much more expensive; zio's
runtime allocator is internal, so those allocation columns are `n/a`.

These numbers explain local CPU demand, not whole-server throughput. Starh2's
`Session request + response` includes frame parsing but stops at outbound
intents; the opponent's inline core starts after frame parsing but continues
through fixed-buffer framing. That asymmetry slightly favors starh2 in a direct
comparison. Neither row contains starh2's scheduler handoff, mutex contention,
TLS record work, or socket I/O, and concurrently overlapped phases must not be
added as though they were serial.

## One-shot packing and the remaining gap

`tools/oneshot-gap.md` is the troubleshooting guide: why a second HEADERS used
to start a new TLS record, which unit tests pin that, which micros are noise,
and what not to retry. The live packing oracle is:

```sh
tools/oneshot-phase-trace.sh
```

Packed drain turns at `-m 10` are about 0.17 TLS records per response. The
script exits 9 if that ratio exceeds 0.4 (one record per response again).
h2c concatenates the same drain-turn into one write chunk. Concat itself is
a few nanoseconds. HPACK, Huffman, and frame parse are not the live ~5× vs
http2.zig; the TLS residue is Connection lifecycle (actor + pumps +
AckDrainer + receipts). Protocol: `tools/oneshot-gap.md`.

The 30s TLS stall (DATA left in FairScheduler after a RST tombstone) is a
different defect. Method: `TLS_STALL_BRIEF.md`. It landed in `57359b7`.
`not-started` (`started < total`) is t-761.

## Linux musl RUN (container)

`x86_64-linux-musl` and `aarch64-linux-musl` ReleaseSafe binaries are compile-gated
by `./zb build release` / `ci`. A RUN against h2spec + multiplex-grader needs a
Linux kernel with working io_uring (privileged container on Colima/Docker).

QEMU amd64 containers on Apple Silicon return `error.SystemOutdated` from
io_uring (`NOSYS`) — that is not a starh2 failure. Prefer `--platform linux/arm64`
with an `aarch64-linux-musl` binary, or a real amd64 Linux host for the
x86_64-linux-musl deploy shape.

```sh
./zb build -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix zig-out-musl-arm starh2-conformance-server
docker run --rm --platform linux/arm64 --privileged \
  -v "$PWD/zig-out-musl-arm/bin/starh2-conformance-server:/srv/starh2-conformance-server:ro" \
  -v "$PWD/tools/multiplex-grader:/grader:ro" \
  golang:1.22-bookworm bash -lc '... start server, h2spec http2, go run grader ...'
# Expected: h2spec 93 pass + 2 published exclusions; multiplex pass true dials 1 sse_ok 100 morph_ok 200
```

## h2c

```sh
./zig-out/bin/starh2-conformance-server --mode h2c --bind 127.0.0.1:0
# stdout: {"ready":true,"mode":"h2c","protocol":"h2c","port":N}
curl --http2-prior-knowledge -H 'x-grader-nonce: n1' "http://127.0.0.1:$PORT/hello"
nghttp -nv "http://127.0.0.1:$PORT/hello" -H 'x-grader-nonce: ng1'
./tools/h2spec/h2spec http2 -h 127.0.0.1 -p $PORT -S -o 10
# Expected result: 93 pass plus only the two published RFC 7540 priority
# exclusions in tools/h2spec/EXCLUSIONS.md.
```

## TLS

```sh
openssl req -x509 -newkey rsa:2048 -keyout testdata/key.pem -out testdata/cert.pem -days 365 -nodes -subj '/CN=localhost'
./zig-out/bin/starh2-conformance-server --mode tls --bind 127.0.0.1:0 --cert testdata/cert.pem --key testdata/key.pem
curl -vk --http2 -H 'x-grader-nonce: tls1' "https://127.0.0.1:$PORT/hello"
# ALPN reject (expect TLS alert 120):
openssl s_client -connect 127.0.0.1:$PORT -alpn http/1.1 -servername localhost </dev/null
nghttp -nv --no-verify-peer "https://127.0.0.1:$PORT/hello" -H 'x-grader-nonce: ngtls'
# The pinned v2.6.0 Darwin release was built with Go 1.12, where TLS 1.3
# requires this compatibility switch. A modern independently rebuilt grader
# does not need it.
GODEBUG=tls13=1 ./tools/h2spec/h2spec http2 -h 127.0.0.1 -p $PORT -t -k -S -o 10
# Expected result: the same 93 pass plus two published exclusions.
```

## Multiplex + stalled-stream isolation

```sh
cd tools/multiplex-grader
go run . -mode h2c -addr 127.0.0.1:$PORT -sse 100 -morph 200
go run . -mode tls -addr 127.0.0.1:$PORT -sse 100 -morph 200
```

The grader requires one physical connection, opens 100 SSE streams, exhausts
one stream's window by dropping only its WINDOW_UPDATE frames, runs 200 morph
requests while all streams remain open, and verifies unaffected streams before
and after the burst. Success is a JSON result with `"pass":true`,
`"dials":1`, `"sse_ok":100`, `"morph_ok":200`, and `"probe":true`.

## HPACK oracle

```sh
cd tools/hpack-oracle && go run . > ../../tests/go_huffman_bytes.txt
```

This regenerates `tests/go_huffman_bytes.txt` from Go `x/net/http2/hpack`;
`tests/interop.zig` decodes every generated single-byte encoding.

## Fuzz

```sh
./zb build fuzz-frame --fuzz=100K
./zb build fuzz-hpack --fuzz=100K
./zb build fuzz-session --fuzz=100K
```

The fuzz modules disable error tracing because Zig 0.16.0's bundled fuzz
runner passes `builtin.StackTrace` to the incompatible `std.debug.StackTrace`
API when error tracing is enabled.

## h2spec binary

v2.6.0 darwin amd64 (Rosetta): `tools/h2spec/h2spec` from https://github.com/summerwind/h2spec/releases/download/v2.6.0/h2spec_darwin_amd64.tar.gz
