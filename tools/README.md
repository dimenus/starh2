# Grader / interop commands (local)

Pins live in `tools/lock.json`. Held-out seeds stay outside this repo (`tools/held-out/README.md`).

## Build / unit

```sh
./zb build test
./zb build starh2-conformance-server example-hello example-datastar-sse
./zb build starh2-conformance-server example-hello example-datastar-sse -Doptimize=ReleaseSafe
./zb build starh2-conformance-server -Doptimize=ReleaseSafe -Dtarget=aarch64-linux-gnu
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
