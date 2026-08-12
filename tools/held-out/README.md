# Held-out grader interface

Exact fixture bytes and seeds remain outside this repository.

## Reproducible commands

```sh
# Conformance server (h2c)
./zb build starh2-conformance-server
./zig-out/bin/starh2-conformance-server --mode h2c --bind 127.0.0.1:0

# curl cleartext prior knowledge
curl --http2-prior-knowledge -D- "http://127.0.0.1:$PORT/hello" -H 'x-grader-nonce: n1'

# curl TLS (requires --cert/--key on server)
curl --http2 -vk "https://127.0.0.1:$PORT/hello" -H 'x-grader-nonce: n1'

# h2spec v2.6.0 (install separately)
h2spec -p $PORT -k --strict  # TLS
h2spec -p $PORT --strict      # after enabling h2c prior-knowledge in h2spec config

# Go oracle HPACK
cd tools/hpack-oracle && go run .
```

## Published h2spec exclusions (RFC 9113)

Only RFC 7540-only priority dependency semantics removed by RFC 9113, and
optional features this server did not advertise (server push), are excluded.
