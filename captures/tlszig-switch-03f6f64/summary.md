# tls.zig switch (03f6f64) against the boring build (897f33e), 2026-10-04

Mac (arm64, macOS 26, Zig 0.17.0, ReleaseFast). Raw output:
`~/Dropbox/starh2-captures/starh2-tlszig-03f6f64.tar.gz`. "boring" is the
0.17 tree just before the switch, built with the locally patched boring
v0.1.1 package; it ran as the `--opponent` arm of the same harness.

## conns50 (oneshot, 50 connections, record layer dominates)

`./zb build bench -Doptimize=fast -- -n 100000 -c 50 -m 10 -t 4 --rounds 6
--opponent <boring-arm.sh> --opponent-url https://127.0.0.1:18444/`
(arm order rotates every round). 3 complete runs, 18 rounds per arm:

| run | tls.zig req/s | boring req/s | tls.zig CPU/req | boring CPU/req |
|---|---:|---:|---:|---:|
| 2 | 847,630 | 841,975 | 16,975 ns | 17,263 ns |
| 3 | 822,883 | 841,657 | 17,772 ns | 17,082 ns |
| 4 | 821,965 | 844,635 | 17,442 ns | 16,835 ns |

Within about 2%. tls.zig was lower in 2 of 3 runs, and the Mac's 1-minute
load rose from about 2 to 5 across the runs. Run 1 aborted inside the
harness ("could not parse h2load request percentiles") before any round
printed. Every arm then served 100k/100k by hand, and the rerun was clean.

## Handshake cost (server CPU per full handshake)

An in-process microbenchmark: 2000 fresh handshakes, client offers x25519
only, ECDSA P-256 fixture certificate, timing the server's handshake calls
only. 3 runs each (method in the archive's `microbench-README.txt`):

- tls.zig: 109, 110, 111 us
- BoringSSL: 43, 43, 43 us (default config, which also issues session tickets)

So a tls.zig server handshake costs about 2.5x the CPU, and there is no
resumption to skip it. Shapes with one or a few connections per client
(the SSE benches, conns50) do not see this. Connection churn does: about
110 us of server CPU per new connection, against about 43 us.

End-to-end handshake runs (`hs.sh`, h2load with one request per new
connection) did NOT isolate TLS cost. The h2c arm, with no TLS at all, used
the same server CPU per connection as both TLS arms. Later pairs failed
requests after about 10k connections. They are in the archive, but are not
a result.

## ci on this tree

`zig build ci`, work stealing / pinned:
- Mac: 387/397 (10 skipped) / 380/380 + 17 cached. The only failure is the
  known `macos-libcxx-probe` step.
- nachos: the same counts, and all 136 steps pass.

The first nachos attempt failed when its 16 GB `/tmp` tmpfs filled up
("Quota exceeded").
