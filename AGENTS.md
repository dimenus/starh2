# starh2

Server-side HTTP/2 stack shaped around Datastar. Design of record: `DESIGN.md`.

Zig agents: read `~/.claude/skills/zig/SKILL.md` and use `zigstd` for stdlib lookups. Do not guess 0.16 APIs.

## Gates

```sh
./zb build ci # definition of done (suite + exact + fuzz smoke + release targets)
```

Release targets included in `ci`: native ReleaseSafe examples via `release`, plus
`x86_64-linux-musl`, `aarch64-linux-musl`, and `aarch64-linux-gnu`. Individual
steps remain available (`test`, `test-exact`, `fuzz-*`, example names).

Interop commands and expected output are in `tools/README.md`. h2spec has exactly
the two published RFC 7540 priority exclusions in `tools/h2spec/EXCLUSIONS.md`;
do not weaken other failures into exclusions. The pinned Darwin h2spec needs
`GODEBUG=tls13=1`. Linux musl RUN proof uses a privileged container (io_uring);
see `tools/README.md`.

## Ownership and concurrency

- `Session` is the deterministic protocol authority. `Connection` serializes
  Session access with `session_mu`; handlers communicate through commands.
- ReadPump and WritePump are the sole owners of their socket directions. TLS
  state remains actor-owned; never share a tls.zig Connection with a pump.
- All production wire output passes through `FairScheduler`'s sink. Preserve
  the `test_queue_wire_bypass == 0` mutation canary.
- Outbound accounting distinguishes pending body bytes from framed wire bytes.
  Pending bytes release when the scheduler accepts the frame; wire/control
  occupancy releases only on WritePump completion.
- Cancellation ownership transfers to the reaper only when a join handle
  exists. Shutdown waits until every handler slot is released, not merely
  `live_handlers == 0`, and drains completions through `releaseSlot`.

## TLS and allocation traps

TLS is pinned in `build.zig.zon` to the `starh2-nonblock-v1` archive of
`dimenus/tls.zig` (URL + content hash; see `tools/lock.json`). The source-visible
patch remains at `vendor/tls-zig-nonblock-v1.patch`. If the fork or patch
changes, update the zon URL/hash, fork commit, and patch SHA-256 together.

- TLS 1.3 plaintext scratch is 16 KiB plus the inner content-type byte.
- `tls_edge.firstRecord` deliberately feeds one record per decrypt call. Without
  it, a coalesced small record followed by a maximum record can advance the
  cipher sequence and then fail for insufficient remaining output space.
- The TLS receive accumulator is reserved at connection boot and bounded by
  `Limits.tls_recv_acc_bytes`; do not replace `appendSliceAssumeCapacity` with a
  hot-path growing append.
- Intent payloads, decoded headers, dispatch requests, scheduler leases, and
  tickets have explicit owners. Error paths must release exactly once.
- `std.testing.FailingAllocator` is not thread-safe; fail-index/counting tests
  intentionally use one executor. Never make those tests concurrent.
- Fuzz modules disable error tracing due a Zig 0.16 runner StackTrace type bug.
