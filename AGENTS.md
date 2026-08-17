# starh2

Server-side HTTP/2 stack shaped around Datastar. Rationale lives in the git log
(initial commit and the design-retirement commit).

- **Search before you reinvent.** qmdsync indexes this repo (documents plus
  session transcripts) and the task store — sources grep cannot read. Run
  `qmdsync find "<topic>" --repo starh2`, or omit `--repo` to search every
  synced repo; `qmdsync repos` shows valid scopes. Worth it before proposing a
  design that may already be decided, before filing a task, and when a constraint
  here has no stated reason.
- Zig agents: read `~/.claude/skills/zig/SKILL.md` and use `zigstd` for stdlib lookups. Do not guess 0.16 APIs.

## Premises — treat these as settled, and say so when you delegate

State these in any brief you hand to another agent or reviewer. A reviewer
cannot tell which of your constraints are real, and it will argue a false one
with more confidence than a true one, costing a whole round.

- **This stack is malleable and not in real use.** There are no external
  consumers. A breaking change costs its lifetime minimum today and rises from
  here. Do NOT weigh backward compatibility, do not add a compatible shim beside
  the better design, and do not defer a redesign to protect a caller that does
  not exist. Propose the breaking change, name what it breaks, and land it.
- **Prefer share-nothing.** A task should own its state outright and exchange
  messages, rather than share state behind a lock. Where this stack shares, it
  has paid for it twice: `session_mu` covers `Session`, the `FairScheduler` and
  the TLS connection together, and that single domain is both the measured
  throughput ceiling of one connection and the site of the ownership violations
  fixed in `cad283d`. When a design needs a new mutex, that is the signal to ask
  which task should have owned the state instead.
- **Do not fight `std.Io` or `zio`.** Use the grain: a buffered writer with an
  explicit `flush`, `std.Io` ownership and cancellation, zio's tasks and
  channels. Building parallel machinery beside the standard shape is how a
  contract ends up stated in a comment and contradicted by the code. If the
  grain genuinely does not fit, say which API and why, in the commit message.

Re-derive the performance claims above rather than citing them; the numbers move
with the machine:

```sh
./zb build bench -Doptimize=ReleaseFast -- -n 100000 -c 50 -m 10 -t 4 --rounds 3
tools/bench-hendrik.sh -n 100000 -c 50 -m 10 -t 4 --rounds 3 # builds + identifies the Zig opponent
./zb build bench-pipeline -Doptimize=ReleaseFast -- -n 1000000 --rounds 5 # isolated CPU/allocation costs
tools/bench-hendrik-pipeline.sh -n 1000000 --rounds 5 # same isolation against the identified opponent
tools/oneshot-phase-trace.sh    # packing + alloc oracle; exits 9 if records/response > 0.4
tools/sse_bench/run.sh          # concurrent SSE against Go net/http
tools/sse_bench/phase-trace.sh  # where one flushed event spends its time
```

## Gates

```sh
./zb build ci # definition of done (suite + exact + fuzz smoke + TLS gate + release targets)
```

Release targets included in `ci`: native ReleaseSafe examples via `release`, plus
`x86_64-linux-musl`, `aarch64-linux-musl`, and `aarch64-linux-gnu`. Individual
steps remain available (`test`, `test-exact`, `fuzz-*`, `tls-smoke`, example names).

```sh
./zb build tls-smoke # TLS edge: fresh curl connections + clean shutdown
```

**`zig build test` cannot reach the TLS edge at all.** No test binds a `tls_h2`
endpoint, so `tlsHandshakeViaPumps`, `driveDecrypt`, and the TLS branch of
`queueWire` never run under the suite. That is measured, not assumed: an
always-false assert inside `driveDecrypt` leaves the suite green and aborts on
the first curl request. `tls-smoke` is therefore the only gate that covers that
code, and it is in `ci`.

It uses curl because the oracle must share no code with the stack under test;
nghttp2 is strict about frame order and pipelines its preface, which is the
client shape both t-538 defects needed. It checks the server STOPS on SIGTERM,
not only that it answers — a stranded handler blocks `shutdownHandlers` while
every response still looks perfect on the wire. Mutation-proven: making the TLS
encrypt loop never mark its last record keeps all 126 tests green and fails
`tls-smoke` in 10 s. Needs curl with HTTP2; it ABORTS rather than skipping if
that is missing, because a skipped TLS gate reads as a pass.

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
