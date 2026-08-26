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
- **Never guess a dependency's contract; read its source.** Every dependency
  is vendored at `zig-pkg/<name>/src` precisely so its implementation is one
  `Read` away. Before calling an API, read the function body and its callers
  in the dependency's own tests, not just the doc comment — the t-878 cut
  found a wake race in `zio.CompletionQueue`'s future protocol that the doc
  comment does not mention and only the implementation shows. A delegation
  brief must name the exact dependency files to read, and a delegate that
  used an API it did not read has not finished its brief.

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
tools/sse_bench/run.sh          # concurrent SSE against Go net/http, Kestrel, hyper
tools/sse_bench/mixed.sh        # SSE + oneshot on one TLS connection; STALL=0 skips the blocked reader
tools/sse_bench/burst-probe.sh  # 200 SSE streams opened at once on one TLS connection, 10 rounds; fails on any silent fail-close
tools/sse_bench/phase-trace.sh  # where one flushed event spends its time
tools/perf-story.sh             # the whole two-OS characterization matrix in one run
```

## Review recall — is an agent reviewer worth a pre-push gate?

`tools/review-recall/` measures whether a model reviewing one commit reproduces
the findings CodeRabbit raised on the same commit. The oracle is 13 transcribed
findings on lalinsky/zio PRs 711-713 (`findings.json`); the corpus is the eight
reviewed commits extracted with `git archive`, so an arm has no history to reach.

```sh
tools/review-recall/corpus.sh          # build the isolated corpus; aborts if it leaks an answer
tools/review-recall/run-all-cursor.sh  # every cross-vendor arm over every commit
tools/review-recall/judge-all.sh       # two vendor-disjoint judges per run, agreement required
tools/review-recall/score.py           # recall per severity band, plus the unmatched load
```

Re-derive the recall numbers with these; do not cite a number from prose.

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
endpoint, so `handshakeTls`, `TlsPump`, and leftover-preface drain never run
under the suite. That is measured, not assumed: an always-false assert inside
the TLS handshake left the suite green and aborts on the first curl request.
`tls-smoke` is therefore the only gate that covers that code, and it is in
`ci`.

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
- ReadPump and WritePump are the sole owners of their socket directions on h2c.
  On TLS, `TlsPump` is the sole SSL_read/SSL_write owner; never share an SSL
  object with a second task. `CipherRead` posts ciphertext and never touches
  SSL. `session_mu` covers Session and FairScheduler, not the cipher.
- All production wire output passes through `FairScheduler`'s sink. Preserve
  the `test_queue_wire_bypass == 0` mutation canary.
- Outbound accounting distinguishes pending body bytes from framed wire bytes.
  Pending bytes release when the scheduler accepts the frame; wire/control
  occupancy releases only on WritePump completion.
- Cancellation ownership transfers to the reaper only when a join handle
  exists. Shutdown waits until every handler slot is released, not merely
  `live_handlers == 0`, and drains completions through `releaseSlot`.

## TLS and allocation traps

TLS is BoringSSL via `hendriknielaender/boring` v0.1.1 (`build.zig.zon`).
HTTP/2 writes plaintext into a `std.Io.Writer`; flush is SSL_write. Pass
`-Dboringssl-source-path`, or place a checkout at `vendor/boringssl`, or use
the sibling `../../oss/http2-zig-hendrik/boringssl`. The expected BoringSSL
revision is in `tools/lock.json`. `tools/build-boringssl.sh` overlays the
fetched boring package so zig-cc glibc headers do not -Werror memchr on
`aarch64-linux-gnu`. Do not wrap a record API beside this stream.

- `TlsPump` is the sole SSL_read/SSL_write owner. Concurrent ReadPump+WritePump
  on one SSL object is a data race; h2c keeps the dual pumps. TLS ciphertext
  arrives through a dedicated read task posting to a cipher queue; outbound
  frames use `write_ch`. The pump waits on an Event after tryGet of both
  (no Select). The SSL object's BIOs are a bounded memory pair
  (`BIO_new_bio_pair` in `src/edge/tls.zig`); do not restore socket-coupled
  BIO callbacks. Handshake still runs on the actor (blocking SSL_accept over
  the same memory BIOs) with the read task already the ciphertext source.
- Handshake leftover plaintext (a pipelined preface) is ingested on the actor
  before `TlsPump` starts.
- Packing stays in `emit_batch` (16 KiB concat). Do not put a record loop back
  on the actor.
- Per-connection TLS Io buffers are bounded by `Limits.tls_stream_bytes`
  (floor `TLS_CONN_BUFFER_BYTES` = tcp in/out + BioPair). The ciphertext
  chunk pool and work queue are separate declared terms
  (`tls_cipher_chunks_per_connection`).
- Intent payloads, decoded headers, dispatch requests, scheduler leases, and
  tickets have explicit owners. Error paths must release exactly once.
- `std.testing.FailingAllocator` is not thread-safe; fail-index/counting tests
  intentionally use one executor. Never make those tests concurrent.
- Fuzz modules disable error tracing due a Zig 0.16 runner StackTrace type bug.
