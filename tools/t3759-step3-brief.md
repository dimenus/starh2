# t-3759 step 3: account records, the client table, client identity, PROXY v2

You implement design step 3 of `tools/hardening-design.md` (v3). Read, in
order: `AGENTS.md`; the design in full, above all "The rule everything
follows", "The client table", "Client identity (Q1)", "Tests" and "Decisions";
the full commit messages of 3fca452 (step 1), 6ea2fcf (step 2) and d33e615
(step 7, HTTP/1.1 onto links), which hold the decision record and the review
rounds; `tools/t3759-inventory.md` and `tools/t3759-research.md` for the
resources and what other servers do. The design is settled. If it cannot be
built as written in your scope, STOP and report the options. Do not choose a
different design quietly.

Worktree `/Users/rsaunderson/Source/mine/starh2-wt-3737`, branch
`carmack/t-3737`. Tripwire: `git log --first-parent --format=%h` from HEAD
must reach `d33e615` with only brief commits (subjects starting "Add the")
above it. If not, STOP. Untracked `tools/dst-r*/` and report files are not
yours; leave them alone.

## Scope of this step

In: the account records and their charge/release discipline; the client
table; client identity per endpoint (`.peer`, `.proxy_protocol`, `.none`);
PROXY protocol v2; run-time conservation checks. Every resource the design's
share table lists is CHARGED to the connection, client, aggregate (IPv6) and
server accounts from this step on.

Out (later steps, do not build): refusing by share or pressure, the
connection-start and handshake buckets, rate-only records and their eviction
(step 4); route accounts and route shares (step 5); DATA bucket changes,
running-handler GOAWAY (step 6); pressure-scaled timeouts (step 8);
observability beyond what a test needs (step 9). The existing server-wide
limits keep deciding refusals in this step. If a later step's shape forces a
choice now (for example the record layout must hold bucket state), make room
for it and say so; do not build it.

## Premises (settled; do not argue them)

- No external users; breaking changes are free; no shims.
- zio primitives only (`./zb build std-io-gate`). Read a dependency's source
  before using it (`zig-pkg/<name>/src`); for Zig 0.17 read
  `~/.claude/skills/zig/SKILL.md`.
- Ryan's lifetime rule: minimise borrowing; scoped bulk lifetimes. A charge is
  a small fixed list of record indices taken at admission, released by walking
  the same list. The link and gate from steps 2 and 7 stay the only way a
  handler reaches the connection.
- A process-wide panic is never the answer to client or handler behaviour. A
  release below zero is a bug: refuse the subtraction, count it, log it, free
  nothing on its strength (the design's "Underflow" rule). Conservation checks
  at `deinit` run in every build (step 2's rule).
- Identity: exactly the design's Q1 section. IPv4 per address; IPv6 by /64
  plus a /48 aggregate (configurable); v4-mapped addresses are already `.ip4`;
  `.none` on Unix sockets; PROXY v2 only, only with a non-empty trusted source
  list (boot error otherwise) or on a Unix socket; untrusted source closed at
  accept; TLVs ignored; the header capped small and closed over it; `LOCAL`
  to one bounded infrastructure account per listener; the trusted check
  first, then a small cap of connections waiting for the header (not the TLS
  handshake slots), and the preface deadline covers the wait. No
  X-Forwarded-For. `Request.Peer.unknown` is one shared identity.
- The table: a fixed slab of records sized at boot, an open-addressing index
  with backward-shift deletion and a boot-random hash seed, records that never
  move, a generation per record, one `zio.Mutex` for lookup-or-insert with the
  first reference and for release-to-zero with removal, lock-free CAS for other
  charges. Capacity as the design says; an insert that still fails refuses the
  connection and counts it; never admit uncounted.

## Tests

- **Conservation, the biggest risk.** After every test in the suite, the slab
  holds no charged record and every account is zero (the run-time check, not
  a test-only assertion). A soak over h2c, TLS and HTTP/1.1 with random RST,
  close, GOAWAY and shutdown timing, against handlers that honour and ignore
  cancel (orphans), asserting zero at the end; it covers same-key delete and
  reinsert, and a record reused with a bumped generation.
- **Identity**: 127.0.0.1 against ::1 for the plain path (no test hook for
  identity); PROXY v2 headers for the rest: v4, v6 (two addresses in one /64
  are one client, two /64s in one /48 share the aggregate), `LOCAL`, an
  untrusted source closed at accept, an over-size header closed, a header that
  never finishes held only to the preface deadline and only inside its own
  cap, `.none` on a Unix socket, a boot error for `.proxy_protocol` with an
  empty trusted list.
- **Underflow**: a forced double release is refused, counted and logged; no
  panic; nothing freed.
- **Orphans stay charged**: a handler that outlives its connection keeps its
  client, aggregate and server charges until it returns; only the connection
  charge goes at teardown.
- Every new test must fail on d33e615 (or not compile there, when it tests a
  new API; say which) and pass after. Mutation-test every release and every
  undo line: delete each in turn and show a test fails; read why.

## Gates and evidence

- Mac: `./zb build ci` on a `git archive` export with a fresh `--cache-dir`.
  Known failures: the two `macos-libcxx-probe` steps (t-3540), occasional
  `deadlines T2/T3`, `compression I4`, `t3567` idle table,
  `outbound_budget order (h2c)`, `t3562` half-close. Report every failure with
  its output anyway.
- nachos (Linux): only in a new directory `~/src/s-3759-step3` (copy `zig-pkg`
  and `testdata/*.pem` from `~/src/s-3759-spike`; `git archive <sha> | ssh
  nachos 'tar -x -C ~/src/s-3759-step3'`; `export
  PATH="$HOME/.local/share/mise/shims:$PATH"` first). Touch no other directory,
  run no bench there, change no system setting, kill only your own PIDs.
- Fresh `--cache-dir` per test binary, exactly one binary, run directly at
  least 5 times on each machine (zig caches passing runs). Time limits on every
  build and run (`perl -e 'alarm shift; exec @ARGV' <s> ...` on the Mac,
  `timeout` on nachos). Never block a zio executor thread in a test client.
- Cross-compile every test binary for `x86_64-linux-gnu` before nachos.
- Bench: the charge walk is on the hot path (every stream and every task).
  Run at least 6 alternating pairs of d33e615 against your commit on the Mac
  for the h2c, TLS and h1c arms (`-Doptimize=fast`), `failed == 0` read first.
  If the cost cannot be separated from noise, say so with the numbers.
- Do not regenerate any golden or expected-output file. Do not tell the
  orchestrator the tests pass; the orchestrator recomputes.

## Commit and report

One commit on top of d33e615, below the brief commits (rebase them onto it,
unchanged). Full decision-record message: problem, changes, benefits,
alternatives rejected, what it depends on, statistics, and every cross-scope
pointer you keep with the reason. Never push. Never touch other worktrees or
branches.

Report to `tools/t3759-step3-report.md` (not committed): the commit; each test
with its result on d33e615 and on your commit; ci on both machines; the
mutation table; the bench table; every place the design did not fit; every
corner cut; open questions. Label anything not measured as inference.
