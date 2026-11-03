# t-3759 step 4: shares under pressure, the connection-start and handshake buckets

You implement design step 4 of `tools/hardening-design.md` (v3). Read, in
order: `AGENTS.md`; the design in full, above all "The goal, stated as a
test", "The rule everything follows" (and "Pressure"), "The client table",
"Shares", "Admission and overload", "Tests" and "Decisions"; the full commit
messages of the series so far, which hold the decision record: 3fca452 (step
1), 6ea2fcf (step 2), d33e615 (step 7), 9cfa00e and 0fdbc59 (step 3), and the
t-3763 commits (std.Io concurrency retired). The design is settled. If it
cannot be built as written in your scope, STOP and report the options. Do not
choose a different design quietly.

Worktree `/Users/rsaunderson/Source/mine/starh2-wt-3737`, branch
`carmack/t-3737`. Tripwire: going down first parents from HEAD you reach the
last t-3763 commit with only brief commits ("Add the ...") between. If not,
STOP. Untracked `tools/dst-r*/`, `tools/pcap-r1/` and report files are not
yours.

## Scope

In:
- **Refusal by share under pressure**, for every row of the design's share
  table: an identity over its share is refused while the resource's server
  use is at or above the pressure mark (1/4); the IPv6 /48 aggregate the same
  way, on the same mark. The refusal for each resource is the one in the
  table (close at accept, RST_STREAM REFUSED_STREAM, H1 503, the existing
  outbound shed, identity encoding). `Charge.add` becomes the bounded CAS that
  returns whether it took the charge; each caller undoes the earlier list
  entries on refusal (step 3 left the server-refusal undo lines in place).
- **`request_bytes_per_connection` default 64 MiB -> 32 MiB**, and the boot
  validation `stream <= connection <= client <= server`.
- **The connection-start and TLS-handshake-start buckets** (burst 128, 16/s,
  gated on their own resource's pressure), on every endpoint, state kept in
  the client record.
- **Rate-only records**: a record whose only content is a not-yet-refilled
  bucket stays in the table and holds no charge; when the slab is full, the
  rate-only record with the oldest refill time is reused. A serial reconnect
  must not regain its burst.
- **The PROXY header waiter cap, decided by Ryan (task t-3820)**: the cap
  PARKS the accept loop (no close). One server-wide ceiling,
  `proxy_header_waiters` = 64. Each PROXY endpoint gets a reserved floor of
  64/(2N) (N = PROXY endpoints) and the other half is a shared pool; no
  endpoint borrows another's floor; boot error if PROXY endpoints exceed the
  ceiling. A separate explicit `proxy_header_timeout_ns` (a u64, not a
  sentinel; default 5 s = the preface deadline; validated <= the preface
  deadline; started at accept; covering only the header read; the TLS
  handshake-slot wait keeps the preface deadline). The reason for floors: a
  TLS PROXY endpoint holds its waiter slot until a handshake slot is free, so
  handshake load on one PROXY endpoint parks every other PROXY endpoint today.
- The refusal log line the design names (rate-limited, naming identity, its
  share of server use and the endpoint's mode).

Out: route shares (step 5), the DATA bucket and running-handler GOAWAY (step
6), pressure-scaled timeouts (step 8), observability beyond the refusal line
(step 9).

## Premises (settled)

- No external users; breaking changes are free; no shims.
- zio only; `./zb build std-io-gate` passes with an empty baseline.
- Check-then-reserve is accepted: concurrent admissions may pass a mark by the
  number in flight; tests assert bounds with that tolerance (the design says
  so).
- Every release below zero is refused, counted and logged; nothing panics on
  client or handler behaviour.

## Tests

- **One attack test per share-table row** on real sockets (h2c and TLS where
  both apply, HTTP/1.1 where it applies): one identity alone with many
  connections, serially and at once, holds at most its cap (1/4); a second
  identity is still admitted up to a full share during the attack. Each test
  fails on the last t-3763 commit and passes after, and prints its refusal counters, so an attack that never
  engaged is visible. Identities: 127.0.0.1 against ::1, IPv4 loopback
  aliases on Linux, PROXY v2 headers elsewhere; no test hook for identity.
- The NAT case at idle and at 30% use (what the design says is NOT promised,
  shown).
- A serial-reconnect test that the bucket does not refill on reconnect.
- The PROXY floors: a stalled TLS PROXY endpoint at its floor does not park a
  second PROXY endpoint; the shared half absorbs a burst; the header timeout
  is separate from the TLS-slot wait.
- Charge conservation after every test (step 3's run-time checks) and the
  soak extended with refusals.
- Mutation-test every refusal and every undo line; read why each mutant dies.

## Gates

As step 3: Mac and nachos ci once each on `git archive` exports; changed
binaries 5x direct per machine; nachos only in a new `~/src/s-3759-step4`;
time limits; fresh `--cache-dir`; never `rm -rf` a path built from variables;
never write the shared Zig cache by hand. Bench: the bounded CAS is on every
stream and task; 6 alternating pairs against the last t-3763 commit on the Mac
and on nachos, h2c / TLS / h1c, `failed == 0` read first. Do not tell the
orchestrator the tests pass; the orchestrator recomputes.

## Commit and report

One or more commits on top of the last t-3763 commit, below the brief commits
(rebase them on top, unchanged), each with a full decision-record message.
Never push. Report to `tools/t3759-step4-report.md` (not committed). Stop and
report on any design question.
