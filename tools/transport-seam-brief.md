# Review brief: a transport seam for starh2's stall tests

You are reviewing a DESIGN that is not built yet. Your working directory is the
starh2 repository (branch `carmack/t-3562`, HEAD `5a85cb3`). Read the code named
below; do not answer from this brief alone. Read-only: change nothing.

## Premises — settled; a finding that depends on their negation is out of scope

- starh2 is a Zig 0.17 HTTP/2 server (h2c and TLS). It is not in production and has
  no external consumers. Breaking changes and test rewrites are cheap; do not argue
  for compatibility.
- zio owns concurrency (coroutines, channels, a completion queue). The vendored copy
  is `zig-pkg/zio-0.19.0-xHbVVOZILQDHwvSkKoyueGl29__yoF4DRzNDIEzjgprv/` (upstream
  lalinsky/zio be65431). It has NO deterministic-simulation mode. A fork once had
  one (`tools/zio-dst`, pinned to dimenus/zio 913ffda); bringing it forward is a
  separate, later project and not an option for this decision.
- The owner's rule: test control belongs in the harness, not in production code.
  Two implementations of one contract (a real one and a test double) drift.
- A peer-side protocol oracle now exists (`tests/h2_oracle.zig`, fed by
  `tests/wire_client.zig`): it checks what RFC 9113 owes the client on every
  connection. The seam is meant to give it many more peer behaviors to judge.

## The problem (measured, not inferred)

Stall tests (`tests/t3562_terminal_bound.zig`) need the server's writer to stop and
STAY stopped while the actor keeps reading, so frames pile up in the scheduler's
rings. Over TCP loopback this did not hold on macOS: after the stall state was seen,
the h2c write pump still completed one or two write batches every 300 ms with a
client that read nothing (20 runs). So rounds 3 and 4 added test-only machinery to
production code: `wire_pump.test_hold_writes`, `connection.test_hold_tls_take`,
`test_hold_stash`, a `test_sndbuf_bytes` setter, a `test_stalled` gauge computed in
every `drainEmit`, `wire_pump.test_writing`, client "nudge" PINGs, and in round 4 a
1 ms poll in `Connection.nextDeadlineNs` that exists only because clearing a test
hold rings nothing. Find these with `grep -n test_hold\|test_stalled\|tlsTakeHeld
src/edge/*.zig`.

## The proposed design

1. The seam is a connected Unix `socketpair` the test owns, not a new interface in
   `Connection`. One end goes to the server; the test drives the other end with the
   existing `Wire` client and the oracle. A stall is the peer not reading.
   Expectation (an INFERENCE, not measured): a Unix socket pair has fixed buffers
   with no autotuning, so it holds a stall on both macOS and Linux.
2. Step 1 validates that before anything else: a probe in which the server writes
   into a socket pair whose other end reads nothing, checking that progress stops
   and stays stopped; h2c and TLS, 20 runs each, macOS and Linux.
3. One production addition: "serve an already-connected stream", running the same
   connection entry the accept loop uses (slot, admission, the real actor loop).
   `Connection` already takes a connected `std.Io.net.Stream` (see `BenchHop.open`
   and `serveAccepted` in `src/edge/connection.zig`, `connEntry` in
   `src/edge/server.zig`).
4. `Wire.fromFd`, and peer behaviors on the client: stop reading, half-close,
   flood, reset. The oracle judges every run.
5. Port the t3562 scenarios, then delete the holds, the sndbuf setter, the nudges,
   the poll wake, `test_writing`, and `test_stalled` if nothing needs it.
6. Keep real TCP only where the kernel is the subject: a Linux-only socket-stall
   test (Linux answers data that reaches a closed socket with a TCP RST, which
   dropped a client's unread GOAWAY in round 3), and the accept/admission tests.
7. Later: a seeded peer that picks behaviors from a seed, judged by the oracle.

Rejected: (a) replace the pumps with a simulated pump inside `Connection` — a second
implementation of the pump contract, and more change in the actor; (b) zio's sim
mode — not in the zio this repo uses.

## What to answer

Pick decisively: is the socket-pair seam the right next step, or is something else
better? Then say what the design understates. In particular:

- Does zio's `NetRecv` / `NetSend` (the TLS pump drives them on the raw fd: see
  `src/edge/tls.zig`, `Pump.start`, `rearmRecv`, `armSend`) and the h2c pumps'
  `std.Io.net.Stream` reader/writer (`src/edge/wire_pump.zig`, `ReadPump.run`,
  `WritePump`) work on an AF_UNIX stream fd the same as on TCP, on both kqueue
  (macOS) and io_uring/epoll (Linux)? Quote the zio source that answers it.
- Will a Unix socket pair really hold a stall where TCP loopback did not? What do
  macOS and Linux do with SO_SNDBUF/SO_RCVBUF on AF_UNIX stream sockets? Say what
  you know versus what step 1 must measure.
- What does the server do with the peer address or socket options of a TCP
  connection (TCP_NODELAY, getpeername, the `test_sndbuf_bytes` setsockopt) that
  would fail or differ on a Unix socket? Quote the call sites.
- Which stall tests, if any, cannot move to a socket pair and why.
- Is there a cheaper seam you would choose instead, given the premises?

Label every finding [LOGIC] (holds whatever the deployment facts) or [PREMISE]
(depends on a fact you name). Unlabelled findings are discarded. Quote at least one
line of code for each claim about the code, with its file and line, so the claim can
be checked. Keep the answer under 1,200 words.
