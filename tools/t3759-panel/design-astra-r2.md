The pressure-only policy can stay, but its guarantees need tightening: admission gating does not reclaim existing holdings, and deleting idle identities would erase handshake-rate history. I’m checking the ownership and shutdown paths for implementation blockers.


**[LOGIC] Ready to build: no — account lifetime, admission atomicity, rate-state retention, and overload timing remain unresolved.**

### Q1: identity decision

**[PREMISE] A network address identifies an accounting group, not a person.** Default TCP endpoints to `.peer`: canonical IPv4 address, IPv6 `/64`, no port. Keep IPv4-mapped normalization. Make `/56` aggregation **opt-in**, not universal: delegation boundaries cannot be inferred from addresses. `/64` contains privacy-address rotation; `/56` can contain subnet rotation within that allocation, but a `/48` contains **256 distinct `/56`s**.

The failure mode is explicit: NAT/CGNAT users share a budget; NAT64 clients can appear behind shared translator IPv4 addresses; translator pools, dual-stack access and larger IPv6 delegations give one user multiple budgets. Do not invent a v4/v6 correlation.

**[PREMISE] PROXY v2 describes one transport connection.** Enable `.proxy_protocol` only for explicitly trusted senders or permission-protected Unix sockets, and only where an upstream connection represents one original client. Ignore TLVs for identity: `UNIQUE_ID` is not a user identifier; CRC is not authentication. Give `LOCAL` a bounded infrastructure account, including a listener-scoped account on Unix sockets.

**[PREMISE] L7 connection pooling combines users.** Use `.none` behind ALB/Cloudflare-style multiplexed origin connections, **with or without PROXY**, unless that one-client-per-connection property is established. Enforce end-user fairness upstream. Direct HTTP/2 coalescing merely combines origins within the same network account; never split shares by `:authority`. Unix sockets without trustworthy original-client metadata should default to `.none`.

### New holes and implementability

- **[LOGIC] Pressure gating limits admissions, not existing holdings.** With connection share 128 and mark 256, one client can retain 256 connections after pressure begins. An honest NAT holding 200 connections can have its next admission refused when an attacker adds just 56—while 768 server slots remain free. That is a consequence of agreed Q2, not grounds to reverse it; narrow “every other client keeps working” accordingly. Staying below the mark also leaves work-rate attacks unaffected.

- **[LOGIC] “One atomic load” cannot establish the claimed bound.** Concurrent admissions can all observe below-mark usage; byte allocations can cross the mark in one charge. Specify a linearization point coupling the pressure decision to the reservation, including charge size. Fixed-order CAS plus rollback gives provisional reservations, not an atomic five-account snapshot; specify whether conservative refusals are acceptable. The listed order also omits the connection account.

- **[LOGIC] The table needs stable objects and retained debt.** Backward-shift deletion must move **indices**, not account objects addressed by outstanding charges. Serialize lookup-plus-reference acquisition against last-reference deletion/reuse, with stable pool slots and generations. Executor sharding by `/64` alone does not serialize its shared `/56` account. Most importantly, deleting an identity when its last holding ends resets its handshake bucket: serial reconnects repeatedly regain burst 32. Retain depleted rate state until refill; revise capacity, saturation policy and the “empty at quiescence” test accordingly.

- **[LOGIC] `/56` pressure arithmetic is ambiguous.** Its share is four base shares, or half the server. Applying “mark = twice the share” to that account activates it only at full server capacity. If retained, specify one resource-level pressure mark, rather than recomputing it from each account’s share.

- **[LOGIC] A PROXY waiting cap bounds occupancy, not fairness.** Check sender trust before waiting; bound header bytes, time, FDs and task state; do not consume a TLS-handshake permit yet. A stalled trusted sender can monopolize the pending-header allowance repeatedly. Isolation before original identity arrives requires upstream guarantees or reserved capacity, not merely a deadline.

- **[LOGIC] The protocol buckets leave cheap work uncovered.** Reconnects replenish per-connection bursts, particularly on h2c; one-byte DATA bypasses the proposed zero-body bucket. Closed-stream DATA is especially relevant: `session.zig:1613–1615` replenishes connection credit and returns without retaining a body. Also correct the draft’s padding claim: `session.zig:1579–1582` explicitly charges the **whole payload**, padding included. Bound aggregate protocol work across reconnects and test tiny nonempty frames—not only empty frames.

### Did each v2 change fix round 1?

| Change in `433f426` | Assessment |
|---|---|
| Separate protocol streams/task handlers | **[LOGIC] Yes.** `session.zig:1123` releases only when `s.state == .closed`; `connection.zig:1667` releases the task token at final link release. Preserve that distinction. |
| Pressure-only shares; endpoint identity; `/56`; post-PROXY charging | **[LOGIC] Partial.** Correct separation, but the pressure, identity and pending-header qualifications above are necessary. |
| Handshake-rate and protocol-work buckets | **[LOGIC] Partial.** Rate history cannot disappear with the connection/account; empty-only DATA protection is insufficient. |
| Response/body minimum rates | **[LOGIC] Correct direction, not fully specified.** Pause body timing only when advertised receive credit prevents progress. Response timing must follow bytes through scheduler, wire queue and completion: today `checkSlowConsumers` skips `pw.len == 0` (`connection.zig:3530`), even if output remains downstream. |
| Replace dead 2× handler cap with GOAWAY | **[LOGIC] Correct, but trigger when the queue becomes full.** Merely replacing the existing overflow branch can miss it: `session.zig:2043` refuses the next stream before dispatch when all 256 protocol slots are occupied. |
| Pressure-scaled idle timers; evict top holder’s oldest connection | **[LOGIC] Incomplete.** Reading an atomic does not wake an already-parked actor; `nextDeadlineNs` currently arms the old idle deadline (`connection.zig:3784–3793`). Require bounded resampling/rearming. GOAWAY also needs a close deadline and an eviction-in-progress marker; sending it alone releases nothing. H1/pre-handshake sockets need close equivalents. |
| Heap-core lifetime solution | **[PREMISE] Q5 supersedes this commit-message item.** Never returning while handlers remain fixes the ordinary-cleanup lifetime hole. Logging must cover waits **before** orphan polling: currently `connection_group.await` precedes `waitOrphanedHandlers` (`server.zig:392–399`), and H1 can remain there indefinitely. |
| Independent executor watchdog; executor-time exemption | **[LOGIC] Correct architecture.** The fork must distinguish parked executors from stuck tasks and publish a lifetime-safe task identity. Today the monitor starts only with enabled metrics and a positive interval (`zio/runtime.zig:1774–1782`); reuse alone does not enable detection. |
| Route defaults; hierarchy validation | **[LOGIC] Defaults follow Q2.** Charge existing arena bytes at dispatch **and subsequent growth**, including orphan growth: `request_pool.zig:710` currently permits connection charging to disappear when the gate closes. Keep client/route charges independent of that gate. |
| Conservation tests, soak, mutation | **[LOGIC] Necessary, insufficient alone.** Zero final counters miss transient overshoot, account misattribution and forgotten rate debt. Add concurrent peak-bound, same-key deletion/reinsertion and reconnect-history assertions. Runtime underflow checks must prevent subtraction corruption; logging before continuing to free reachable memory is not safe. |
| Bisectable series pushed together | **[PREMISE] Q6: sufficient.** The shutdown-reporting commit must handle existing H1 waits until the later link conversion lands. |

