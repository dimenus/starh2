[LOGIC] **Review blocked, not completed.** Permission was denied for both mandatory prerequisites:

- `/Users/rsaunderson/.claude/skills/zig/SKILL.md`
- `qmdsync find 't-3759 hardening shutdown account ownership' --repo starh2`

Please supply those contents/results or authorize access. I cannot provide the requested code-verified conclusions or a clean security verdict without completing the review. No files were modified.The reviewed documents match the requested revision; HEAD adds only the panel brief. Two prerequisite resources are inaccessible, so the review will distinguish supported findings from any resulting gaps.


## Security-relevant design gap

### [PREMISE: a handler remains runnable after shutdown returns] Retaining allocations does not retain their owners
**File:** `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/hardening-design.md:198–204`  
**Category:** MemorySafety  
**Severity: HIGH | Confidence: 9/10**

**Problem:** The proposed bounded return needs an explicit lifetime contract beyond “`deinit` frees nothing.” Pool/account objects themselves are embedded in `Server`; leaving their allocations mapped does not preserve a stack-allocated `Server`, caller-owned balancer, handler context, allocator, or runtime when the caller unwinds.

**Evidence:**
- `/Users/rsaunderson/Source/mine/starh2-wt-3737/src/edge/server.zig:132–157`: accounting and request/compression pools are embedded members.
- `/Users/rsaunderson/Source/mine/starh2-wt-3737/src/edge/connection.zig:1646–1676,7943–7971`: orphan completion releases arena memory and updates accounting/balancer **outside the connection gate**. Closing that gate is therefore not a guard for these owners.
- `/Users/rsaunderson/Source/mine/starh2-wt-3737/examples/datastar_sse.zig:15–36`: the existing caller keeps `Server` on its stack and subsequently deinitializes the runtime.
- `/Users/rsaunderson/Source/mine/starh2-wt-3737/zig-pkg/zio-0.19.0-xHbVVPYzLgBGEz8P__YtqSzJ0whhX8mw9hP3Zg2Gdg6S/src/runtime.zig:1870–1924`: runtime destruction requires all task stacks returned; server-side checks cannot change that requirement.

**Suggested fix:** Define a stable owner for the entire reachable state graph and explicitly specify what callers must retain after `HandlersStuck`, including application handler state and the runtime. Returning an error must not imply ordinary cleanup is safe.

**Status:** Design gap, **not a demonstrated current UAF**. The existing orphan wait at `/Users/rsaunderson/Source/mine/starh2-wt-3737/src/edge/server.zig:399–415` currently guards this lifetime boundary.

## Bounded coverage notes—not additional vulnerability findings

- **[LOGIC] Deadline semantics need clarification.** `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/hardening-design.md:149–155` proposes sent-bytes/window progress while allowing hours-long SSE handlers. Its cited precedent instead times **outstanding writes**, using `max(grace, write_bytes/rate)`—`/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/t3759-research.md:739–751`. Specify that application-idle periods with nothing pending do not fail the response rate. Otherwise healthy sparse SSE is indistinguishable from a slow reader.

- **[LOGIC] Add explicit protocol test shapes, without claiming throughput protection from occupancy caps.** Empty DATA, padding-only DATA, and client-induced **server** resets deserve separate coverage. DATA/HEADERS bypass frame buckets at `/Users/rsaunderson/Source/mine/starh2-wt-3737/src/core/rates.zig:112–143`; DATA flow control charges payload length, while application-body accounting excludes padding at `/Users/rsaunderson/Source/mine/starh2-wt-3737/src/core/session.zig:1579–1601,1641–1678`. Thus a body minimum must count useful body bytes, not frame count or padding. These observations do not establish measured executor starvation.

- **[PREMISE: honest users share a peer identity] Normal-client isolation is only identity isolation.** Under `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/hardening-design.md:97–125`, NAT/shared `/64` users collectively receive 128 connections, eight concurrent handshakes and 512 streams. Nine simultaneous legitimate TLS handshakes already exceed the stated share. This is a deployment consequence, not a challenge to the settled premises or an identity-policy recommendation.

## Arithmetic cross-check for consolidation

**[LOGIC; conditional on the proposed accounting actually being implemented]** Defaults from `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/hardening-design.md:117–139`, applied to inventory rows at `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/t3759-inventory.md:239–254`:

| Row | Proposed bound / qualification |
|---|---|
| 1 | 512 × 64 KiB = **32 MiB**; body clock must start before first DATA. |
| 2 | **8/128** handshakes, renewed every 5 s; this is **1/16**, not 1/8. |
| 3 | **128/1,024** accepted connections per identity. |
| 4 | Same socket share; boot FD margin is not a bound on descriptors opened by handlers. |
| 5 | Detection only; no executor-containment claim follows. |
| 6 | Client: **512 handlers AND 32 MiB**; route: **1,024 AND 64 MiB**, including orphans. |
| 7 | Reaper bottleneck disappears only with the specified H1 replacement; handler charges remain. |
| 8 | At most **56** steady-state 576-KiB requests under 32 MiB; transient growth can lower this. |
| 9 | 32 MiB equals **512 × 64 KiB**, or eight 4-MiB connection budgets; rate semantics still need definition. |
| 10 | **16/128** contexts per client. No route context quota is specified; identity fallback remains the documented outcome. |

No compiler/tests were run. Git inspection was permission-denied; commit identity relies on the supplied scope. No clean implementation verdict follows from this design assessment.**[LOGIC] Not a high-confidence, material finding.** `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/hardening-design.md:89–93,208–211` does not specify where logging-throttle state lives.

- **If stored in the client entry:** deletion/recreation would reset it. Repeatedly hitting a logging trigger—not reconnecting alone—could bypass once/minute.
- **If retained separately:** its capacity and expiration rules are unspecified.

This warrants a brief design clarification: define throttle-state lifetime and its storage bound. The documents alone establish neither an exploitable logging bypass nor unbounded retention.**The quotas substantially improve containment, but the blanket isolation claim is not yet supportable.** The main blocker is the shutdown lifetime contract.

| # | Severity | File | Lines | Vulnerability | Confidence |
|---|----------|------|-------|---------------|------------|
| 1 | 🟠 HIGH | tools/hardening-design.md | 198–204 | **[PREMISE: caller performs ordinary cleanup after `HandlersStuck`]** Retaining allocations does not retain their owners: surviving handlers can still reach a stack-owned `Server`, caller state and runtime. Proposed lifetime gap, not a demonstrated current UAF. | 9/10 |

**Inventory coverage**

“Contained” means the proposed defaults prevent that one-client exhaustion, assuming accounting follows every allocation and release.

| Row | Verdict |
|---|---|
| 1 — silent request bodies | **[LOGIC] Contained:** 512 × 64 KiB = **32 MiB**, not all 256 MiB. Starting the body clock at HEADERS closes indefinite silence. |
| 2 — silent TLS connections | **[LOGIC] Contained:** eight handshakes, renewed at **8/5 = 1.6 connections/s**, cannot occupy all 128. Note: eight is **1/16**, not the advertised 1/8. |
| 3 — connection slots | **[LOGIC] Contained:** 128/1,024 connections per identity. |
| 4 — descriptors | **[LOGIC] Permanent accept-loop exit is addressed. [PREMISE: a broken handler opens descriptors]** Exhaustion remains: one handler can consume the FD headroom while using just **1/1,024** route task permits. A boot margin is not a handler FD budget. |
| 5 — executors | **[LOGIC] Not contained:** N non-yielding requests pin N executors; for N ≤ 512 this fits one client’s task allowance. Detection is compatible with the premises, but contradicts the goal/test requiring every other client to remain served. Explicitly exempt this row from that guarantee. |
| 6 — orphan tasks/memory | **[LOGIC] Contained:** client **512 tasks AND 32 MiB**; route **1,024 AND 64 MiB**, retained until actual release. |
| 7 — H1 reapers | **[LOGIC] Closed by removal:** the H1 link/pool conversion eliminates the 64-worker choke point. It is essential, not optional scope. |
| 8 — slow bodies | **[LOGIC] Contained:** at 576 KiB/request, 32 MiB admits at most **56**, versus roughly 452 filling today’s server pool. Useful-body-byte rate accounting closes the drip. |
| 9 — outbound budget | **[LOGIC] Contained per client:** 32 MiB = **512 × 64 KiB**, or eight full 4-MiB connection budgets. Response timer semantics still need clarification below. |
| 10 — compression | **[PREMISE: compression enabled] Partial:** one client gets only 16 contexts, but one broken route can retain **8 clients × 16 = all 128**. That is only 128 handlers and initially about 8 MiB of request blocks, below both route limits. Add a route context quota; otherwise other routes still lose compression. |

**Contradictions and missed cases**

- **[LOGIC] Response rate must measure outstanding-write time, not response lifetime.** Design lines 149–155 permit long-lived SSE but prescribe sent bytes/window. A healthy 100-byte event each minute averages **1.67 B/s**, below the cited 240 B/s. Explicitly exclude application-idle periods. The cited Kestrel mechanism times outstanding writes, not silence between events (`t3759-research.md:739–751`).

- **[LOGIC] Protocol work remains outside the shares.** Zero-length DATA consumes zero flow-control bytes; DATA and HEADERS bypass frame buckets (`src/core/rates.zig:112–143`). A client can repeatedly spend parsing work without accumulating meaningful charged bytes. This establishes an unbounded work-rate dimension—not measured CPU starvation. Add empty/padding-only DATA tests and protocol-work protection; application request-rate limiting cannot cover requests never dispatched. Padding must not satisfy the body minimum.

- **[PREMISE: NAT, shared /64, or proxy aggregation] “A normal browser … never reaches” is untenable.** Nine simultaneous legitimate TLS handshakes sharing an identity exceed eight. Raising the proxy’s limits to server-wide limits restores the original exhaustion opportunities through that proxy. Document this as disabling end-user isolation, not preserving it. A multiplexing L7 proxy also cannot supply different end-user identities through one connection-level identity.

**What to cut**

- **[LOGIC] Cut the proposed 512-handler connection-debt threshold.** The spike allocates only 256 handler slots (`connection.zig:2405`), and retains them until completion (`4998–5027`). The design’s “over” 2×256 trigger cannot fire without enlarging that existing bound. Keep the stronger bound.
- **[LOGIC] Defer ranked “largest holder” refusal.** Refusing future work cannot reclaim an orphan’s memory when its client sends nothing further. Hard shares and idle-connection reclamation provide the concrete containment; ranking does not supply missing capacity.

**Q1–Q4**

| Decision | Pick |
|---|---|
| Q1 | **[PREMISE: direct or identity-preserving proxy deployment] Yes**, with explicit disclosure that aggregated identities lose end-user isolation. |
| Q2 | **[LOGIC] Yes to configurable 1/8 client and 1/4 route caps**, including route compression; reconcile eight versus sixteen handshakes and remove the “never reached” promise. |
| Q3 | **[LOGIC] Yes, in this change:** otherwise the H1 cancellation and arena holes remain. |
| Q4 | **[LOGIC] Yes, detection only:** the watchdog must run independently of stalled executors, and the availability guarantee must exclude them. |

**Biggest implementation risk**

**[LOGIC] Ownership crossing connection teardown and server shutdown.** Orphan completion accesses accounting, arenas and the balancer outside the gate (`connection.zig:1646–1676,7943–7971`); pool/account objects are embedded in `Server`. Skipping frees cannot preserve a caller’s stack or make runtime destruction safe.

Specify the retained owner and caller obligations before implementation. Test a yielding, cancel-ignoring handler across RST, disconnect, slot reuse and bounded shutdown; then let it allocate and finish. Assert no premature account reuse, exactly-once release, restored counters, and no access to destroyed owner state. Inject partial-admission failures into those same transitions.

No files changed. The prior-decision search and external Zig skill were inaccessible; this review uses the supplied documents and inspected source.

Follow-up choices:
1. Fix highest severity issues.
2. Fix all issues.
3. Commit a summary of findings.

Reply with a choice.

