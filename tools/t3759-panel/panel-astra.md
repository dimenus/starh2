**[LOGIC] Pick option 1, with age-aware reporting: retain each handler’s admission token and memory charges until release, add no separate orphan cap, and let `serve` return only when teardown is safe—not kill its host.**

**[LOGIC] Reporting:** expose current count, retained bytes, oldest age, and admission refusals; use rate-limited diagnostics keyed by registered route. Warn about persistent orphans, not every orphan transition: orphaned does not necessarily mean cancellation-defying.

**What the options miss**

- **[LOGIC] 1:** Accounting bounds resource consumption, not availability. One token is not a fixed memory cost, and logging cannot recover capacity.
- **[LOGIC] 2:** A smaller orphan threshold is an admission circuit breaker, not a hard cap: already-admitted handlers can all become orphans afterward. A hard bound requires reserving potential orphan capacity **before spawning**. Global refusal also blocks healthy routes; route refusal cannot reclaim existing orphans or their shared-pool resources.
- **[PREMISE] 3:** Peer isolation helps only with a meaningful, stable identity. With NAT, proxies, or rotating addresses, address-based quotas punish unrelated clients or permit evasion. They do not contain the faulty handler itself.
- **[LOGIC] 4:** Unlimited waiting preserves memory safety but promises no shutdown deadline. Returning an error and allowing ordinary `deinit` is not a safe timeout policy.
- **[PREMISE] 4, deployment policy:** For a dedicated process whose owner requires bounded shutdown, use an external supervisor’s deadline followed by process termination without teardown. That authority belongs to the application, not this library; an in-runtime timer cannot guarantee progress against non-yielding tasks.

**Concrete failure of my pick**

**[PREMISE]** Suppose an exposed `/stuck` handler indefinitely shields a yielding wait, and each request fits one minimum arena block. A client starts 256 handlers, disconnects, and repeats sixteen times; those connections need not coexist. The resulting 4096 orphans consume every token **and 256 MiB of request-pool capacity**: `request_pool.zig:83` says `min_block_bytes: usize = 64 * 1024`, and `limits.zig:196` sets the pool to 256 MiB. New requests—including complete handlers needing request memory—cannot proceed; shutdown never completes without external termination. Larger arenas exhaust memory sooner.

**Corrections to the facts**

- **[LOGIC] “Only application code that ignores cancellation” is false for the counter.** `connection.zig:5589-5590` executes `link.gate.close();` then `self.requestHandlerCancel(i);`; line 5612 calls `self.orphanSlot(i);` without waiting for application completion. A cooperative sleeper can therefore be orphaned briefly before processing cancellation. The reported “0 ms” measures connection disappearance: `tests/goaway_delivery.zig:1750` computes `const gone_ms = (nowNs() - t_gone) / std.time.ns_per_ms;`, not handler cancellation latency.
- **[PREMISE] With streaming compression active, the resource inventory is incomplete.** An orphan can retain a pooled encoder: `connection.zig:8629` assigns `hctx.encoder = enc;`; handler completion releases it at lines 7946-7947: `if (link.hctx.encoder) |enc| { enc.destroy();`. Token and arena metrics alone miss that retention.

