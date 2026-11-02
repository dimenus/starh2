The three answer files are permission-blocked, so I’ll use your condensed positions. The worktree is at `50bd5b1`; I’ll check the code at the requested `839dd69`.


*The answer files were permission-blocked; this responds to your summaries, with code checked at `839dd69`.*

1. **[LOGIC] Cap — partly changed by A/B.** I now support a **soft per-route admission breaker**, not a hard orphan cap. My previous objection proves that a threshold cannot guarantee containment, not that it is useless: it can stop successive waves. Already-admitted handlers can still exhaust global capacity; guaranteed isolation requires reservations before spawn.

2. **[LOGIC] Count versus bytes — A persuades me: both.** Track handler count and current retained arena bytes, not merely a teardown snapshot. B’s count-only approach bounds arena memory only through the per-request worst case. Orphans can continue allocating: a closed gate bypasses connection charging (`request_pool.zig:705`), while per-request/server limits remain (`775–785`). Neither post-hoc threshold constrains already-admitted growth.

3. **[LOGIC] Refusal — adopt B’s 503.** `REFUSED_STREAM` is valid before execution, but explicitly permits safe retry; that is an undesirable signal for persistent route failure. **[PREMISE: client retry policy]** 503 avoids that transport-level retry signal, but does not guarantee clients stop retrying.

4. **[LOGIC] Shutdown — unchanged.** The library waits until reachable state is safe to destroy. A’s exit belongs to the host, not the library; B’s error return is not an exit and can unwind through `defer deinit`. A bounded-return design needs explicit lifetime transfer, not merely another error. **[PREMISE: an external supervisor exists]** The supervisor bounds shutdown; without one, shutdown may remain stuck indefinitely.

**[LOGIC] Code corrections:** I cannot attribute additional claims to the inaccessible originals, but these interpretations are contradicted:
- An unconditional deinit safety guard: `server.zig:752` is only `std.debug.assert(self.accounting.orphaned_handlers.load(.acquire) == 0);`.
- Cancellation escaping the orphan wait: `server.zig:413` is `zio.sleep(.fromMilliseconds(1)) catch {};`.
- The brief’s “only through application code that ignores cancellation” is too strong for **transient** orphans: teardown calls `self.requestHandlerCancel(i);` (`connection.zig:5590`), then `self.orphanSlot(i);` (`5612`) without awaiting handler completion; a cooperative handler may simply not have resumed yet.

**[LOGIC] Final pick:** Keep global reservations, add soft per-route count/byte admission breakers and route-tagged reporting, refuse with 503, and retain lifetime-safe unbounded library shutdown, with any process deadline owned by the host.

