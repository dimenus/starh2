# Panel round 2: review the starh2 hardening design v2

Worktree /Users/rsaunderson/Source/mine/starh2-wt-3737, branch carmack/t-3737,
commit 9c16242. Read `tools/hardening-design.md` (v2) in full. The evidence is
`tools/t3759-inventory.md` and `tools/t3759-research.md`; the code is in the
tree (the spike is 839dd69). Round-1 reviews: `tools/t3759-panel/design-astra.md`
(the Opus and Fable round-1 reviews were folded into v2; see the v2 commit
message, `git show -s 433f426`).

Premises: the six at the top of the design, plus Ryan's decisions Q2-Q6 in
its last section. Findings that argue against those are out of scope.

## What we want

1. **Q1, client identity. Ryan deferred it: he does not know what the right
   identifier is.** Decide it. Is `.peer` / IPv6 /64 + /56 aggregate /
   `.proxy_protocol` v2 / `.none` right? Consider: NAT and CGNAT (IPv4 and
   NAT64), IPv6 privacy addresses, dual-stack clients (one user on v4 and v6),
   HTTP/2 connection coalescing, Unix-socket endpoints behind a local proxy,
   PROXY v2 TLVs, a cloud L7 load balancer (ALB, Cloudflare) with and without
   PROXY. Say what the default should be and why, and what the failure mode
   of your pick is.
2. **Did v2 fix what round 1 found?** For each change in the v2 commit
   message, is it correct and sufficient? Quote code where it matters.
3. **New holes in v2.** In particular: the pressure gate (can an attacker
   stay just under the mark, or use the gate to push honest clients out?),
   the /56 aggregate, PROXY header waiting connections, the protocol-work
   buckets, the unbounded logged shutdown wait.
4. **Implementability.** The client table's insert/delete race across
   executors; the all-or-nothing charge across five accounts; anything the
   code cannot do as written.
5. One line: is v2 ready to build, yes or no, and if no, what blocks it.

Label every finding [LOGIC] or [PREMISE] (name the fact). Unlabelled findings
are discarded. Be short. Do not edit any file.
