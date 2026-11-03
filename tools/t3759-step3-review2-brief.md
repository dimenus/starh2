# Review brief, round 2: t-3759 step 3 (934df12, 91f9662)

Review two commits on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`: `934df12` (step 3, the
accounts, parent `d33e615`) and `91f9662` (identity and PROXY v2 framing as
endpoint fields, parent `934df12`). Round 1 reviewed `8eb220f`. Read
`git diff 8eb220f 934df12` and `git show 91f9662` first, then both commit
messages in full (each has a review-round section). Committed objects only.
The round-1 brief, `tools/t3759-step3-review-brief.md`, holds the premises;
they are unchanged, plus these owner decisions: table capacity doubled;
identity is a field of each endpoint, with PROXY framing separate from the
identity mode; the PROXY waiter cap stays server-wide in this step;
`PeerIdentityBehindProxyProtocol` and `ProxyTrustedRangeHoldsIpv4` are
intended boot errors.

## What to look for

Above all what round 1 changed:
- **`Record.word`**: the generation and the holder count are one atomic word;
  every holder change is a CAS that compares the generation; the generation
  moves when a record leaves the index. Is every path (acquire, retain,
  release, the locked drop, quarantine, reuse) correct under concurrency? Can
  the counter or the generation wrap? Can a stale holder still change a live
  record?
- **CIDR normalisation** (v4-mapped ranges, the new boot error, the server's
  normalised copy): can a trusted check be bypassed, or a legal config refused?
- **The endpoint field**: every endpoint literal kept its meaning; the boot
  errors fire exactly when they should; `.none` with PROXY framing strips the
  header and charges nothing.
- **The free on the boot error path** that round 2 fixed in `934df12`: any
  other partly-built union or slice freed on an error path in `Server.init`.

Correctness defects only. Label every finding **[LOGIC]** or **[PREMISE]**
(name the deployment fact). Unlabelled findings are discarded. For each:
file:line in the commit, a concrete failure scenario, and the quoted code. Say
"no finding" for each area you checked and found sound.
