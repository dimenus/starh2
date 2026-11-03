# Debate brief: the PROXY header waiter cap, round 2

Round 1 is `tools/proxy-cap-panel-brief.md` (commit 93f9d2d). The three answers
are in `/Users/rsaunderson/Source/mine/starh2-wt-3737/tools/pcap-r1/`
(`pcap-astra.md`, `pcap-wonder.md`, `pcap-fable.md`). One is yours. Read the
other two in full. The code is `/Users/rsaunderson/Source/mine/starh2-wt-3737`
at commit `8eb220f` (`git show 8eb220f:<path>`).

All three chose PARK. That is settled. Two points are open:

1. **The cap's scope.** astra and Wonder: keep it server-wide (one counter);
   per-endpoint caps would multiply the real ceiling (8 endpoints x 64 = 512).
   Fable: per endpoint, with a boot check that the sum over PROXY endpoints is
   at most `max_connections / 4`, because today a stall or a handshake wait on
   one PROXY endpoint parks every other PROXY endpoint, even one fed by a
   different proxy. Does Fable's sum check answer the ceiling objection? Is
   cross-endpoint parking a real harm, given the design's "occupancy, not
   fairness"? Is there a third shape (a global ceiling plus per-endpoint
   reservations)?
2. **A separate PROXY header timeout** (Fable: about 1 s, at most the preface
   deadline), which would turn a stalled proxy's 64 slots over five times
   faster. It is safe only if every supported proxy sends the header at
   connect, before any client byte. HAProxy, nginx `stream` and Envoy: Fable
   says yes. AWS NLB: unverified. Adopt now, adopt behind a config default of
   "same as the preface deadline", or defer until NLB is measured?

For each point: **concede** (say what convinced you), **contest** (cite code
file:line in 8eb220f, or a named and labelled premise), or **name the fact that
would settle it**. Preference without a citation is discarded. Then write the
joint position you would sign, so the other two could sign it too, and at most
one open dispute with your pick. Label claims [CODE file:line], [INFERENCE] or
[PREMISE]. Plain short sentences. Design only.
