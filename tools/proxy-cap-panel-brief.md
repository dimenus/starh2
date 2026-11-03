# Panel brief: over the PROXY header waiter cap, close or park the accept loop?

You are one of three independent reviewers (different vendors). Design
question for starh2, an HTTP/2 and HTTP/1.1 server in Zig on the zio runtime.
Code to read: `/Users/rsaunderson/Source/mine/starh2-wt-3737` at commit
`8eb220f` (`git show 8eb220f:<path>`): `src/edge/server.zig` (the accept loop,
`awaitAdmission`, `admitAccepted`, `takeProxyHeader`, the waiter cap
`Limits.proxy_header_waiters`), `src/edge/proxy_protocol.zig`, and the design
`tools/hardening-design.md` (sections "Client identity (Q1)" and "Admission
and overload"). The commit message of `8eb220f` is the decision record.

## The question

On an endpoint with PROXY protocol v2, a connection from a trusted proxy is
accepted, then waits for its PROXY header (bounded by the preface deadline)
before the client's identity is known. A small cap bounds how many connections
may wait for a header at once. When that cap is full, today's code PARKS the
accept loop: new connections stay in the kernel's listen queue until a waiter
finishes. Alternatives: CLOSE the new connection at once (accept and close,
counted and logged), or something else you can justify.

## Premises (settled)

- No external users; breaking changes are free.
- The design's admission rule: "Over a share under pressure: refuse that
  identity. Never park the accept loop for everyone because of one identity."
- The design on this cap: "Connections waiting for the header: the
  trusted-source check comes first; then a small cap of their own (not the TLS
  handshake slots ...) and the preface deadline. The cap bounds occupancy, not
  fairness: a stalled trusted proxy can fill it, and the log names the proxy."
- Proxies that send PROXY v2 keep one upstream connection per client (NLB,
  HAProxy or nginx `stream`, Envoy TCP), or pool (then identity is `.none`).
- Connection-level limits (max connections, TLS handshakes) also park the
  accept loop today; step 4 adds per-identity refusal by share.

## Answer

1. Close, park, or another option. Decide; do not survey.
2. What happens in each option when (a) one trusted proxy stalls (sends no
   header) and fills the cap, (b) a burst of legitimate connections arrives
   faster than headers complete, (c) the server has several endpoints, some
   without PROXY. Cite the code paths that decide it (file:line in 8eb220f).
3. What the client of the proxy sees in each case (a reset, a timeout, a
   queue wait), and what the proxy itself does with a closed upstream
   connection (if you rely on proxy behaviour, label it [PREMISE] and name it).
4. Is the cap per endpoint, per trusted proxy address, or server-wide, and
   should that change with your answer?
5. What would change your mind.

Label every claim about the code [CODE] with file:line, everything else
[INFERENCE] or [PREMISE]. Plain short sentences.
