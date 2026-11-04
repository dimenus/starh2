# Review brief: t-3759 step 4 (adab61c, 869f9cb, 3f8ad9f)

Review three commits on branch `carmack/t-3737` in the repo at
`/Users/rsaunderson/Source/mine/starh2-wt-3737`: `adab61c` (shares under
pressure), `869f9cb` (connection-start and handshake-start buckets in
rate-only records), `3f8ad9f` (PROXY waiter floors, `proxy_header_timeout_ns`).
Parent of the first: `42c65d8`. Read committed objects only
(`git show <sha>`, `git show <sha>:<path>`); the commits may be amended later
for a bench setting only, so review these hashes. Read each commit message in
full. The design is `tools/hardening-design.md` (v3): "The goal, stated as a
test", "The rule everything follows" and "Pressure", "The client table",
"Shares", "Admission and overload". The implementer's brief is
`tools/t3759-step4-brief.md` (commit b60d520), which also records the owner's
PROXY decision.

## Premises (settled; findings that depend on their negation are out of scope)

- No external users; breaking changes are free.
- Shares apply only under pressure (the resource's server use at or above
  1/4 of the limit). Client share 1/8, cap 1/4. One NAT address over its share
  under pressure is refused: accepted by the owner.
- Check-then-reserve may overshoot a mark by the number in flight.
- The PROXY waiter cap parks (never closes); one ceiling of 64; a reserved
  floor of 64/(2N) per PROXY endpoint plus a shared half; a separate header
  deadline started at accept.
- Route shares, the DATA bucket and pressure-scaled timeouts are later steps.

## What to look for

Correctness defects only:
- **Refusal and undo**: every charge that can now be refused undoes the
  earlier list entries exactly once; no path admits uncounted; no refusal
  leaves a slot, a waiter, a handshake permit or a lease held.
- **Pressure**: read from the server account of the same resource; can a
  refusal fire with the server below the mark, or never fire above it?
- **Buckets and rate-only records**: refill arithmetic and overflow; a serial
  reconnect regaining its burst; eviction of a rate-only record that still has
  a holder; the reserve running out.
- **The PROXY floors**: can one endpoint take another's floor; is the shared
  half accounted exactly; does the header deadline start at accept and cover
  only the header; does the TLS-slot wait keep the preface deadline.
- **Tests that would pass with the defence removed.**

Label every finding **[LOGIC]** or **[PREMISE]** (name the deployment fact).
Unlabelled findings are discarded. For each: file:line in the commit, a
concrete failure scenario, and the quoted code. Say "no finding" for each area
you checked and found sound.
