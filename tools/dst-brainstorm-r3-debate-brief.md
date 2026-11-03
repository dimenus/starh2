# Debate brief, round 3: deterministic simulation for starh2

Rounds 1 and 2: `tools/dst-brainstorm-brief.md`, `tools/dst-brainstorm-r2-brief.md`.
The premises and the labelling rule ([CODE] with file:line, [INFERENCE]
otherwise; unlabelled claims are discarded) are unchanged. Repo:
`/Users/rsaunderson/Source/mine/starh2-wt-3737`.

This round is a debate. Read the three round-2 answers in `tools/dst-r2/`
(`dst2-fable.md`, `dst2-astra.md`, `dst2-wonder.md`) in full. One is yours.
Earlier inputs stay in `tools/dst-r1/` (round-1 answers, the TigerBeetle survey,
the zio sim source at 913ffda).

## The open disputes

1. **First stage.** astra: an HTTP/1.1 TLS failure witness through the real
   pump, no port, 3-5 days. Wonder: an ack/teardown witness on one
   connection, close to astra's. Fable: port the sim first (4 days), then one
   h2c connection in the sim aimed at bug 1 (4 days). Which, and why?
2. **Port cost.** Fable 3-5 days, astra 6-10, Wonder 7-11, all from the same
   source. Whose reading of the hooks and the conflict sites is right? Name
   the specific files and functions that decide it.
3. **The bug-1 oracle, the (send error, peer saw the complete response)
   pair.** Wonder: forbidden in every scenario. Fable: forbidden unless a
   reset or connection error ended the stream. astra: sometimes allowed under
   resets, when a failure settles locally before success is published. Who is
   right, given `ticket_table.zig` and the TLS staging-ack rule?
4. **Is bug 1 a server bug at all?** astra notes the test joins the client,
   not the handler, before reading `ops_done`
   (`tests/t3567_tls_barriers.zig:777-834`), so `ops=0` may be a race in the
   test's own measurement. The orchestrator is measuring this now (recording
   `send`'s result and waiting for the record). Say how your plan changes if
   bug 1 turns out to be a test race and not a server bug.
5. **Bug 2.** Fable leans "race or stale thread-local" (moderate confidence);
   astra and Wonder say undetermined. Which evidence comes first: the faulting
   instruction and registers, a pointer-classifying panic hook, a TSan loop, or
   an in-process create/destroy churn harness?
6. **Smaller disputes.** Randomised select-arm choice (astra: it changes zio's
   declaration-order contract). Mutants as a comptime source flag (Wonder) or
   only as a patch script in a scratch tree (Fable). `Server.serveConnected`
   as a new production entry point (Fable) versus factoring the pump
   construction (astra).

## What to write

For each dispute: **concede** (say what convinced you), **contest** (cite
the code line or measurement that decides it, and quote it), or **name the
fact that would settle it** if neither side can cite one. Assertions of
preference without a citation are discarded. Correct any factual error you
find in another answer, with the citation.

Then end with **one joint plan you would sign**, written so the other two could
sign it too, and a list of at most three disputes you think remain open, each
with the options and your pick. Plain English, short sentences. Design only.
