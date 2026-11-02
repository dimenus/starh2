# Panel: review the starh2 hardening design

You are one of three independent reviewers (different vendors). Your cwd is the
worktree /Users/rsaunderson/Source/mine/starh2-wt-3737, branch carmack/t-3737,
commit 67d0109. Read, in this order:

1. `tools/hardening-design.md` (the design under review)
2. `tools/t3759-inventory.md` (resource inventory, file:line evidence)
3. `tools/t3759-research.md` (what other servers do, with sources); read its
   synthesis first, then the sections you need
4. The code, wherever you need to check a claim. The spike is commit 839dd69.

## Premises — settled. A finding that depends on their negation is out of scope.

The six premises at the top of the design. In particular: tasks cannot be
killed; no external users; a process-wide panic is never the answer.

## What we want

1. **Holes.** For each inventory row (section 4), does the design close it?
   Show the attack that still works after the design, if any, with arithmetic
   from the defaults. Find attacks the inventory missed.
2. **Contradictions.** Two parts of the design that cannot both hold; a rule
   the code cannot implement as written (quote the line); a limit that a
   normal client hits (a browser, a Datastar page with many SSE streams, a
   load balancer health check, HTTP/2 connection coalescing).
3. **Over-engineering.** Anything here that costs more than it buys, or that
   a simpler mechanism covers. Say what to cut.
4. **The four decisions (Q1-Q4)**: your pick for each, one line of reason.
5. **The single biggest risk** in building this, and how the tests should
   catch it.

Label every finding **[LOGIC]** (holds whatever the deployment) or
**[PREMISE]** (depends on a deployment fact; name it). Unlabelled findings are
discarded. Be short and concrete. Do not edit any file.
