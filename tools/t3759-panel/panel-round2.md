# Panel round 2: read the other answers, then update or defend

Round 1 brief: /Users/rsaunderson/Source/mine/starh2-wt-3737/tools/t3737-stuck-handler-panel.md
Code: worktree /Users/rsaunderson/Source/mine/starh2-wt-3737, commit 839dd69.

The three round-1 answers (condensed by the orchestrator, code citations kept):
- Panelist A: /private/tmp/claude-501/-Users-rsaunderson-Source-mine-starh2/d578bf39-a8f8-4ef1-aa4e-8a5e600cf635/scratchpad/t3737/panel-opus.md
- Panelist B: /private/tmp/claude-501/-Users-rsaunderson-Source-mine-starh2/d578bf39-a8f8-4ef1-aa4e-8a5e600cf635/scratchpad/t3737/panel-fable.md
- Panelist C: /private/tmp/claude-501/-Users-rsaunderson-Source-mine-starh2/d578bf39-a8f8-4ef1-aa4e-8a5e600cf635/scratchpad/t3737/panel-astra.md

The orchestrator checked in the code: the deinit guard is `std.debug.assert` only (server.zig:752), and `waitOrphanedHandlers` swallows cancel with `catch {}` (server.zig:413).

The open disagreements:
1. Per-route orphan cap (A, B) or no separate cap, counters only (C: an admission cap cannot bound handlers already admitted; a hard bound needs a reservation before spawn).
2. If a per-route cap: count handlers only (B) or handlers and arena bytes (A).
3. Refusal: REFUSED_STREAM (A) or 503 (B: clients may retry REFUSED_STREAM automatically).
4. Shutdown: wait up to a limit then exit without deinit (A); wait up to a limit, log, `serve` returns an error and the application exits (B); wait with no limit, the supervisor bounds shutdown, the library must not kill its host (C).

What we want back, short:
- For each disagreement: your position now, and whether another panelist changed your mind. If you hold your position, give the strongest reason the others are wrong.
- Any claim in another answer that the code contradicts. Quote the line.
- Your final pick in one sentence.
Keep the [LOGIC]/[PREMISE] labels. Do not edit any file.
