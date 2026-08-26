# Review brief — one commit of the zio async runtime

You are reviewing ONE commit of `zio`, a Zig async runtime library (event loop,
completion queue, coroutines, io_uring / kqueue / epoll backends).

## What you can see, and it is all you may use

Your working directory holds exactly three things:

- `tree/` — the complete source tree AS OF this commit. Read any file in it.
- `diff.patch` — this commit against its first parent. This is the change under review.
- `subject.txt` — the commit subject and message.

There is no git history here on purpose. **Do not use the network. Do not search
the web. Do not look for this project, its repository, its issue tracker, or any
pull request.** A reviewer that reads the answer somewhere else measures nothing.
If you used the network anyway, say so in the `network_used` field. An honest
`true` costs nothing; a hidden one voids the whole run.

## Premises — treat these as settled

A finding that depends on the negation of one of these is out of scope.

- The language is Zig 0.16. Do not report on Zig version churn or stdlib API drift.
- The project supports Linux, macOS and Windows targets. Tests run with
  `zig build test`, examples build with `zig build examples`.
- The commit is one push inside an open review. Earlier pushes on the same branch
  are already part of `tree/`. You are reviewing THIS push.
- The library has external consumers, so a public API change is a real cost.
- Style, formatting and naming are settled by `zig fmt`. Do not report them.

## What to report

Report defects and costs a reviewer would raise on this push. Specifically:

- Correctness, concurrency, lifetime and resource defects in the changed code.
- A contract, doc comment or module header that the change has made untrue.
- A missing test for a predicate or path the change introduces.
- A defect ELSEWHERE in a file this change touches, when the change makes it
  reachable, makes it wrong, or shows it was always wrong. **These are in scope
  even though they are outside the diff hunks.** Read the whole of every file the
  diff touches, not only the hunks.
- A build or packaging consequence, including a target the change breaks.
- A test or example whose cost or exit behaviour is wrong.

Do not pad. Do not report the same defect twice under two headings. Report
nothing rather than something you do not believe.

## Output — a single fenced JSON block, last thing you write

Write your reasoning first if you want, then end with exactly one fenced block:

```json
{
  "network_used": false,
  "files_read": ["tree/src/completion_queue.zig", "diff.patch"],
  "findings": [
    {
      "title": "One sentence naming the defect.",
      "file": "src/completion_queue.zig",
      "start_line": 309,
      "end_line": 320,
      "category": "functional_correctness",
      "severity": "major",
      "effort": "quick_win",
      "detail": "What is wrong, the concrete failure it produces, and the fix."
    }
  ]
}
```

Field rules:

- `file` is relative to `tree/`, with no `tree/` prefix.
- `start_line` and `end_line` are line numbers in that file AS IT IS IN `tree/`.
  Cite the lines the defect is at, not the lines of the fix.
- `category` is one of: `functional_correctness`, `data_integrity`, `stability`,
  `performance`, `maintainability`.
- `severity` is one of: `critical`, `major`, `minor`, `trivial`.
- `effort` is one of: `quick_win`, `moderate`, `heavy_lift`.
- `findings` may be empty. `files_read` may not.
