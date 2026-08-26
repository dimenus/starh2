#!/usr/bin/env python3
"""Decide, per ground-truth finding, whether an arm reported the same defect.

    judge.py <arm> <commit> [--judge <cursor-model> ...]

PROTOCOL
Matching is semantic, so a string comparison cannot do it and a line-overlap
filter must not do it alone: an arm can cite the right defect at the wrong line,
and a filter that drops that pair deletes a true result. So EVERY pair of
(ground-truth row, arm finding) for the commit goes to the judge. The line
anchors are given to the judge as evidence, never as a gate.

INDEPENDENCE
The judge must share no vendor with the arm under test, or a model grades its own
family. VENDOR below encodes that, and the script refuses a same-vendor judge.
Two judges run per (arm, commit) and both must agree. A disagreement is recorded
as `contested` and counted in neither direction, so it stays visible instead of
being resolved by a coin toss.

The judge sees the corpus tree (its cwd) so it can open the cited lines, and it
never sees findings.json beyond the rows for this one commit.
"""
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.environ.get("CURSOR_BIN", os.path.expanduser("~/.local/bin/cursor-agent-personal"))

VENDOR = {
    "gpt-5.3-codex-high": "openai", "gpt-5.2": "openai", "gpt-5.6-sol-high": "openai",
    "cursor-grok-4.6-high": "xai", "cursor-grok-4.6-medium": "xai",
    "claude-sonnet-5-thinking-high": "anthropic",
    "claude-fable-5-thinking-high": "anthropic",
    "claude-opus-5-thinking-high": "anthropic",
}
ARM_VENDOR = {"codex-high": "openai", "grok-high": "xai", "fable-cursor": "anthropic"}
DEFAULT_JUDGES = {
    "openai": ["cursor-grok-4.6-high", "claude-sonnet-5-thinking-high"],
    "xai": ["gpt-5.3-codex-high", "claude-sonnet-5-thinking-high"],
    "anthropic": ["gpt-5.3-codex-high", "cursor-grok-4.6-high"],
}

PROMPT = """You are grading a code review, not performing one.

Your working directory holds `tree/`, the source tree of the commit that was
reviewed. Open any file in it to check a line reference.

Below are REFERENCE findings (the ones a reference reviewer raised on this
commit) and CANDIDATE findings (the ones the reviewer under test raised).

For each REFERENCE finding, decide whether ANY candidate reports THE SAME
DEFECT. Judge the substance, not the wording:

- The same defect described in different words IS a match.
- A different line number for the same defect IS still a match. Line anchors are
  evidence, not the test.
- A candidate that names the same symbol but a different failure is NOT a match.
- A candidate that is broader and clearly contains the reference defect as one of
  its named consequences IS a match. A vague catch-all is NOT.

REFERENCE FINDINGS
{reference}

CANDIDATE FINDINGS
{candidate}

Answer with exactly one fenced json block, and nothing after it:

```json
{{"matches": [{{"reference_id": "F3", "candidate_index": 0, "why": "one sentence"}},
              {{"reference_id": "F6", "candidate_index": null, "why": "one sentence"}}]}}
```

Give one entry for every reference id above. `candidate_index` is the 0-based
index of the matching candidate, or null when none matches.
"""


def render(rows, kind):
    if not rows:
        return "(none)"
    out = []
    for i, r in enumerate(rows):
        head = r["id"] if kind == "ref" else f"index {i}"
        out.append(
            f"--- {head}\nfile: {r['file']} lines {r['start_line']}-{r['end_line']}\n"
            f"title: {r['title']}\n{r.get('summary') or r.get('detail','')}\n"
        )
    return "\n".join(out)


def run_judge(model, cdir, prompt, odir, tag):
    os.makedirs(odir, exist_ok=True)
    ppath = os.path.join(odir, f"judge-{tag}-prompt.txt")
    rpath = os.path.join(odir, f"judge-{tag}-raw.txt")
    open(ppath, "w").write(prompt)
    t0 = time.time()
    with open(ppath) as fin, open(rpath, "w") as fout, open(rpath + ".err", "w") as ferr:
        rc = subprocess.call(
            [BIN, "--print", "--output-format", "text", "--mode", "ask", "--trust",
             "--model", model],
            cwd=cdir, stdin=fin, stdout=fout, stderr=ferr,
        )
    raw = open(rpath, encoding="utf-8", errors="replace").read()
    sys.path.insert(0, HERE)
    from parse_arm import FENCE
    for text in reversed(FENCE.findall(raw)):
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and "matches" in obj:
            return {"model": model, "rc": rc, "seconds": round(time.time() - t0),
                    "matches": obj["matches"]}
    raise ValueError(f"judge {model} produced no parseable verdict; see {rpath}")


def main():
    arm, commit = sys.argv[1], sys.argv[2]
    judges = sys.argv[4:] if len(sys.argv) > 3 and sys.argv[3] == "--judge" else None
    vendor = ARM_VENDOR.get(arm)
    if vendor is None:
        sys.exit(f"FATAL: arm {arm} has no recorded vendor; add it to ARM_VENDOR")
    judges = judges or DEFAULT_JUDGES[vendor]
    for m in judges:
        if VENDOR.get(m) == vendor:
            sys.exit(f"FATAL: judge {m} shares vendor '{vendor}' with the arm under test")

    truth = json.load(open(os.path.join(HERE, "findings.json")))["findings"]
    ref = [r for r in truth if r["commit"] == commit]
    if not ref:
        sys.exit(f"FATAL: no reference findings for {commit}")

    odir = os.path.join(HERE, "runs", arm, commit)
    report = json.load(open(os.path.join(odir, "report.json")))
    cand = report["findings"]

    prompt = PROMPT.format(reference=render(ref, "ref"), candidate=render(cand, "cand"))
    verdicts = [run_judge(m, os.path.join(HERE, "corpus", commit), prompt, odir, f"{i}-{m}")
                for i, m in enumerate(judges)]

    merged = {}
    for r in ref:
        picks = []
        for v in verdicts:
            hit = next((m for m in v["matches"] if m.get("reference_id") == r["id"]), None)
            picks.append(None if hit is None else hit.get("candidate_index"))
        agree = len(set(map(lambda x: -1 if x is None else x, picks))) == 1
        merged[r["id"]] = {
            "picks": picks,
            "agreed": agree,
            "matched": bool(agree and picks[0] is not None),
            "contested": not agree,
        }
    out = {"arm": arm, "commit": commit, "judges": [v["model"] for v in verdicts],
           "judge_seconds": [v["seconds"] for v in verdicts],
           "n_candidates": len(cand), "per_reference": merged}
    json.dump(out, open(os.path.join(odir, "judgement.json"), "w"), indent=2)
    print(json.dumps({k: v["matched"] for k, v in merged.items()}),
          "contested:", [k for k, v in merged.items() if v["contested"]])


if __name__ == "__main__":
    main()
