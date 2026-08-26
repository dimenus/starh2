#!/usr/bin/env python3
"""Extract an arm's structured report from its raw output.

PROTOCOL
Every arm, whatever the vendor, ends its answer with one fenced ```json block in
the shape brief.md specifies. This is the only parser, so all arms are read the
same way and a vendor cannot score better by formatting differently.

It takes the LAST fenced json block, because an arm often quotes the brief's
example block earlier in its answer. Taking the first would score the example.

It exits non-zero when it cannot find or parse a block. That is deliberate: an
unparseable arm must be a loud failure, never an arm with zero findings.
"""
import json
import re
import sys

FENCE = re.compile(r"```(?:json)?\s*\n(.*?)```", re.S)


def extract(raw: str) -> dict:
    blocks = FENCE.findall(raw)
    for text in reversed(blocks):
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and "findings" in obj:
            return obj
    # Fallback: the regex mispairs fences when the answer also contains a
    # non-json code fence whose opener carries text after the backticks
    # (```85:91:path). Walk the lines and treat each line that starts with
    # ``` as a fence toggle, which is the markdown reading. This runs only
    # after the regex found nothing, so a run the regex already parsed can
    # never change its result.
    toggled = []
    cur = None
    for line in raw.split("\n"):
        if line.startswith("```"):
            if cur is None:
                cur = []
            else:
                toggled.append("\n".join(cur))
                cur = None
        elif cur is not None:
            cur.append(line)
    for text in reversed(toggled):
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and "findings" in obj:
            return obj
    raise ValueError("no fenced json block with a 'findings' key")


def normalise(obj: dict) -> dict:
    out = {
        "network_used": bool(obj.get("network_used", False)),
        "files_read": list(obj.get("files_read") or []),
        "findings": [],
    }
    for f in obj.get("findings") or []:
        out["findings"].append(
            {
                "title": str(f.get("title", "")).strip(),
                "file": str(f.get("file", "")).strip().lstrip("./").removeprefix("tree/"),
                "start_line": int(f.get("start_line") or 0),
                "end_line": int(f.get("end_line") or f.get("start_line") or 0),
                "category": str(f.get("category", "")).strip().lower(),
                "severity": str(f.get("severity", "")).strip().lower(),
                "effort": str(f.get("effort", "")).strip().lower(),
                "detail": str(f.get("detail", "")).strip(),
            }
        )
    return out


if __name__ == "__main__":
    raw = open(sys.argv[1], encoding="utf-8", errors="replace").read()
    report = normalise(extract(raw))
    json.dump(report, open(sys.argv[2], "w"), indent=2)
    print(f"parsed {len(report['findings'])} findings, "
          f"{len(report['files_read'])} files_read, network_used={report['network_used']}")
