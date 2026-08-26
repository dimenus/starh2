#!/usr/bin/env python3
"""Derive the ground-truth finding table from the CodeRabbit captures.

The captures in captures/coderabbit-zio-prs/ are the durable evidence: the
raw `gh api` JSON for zio PRs 711, 712, 713. This script re-derives the
13-finding oracle from them on every run, so the table cannot drift from
the evidence. Only the verdict labels are declared here, because a verdict
(by-design, rejected, still-open) is a human judgment recorded from the PR
threads, not a fact the JSON carries.
"""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CAPTURES = os.path.join(HERE, "..", "..", "captures", "coderabbit-zio-prs")

# Findings that live in inline review comments: finding id -> comment id.
INLINE = {
    "F1": ("pr711-comments.json", 3846089231),
    "F2": ("pr711-comments.json", 3849480409),
    "F3": ("pr712-comments.json", 3847020408),
    "F4": ("pr712-comments.json", 3847083700),
    "F5": ("pr712-comments.json", 3847149689),
    "F7": ("pr712-comments.json", 3853156069),
    "F8": ("pr713-comments.json", 3855167251),
    "F9": ("pr713-comments.json", 3855167276),
    "F10": ("pr713-comments.json", 3855167279),
    "F13": ("pr713-comments.json", 3855906664),
}

# Findings that live only in a review body: finding id -> (file, review id,
# the line-range marker that opens the section, the path the section is about).
IN_BODY = {
    "F6": ("pr712-reviews.json", 5012183609, "`382-397`:", "src/completion_queue.zig"),
    "F11": ("pr713-reviews.json", 5021479637, "`260-270`:", "src/completion_queue.zig"),
    "F12": ("pr713-reviews.json", 5021479637, "`1660-1678`:", "src/completion_queue.zig"),
}

# The commit each finding was raised against, and the human verdict.
# Verdicts: true-defect | test-gap | style-cost | by-design | rejected.
META = {
    "F1": ("f1681a4d", "by-design"),
    "F2": ("e04e6e93", "rejected"),
    "F3": ("930921f4", "true-defect"),
    "F4": ("bd7b9ed6", "true-defect"),
    "F5": ("646a0b4c", "true-defect"),
    "F6": ("646a0b4c", "test-gap"),
    "F7": ("ea050293", "true-defect"),
    "F8": ("16df720e", "true-defect"),
    "F9": ("16df720e", "true-defect"),
    "F10": ("16df720e", "true-defect"),
    "F11": ("16df720e", "true-defect"),
    "F12": ("16df720e", "style-cost"),
    "F13": ("0c9271ce", "true-defect"),
}

SEV_RE = re.compile(r"_\W*(Critical|Major|Minor|Trivial)_")
CAT_RE = re.compile(r"_\W*(Functional Correctness|Data Integrity & Integration|Stability & Availability|Performance & Scalability|Security)_")


def parse_marks(text):
    sev = SEV_RE.search(text)
    cat = CAT_RE.search(text)
    if not sev or not cat:
        raise SystemExit("cannot parse severity/category from: %r" % text[:200])
    return sev.group(1), cat.group(1)


def clean_body(text):
    cut = text.find("Prompt for AI Agents")
    if cut != -1:
        head = text[:cut]
        d = head.rfind("<details>")
        text = head[:d] if d != -1 else head
    text = re.sub(r"^> ?", "", text, flags=re.M)
    text = re.sub(r"</?(details|summary|blockquote)[^>]*>", "", text)
    return text.strip()


def main(out_path):
    findings = {}
    for fid, (fname, cid) in INLINE.items():
        comments = json.load(open(os.path.join(CAPTURES, fname)))
        c = next(c for c in comments if c["id"] == cid)
        sev, cat = parse_marks(c["body"])
        commit, verdict = META[fid]
        assert c["original_commit_id"].startswith(commit), (fid, c["original_commit_id"], commit)
        findings[fid] = {
            "commit": commit,
            "file": c["path"],
            "line": c.get("original_line") or c.get("line"),
            "severity": sev,
            "category": cat,
            "verdict": verdict,
            "source": "inline comment %d" % cid,
            "body": clean_body(c["body"]),
        }
    for fid, (fname, rid, marker, path) in IN_BODY.items():
        reviews = json.load(open(os.path.join(CAPTURES, fname)))
        r = next(r for r in reviews if r["id"] == rid)
        body = r["body"]
        start = body.find(marker)
        if start == -1:
            raise SystemExit("marker %r not found in review %d" % (marker, rid))
        section = body[start:]
        end = section.find("Prompt for AI Agents")
        section = section[:end] if end != -1 else section
        sev, cat = parse_marks(section)
        commit, verdict = META[fid]
        lines = marker.strip("`:").split("-")
        findings[fid] = {
            "commit": commit,
            "file": path,
            "line": int(lines[0]),
            "line_end": int(lines[1]),
            "severity": sev,
            "category": cat,
            "verdict": verdict,
            "source": "review body %d" % rid,
            "body": clean_body(section),
        }
    if len(findings) != 13:
        raise SystemExit("expected 13 findings, derived %d" % len(findings))
    with open(out_path, "w") as f:
        json.dump(findings, f, indent=2, sort_keys=True)
    per_commit = {}
    for fid, x in sorted(findings.items()):
        per_commit.setdefault(x["commit"], []).append(fid)
    print("derived 13 findings -> %s" % out_path)
    for c, ids in per_commit.items():
        print(" ", c, " ".join(ids))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "truth.json"))
