#!/usr/bin/env python3
"""Aggregate scored runs into the recall report.

Reports recall per severity band and per verdict class, never one aggregate.
Reports extras (candidate findings that match no reference finding) per run,
because a gate with 30 findings per push is unusable at any recall.
Reports validity: expected runs vs present, invalid runs, and suspect runs
(zero findings at low cost — an instrument that did not fire).
"""
import json
import os
import sys
from collections import defaultdict

COMMITS = ["f1681a4d", "e04e6e93", "930921f4", "bd7b9ed6", "646a0b4c", "ea050293", "16df720e", "0c9271ce"]


def main(workdir, arm, n_reps, truth_path):
    truth = json.load(open(truth_path))
    runs = []
    missing = []
    for c in COMMITS:
        for r in range(1, n_reps + 1):
            p = os.path.join(workdir, c, "%s-r%d" % (arm, r), "score.json")
            if not os.path.exists(p):
                missing.append("%s/%s-r%d" % (c, arm, r))
                continue
            runs.append(json.load(open(p)))
    print("== validity ==")
    print("expected %d runs, scored %d, missing %d" % (len(COMMITS) * n_reps, len(runs), len(missing)))
    for m in missing:
        print("  MISSING", m)
    ok = [r for r in runs if r["status"] == "ok"]
    for r in runs:
        if r["status"] != "ok":
            print("  INVALID %s/%s: %s" % (r["commit"], r["run"], r.get("error")))
    costs = [r["meta"].get("cost_usd") for r in ok if r["meta"].get("cost_usd")]
    median = sorted(costs)[len(costs) // 2] if costs else None
    for r in ok:
        c = r["meta"].get("cost_usd")
        flag = []
        if r["n_candidates"] == 0:
            flag.append("zero-findings")
        if median and c and c < 0.25 * median:
            flag.append("cost %.3f << median %.3f" % (c, median))
        if flag:
            print("  SUSPECT %s/%s: %s" % (r["commit"], r["run"], "; ".join(flag)))
    if median:
        print("cost per run: median %.3f USD, min %.3f, max %.3f" % (median, min(costs), max(costs)))

    per_finding = defaultdict(lambda: [0, 0])  # fid -> [hits, valid runs]
    for r in ok:
        for fid, m in r["matches"].items():
            per_finding[fid][1] += 1
            if m.get("candidate") is not None:
                per_finding[fid][0] += 1

    print("\n== per finding (%s, %d reps) ==" % (arm, n_reps))
    for fid in sorted(truth, key=lambda f: (truth[f]["commit"], f)):
        t = truth[fid]
        hits, n = per_finding.get(fid, [0, 0])
        print("  %-4s %s %-8s %-28s %-12s %d/%d" % (fid, t["commit"], t["severity"], t["category"], t["verdict"], hits, n))

    def band(keyfn, label):
        agg = defaultdict(lambda: [0, 0, 0, 0])  # key -> [hits, valid, findings, any-rep-hit]
        for fid, (hits, n) in per_finding.items():
            k = keyfn(truth[fid])
            agg[k][0] += hits
            agg[k][1] += n
            agg[k][2] += 1
            agg[k][3] += 1 if hits > 0 else 0
        print("\n== recall by %s ==" % label)
        for k, (hits, n, nf, anyhit) in sorted(agg.items()):
            rate = hits / n if n else 0.0
            print("  %-28s findings=%d  per-run recall %d/%d = %.0f%%  any-rep %d/%d" % (k, nf, hits, n, rate * 100, anyhit, nf))

    band(lambda t: t["severity"], "severity")
    band(lambda t: t["verdict"], "verdict")

    extras = [len(r["extras"]) for r in ok]
    if extras:
        print("\n== extras (candidates matching no reference finding) ==")
        print("per run: mean %.1f, min %d, max %d" % (sum(extras) / len(extras), min(extras), max(extras)))
        counts = defaultdict(int)
        for r in ok:
            for t in r["extra_titles"]:
                counts[t.strip()[:80]] += 1
        for t, n in sorted(counts.items(), key=lambda kv: -kv[1])[:25]:
            print("  %2dx %s" % (n, t))


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    workdir = sys.argv[1]
    arm = sys.argv[2] if len(sys.argv) > 2 else "fable"
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    truth = sys.argv[4] if len(sys.argv) > 4 else os.path.join(here, "truth.json")
    main(workdir, arm, n, truth)
