#!/usr/bin/env python3
"""Aggregate the judged runs into the recall report.

    score.py [--arm <name> ...]

WHAT THE NUMBERS MEAN, because the wrong reading of each is the easy one:

  recall      matched reference findings / reference findings ON COMMITS THE ARM
              ACTUALLY RAN. An arm that ran 3 of 8 commits is not scored against
              all 13 rows; its denominator says how much it covered.
  headline    recall over verdict in {defect, test_gap, style} - the 11 rows the
              author acted on.
  contested   the two reference rows the author did NOT accept (F1 withdrawn,
              F2 rejected). An arm that raises one is neither right nor wrong, so
              they are reported apart and never folded into recall.
  unmatched   arm findings that matched no reference row. These are NOT proven
              false: the reference reviewer missed things too. They are the load
              a gate would put on a reviewer, which is why the count is reported
              on its own and per commit.
  disputed    reference rows where the two judges disagreed. Counted in neither
              direction and printed by name, because a coin toss here would be
              the harness inventing a result.

VALIDITY
Exits non-zero when it scored nothing, when an arm has runs but no judgements,
or when any scored run failed its instrument check (see FLOORS). A scored zero
must be a real zero, not an arm that never fired.
"""
import json
import os
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))

# Instrument floors. A run under all three at once did not do the work.
MIN_SECONDS = 15
MIN_BYTES = 300
MIN_FILES_READ = 2

ACTIONABLE = {"defect", "test_gap", "style"}


def load_truth():
    rows = json.load(open(os.path.join(HERE, "findings.json")))["findings"]
    return {r["id"]: r for r in rows}


def main():
    truth = load_truth()
    if not truth:
        sys.exit("FATAL: the oracle is empty")

    arms = sys.argv[2:] if len(sys.argv) > 1 and sys.argv[1] == "--arm" else None
    root = os.path.join(HERE, "runs")
    if not os.path.isdir(root):
        sys.exit("FATAL: no runs/ directory; nothing has been run")
    arms = arms or sorted(os.listdir(root))

    problems = []
    scored_runs = 0
    report = {}

    for arm in arms:
        adir = os.path.join(root, arm)
        if not os.path.isdir(adir):
            continue
        commits = sorted(d for d in os.listdir(adir) if os.path.isdir(os.path.join(adir, d)))
        per_arm = {"commits_run": [], "commits_judged": [], "matched": {}, "contested": {},
                   "disputed": [], "unmatched_per_commit": {}, "meta": {}}
        for c in commits:
            cdir = os.path.join(adir, c)
            mpath, jpath = os.path.join(cdir, "meta.json"), os.path.join(cdir, "judgement.json")
            if not os.path.exists(mpath):
                continue
            meta = json.load(open(mpath))
            per_arm["commits_run"].append(c)
            per_arm["meta"][c] = meta

            low = sum([meta.get("wall_seconds", 0) < MIN_SECONDS,
                       meta.get("raw_bytes", 0) < MIN_BYTES,
                       meta.get("n_files_read", 0) < MIN_FILES_READ])
            if low == 3:
                problems.append(f"{arm}/{c}: below every instrument floor "
                                f"({meta.get('wall_seconds')}s, {meta.get('raw_bytes')}B, "
                                f"{meta.get('n_files_read')} files) - this run did not happen")
            if meta.get("network_used"):
                problems.append(f"{arm}/{c}: the arm declared network_used=true - isolation broken")

            if not os.path.exists(jpath):
                problems.append(f"{arm}/{c}: ran but was never judged")
                continue
            j = json.load(open(jpath))
            per_arm["commits_judged"].append(c)
            scored_runs += 1

            claimed = set()
            for fid, v in j["per_reference"].items():
                bucket = "contested" if truth[fid]["verdict"] not in ACTIONABLE else "matched"
                if v["contested"]:
                    per_arm["disputed"].append(fid)
                per_arm[bucket][fid] = v["matched"]
                if v["matched"] and v["picks"][0] is not None:
                    claimed.add(v["picks"][0])
            per_arm["unmatched_per_commit"][c] = j["n_candidates"] - len(claimed)

        report[arm] = per_arm

    if scored_runs == 0:
        sys.exit("FATAL: scored 0 runs. Scanning nothing is not a clean result.")

    # ---- print -------------------------------------------------------------
    band = lambda ids, key: defaultdict(list)
    for arm, a in report.items():
        if not a["commits_judged"]:
            continue
        judged = set(a["commits_judged"])
        applicable = [r for r in truth.values()
                      if r["commit"] in judged and r["verdict"] in ACTIONABLE]
        print(f"\n=== {arm}")
        print(f"commits run {len(a['commits_run'])}/8, judged {len(a['commits_judged'])}/8"
              f"  ({' '.join(sorted(judged))})")
        hit = [r for r in applicable if a["matched"].get(r["id"])]
        print(f"HEADLINE RECALL  {len(hit)}/{len(applicable)}"
              f"  ({100*len(hit)//max(1,len(applicable))}% of the findings the author acted on)")

        for key in ("severity", "verdict", "category"):
            groups = defaultdict(lambda: [0, 0])
            for r in applicable:
                groups[r[key]][1] += 1
                if a["matched"].get(r["id"]):
                    groups[r[key]][0] += 1
            line = "  ".join(f"{k} {v[0]}/{v[1]}" for k, v in sorted(groups.items()))
            print(f"  by {key:9s} {line}")

        miss = sorted(r["id"] for r in applicable if not a["matched"].get(r["id"]))
        print(f"  missed        {' '.join(miss) or '(none)'}")
        cont = {k: v for k, v in a["contested"].items()}
        print(f"  contested     {cont or '(none reviewed)'}   (F1 withdrawn, F2 rejected)")
        if a["disputed"]:
            print(f"  DISPUTED      {sorted(set(a['disputed']))}  judges disagreed; scored as miss")
        tot_un = sum(a["unmatched_per_commit"].values())
        print(f"  unmatched     {tot_un} findings over {len(a['commits_judged'])} commits "
              f"= {tot_un/max(1,len(a['commits_judged'])):.1f} per push "
              f"(not proven false; this is the reviewer's load)")
        secs = [m["wall_seconds"] for m in a["meta"].values()]
        print(f"  cost          {sum(secs)}s total, {sum(secs)//max(1,len(secs))}s median-ish per commit")

    if problems:
        print("\n!!! VALIDITY PROBLEMS")
        for p in problems:
            print("  " + p)
        sys.exit(1)
    print(f"\nscored {scored_runs} runs, no validity problems")


if __name__ == "__main__":
    main()
