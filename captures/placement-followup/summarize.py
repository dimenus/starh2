#!/usr/bin/env python3
"""Tables for the placement follow-up session (linux-rows.txt, mac-rows.txt).

    uv run --no-project python captures/placement-followup/summarize.py linux-rows.txt

Reads the rows of tools/zio-arm-ab.sh (nachos) and run-mac.sh (Mac); both
print the same row format.

Every metric is reported two ways:
- the per-arm median over rounds, with n;
- the per-round ratio arm/WS (rounds ran back to back, order rotated), as
  median and min-max. The A/A band is WS2/WS from the SAME session: an arm
  whose median ratio sits outside the A/A min-max is marked `*`. Inside the
  band means "not resolved", not "equal".

SSE 500 rows are classified before any latency is read:
- failclosed: any stream failed to open;
- partial: a stream opened but ended early (ended_early > 0) or delivered no
  event in the window (delivering < opened). The old client could not see
  either; such a row is not "ok".
- knee: every stream delivered and pooled p50 > 200 us (the two modes seen
  so far are about 10-40 us and 800-900 us);
- ok: the rest.
Latency medians use ok + knee rows only; the other classes are counted.
"""
import os
import re
import statistics
import sys
from collections import defaultdict

# REF=PL pairs every arm against PL instead; the A/A band stays WS2/WS, the
# only same-binary pair in the session.
REF, AA = os.environ.get("REF", "WS"), "WS2"


def us(v):
    if v.endswith("µs"):
        return float(v[:-2])
    if v.endswith("ms"):
        return float(v[:-2]) * 1000
    if v.endswith("s"):
        return float(v[:-1]) * 1e6
    return float(v)


def kv(line):
    return dict(re.findall(r"(\w+)=([^\s\[]+)", line))


rows = defaultdict(dict)  # (round, arm, metric) -> fields
arms = []
for path in sys.argv[1:]:
    for line in open(path, encoding="utf-8"):
        p = line.split()
        if len(p) < 3 or not re.match(r"^[rbcxq][0-9]+$", p[0]):
            continue
        rd, arm, metric = p[0], p[1], p[2]
        if arm not in arms:
            arms.append(arm)
        d = rows[(rd, arm, metric)]
        f = kv(line)
        if "streams=" in line and "opened=" in line:
            # Two files that reuse a (round, arm, metric) key (e.g. a paced
            # and an unpaced `mix-e8`) would merge silently; refuse instead.
            if "opened" in d:
                sys.exit(f"duplicate row key {rd} {arm} {metric} in {path}: pass files with distinct metric names separately")
            d.update({k: f[k] for k in ("opened", "delivering", "failed", "events", "ended_early", "scan_err", "ticks", "tck") if k in f})
        elif "sse latency" in line:
            d["p50"], d["p99"] = us(f["p50"]), us(f["p99"])
        elif "sse fair" in line:
            d["ev_min"], d["gap_max"] = int(f["ev_min"]), us(f["gap_max"])
            if "stopped" in f:
                d["stopped"] = int(f["stopped"])
        elif "sse conns" in line:
            for k in ("heavy_worst_p50us", "heavy_worst_p99us", "light_worst_p99us"):
                d[k] = float(f[k])
            # port:kind:delivering/streams:events:p50us:p99us per connection
            hs = [t.split(":") for t in line.split() if re.match(r"^\d+:h:\d+/\d+:", t)]
            d["heavy_n"] = len(hs)
            d["heavy_sat"] = sum(1 for t in hs if int(t[4]) > 200)
        elif " place " in line and "heavy_execs=" in line:
            d["place_frac"] = float(f["churn_on_heavy_frac"])
            d["place_uniform"] = float(f["uniform_frac"])
            d["heavy_shared"] = int(f["heavy_shared"])
        elif "churn conns" in line:
            d["churn_ok"], d["churn_p99"] = int(f["ok"]), us(f["p99"])
            d["churn_err"] = int(f["err"])
        elif " cpu executors=" in line:
            d["mix_ticks"] = int(f["ticks"])
            d["E"] = int(f["executors"])
            th = [int(x) for x in f["threads"].split(",") if x]
            top = th[: d["E"]]
            d["exec_spread"] = (max(top) / statistics.mean(top)) if top and statistics.mean(top) > 0 else None
        elif metric == "burst":
            d["burst"] = p[3]
        elif "req/s" in line or "WEDGE-OR-FAIL" in line:
            if "WEDGE-OR-FAIL" in line:
                d["wedge"] = True
                continue
            m = re.search(r"([0-9.]+) req/s", line)
            d["rps"] = float(m.group(1))
            m = re.search(r"(\d+) total, .* (\d+) succeeded", line)
            d["ok_all"] = m and m.group(1) == m.group(2)
            for k in ("p50us", "p99us", "p999us", "offered", "non200"):
                if k in f:
                    d[k] = float(f[k])


def complete(d):
    """Every stream opened, delivered, and kept delivering to the end."""
    return (int(d.get("failed", 0)) == 0 and int(d.get("ended_early", 0)) == 0
            and int(d.get("delivering", -1)) == int(d.get("opened", -2)) and d.get("stopped", 0) == 0)


def classify(d):
    if int(d.get("failed", 0)) > 0:
        return "failclosed"
    if int(d.get("ended_early", 0)) > 0 or int(d.get("delivering", 0)) < int(d.get("opened", 0)):
        return "partial"
    if "p50" not in d:
        return "noline"
    return "knee" if d["p50"] > 200 else "ok"


def value(metric, name, d):
    """The number a row contributes for one table line, or None."""
    if metric.startswith("sse"):
        if classify(d) not in ("ok", "knee"):
            return None
        return d.get(name)
    if metric.startswith("cpu"):
        # CPU rows carry no latency line, so only delivery is checked.
        if classify(d) in ("failclosed", "partial"):
            return None
        ev = int(d.get("events", 0))
        return int(d["ticks"]) / int(d["tck"]) * 1e6 / ev if ev and "ticks" in d else None
    if metric.startswith("mix"):
        if name == "cpu_us_ev":
            ev = int(d.get("events", 0))
            return d["mix_ticks"] / 100 * 1e6 / ev if ev and "mix_ticks" in d else None
        if name in ("heavy_sat", "place_frac", "heavy_shared"):
            return d.get(name)
        if name == "starved":
            return int(d.get("opened", 0)) - int(d.get("delivering", 0)) if "opened" in d else None
        return d.get(name)
    if d.get("wedge") or not d.get("ok_all") or d.get("non200", 0) > 0:
        return None
    if "offered" in d and d["rps"] < 0.98 * d["offered"]:
        return None
    return d.get(name)


def table(metric, name, label, lower_better=True):
    per_arm = defaultdict(dict)
    for (rd, arm, m), d in rows.items():
        if m != metric:
            continue
        v = value(metric, name, d)
        if v is not None:
            per_arm[arm][rd] = v
    if not per_arm:
        return
    def ratios(arm, ref):
        rs = [per_arm[arm][r] / per_arm[ref][r] for r in per_arm[arm] if r in per_arm.get(ref, {}) and per_arm[ref][r] > 0]
        return rs
    aa = ratios(AA, "WS") if AA in per_arm and "WS" in per_arm else []
    lo, hi = (min(aa), max(aa)) if aa else (None, None)
    print(f"\n{metric} {label}  (A/A {AA}/WS: " + (f"median {statistics.median(aa):.3f}, range {lo:.3f}-{hi:.3f}, n={len(aa)})" if aa else "none)"))
    print(f"| arm | median | n | ratio/{REF} median | ratio range | outside A/A |")
    print("|---|---:|---:|---:|---|---|")
    for arm in arms:
        if arm not in per_arm:
            continue
        vals = list(per_arm[arm].values())
        med = statistics.median(vals)
        if arm == REF:
            print(f"| {arm} | {med:.1f} | {len(vals)} | - | - | - |")
            continue
        rs = ratios(arm, REF)
        if not rs:
            print(f"| {arm} | {med:.1f} | {len(vals)} | - | - | - |")
            continue
        rm = statistics.median(rs)
        out = "*" if (lo is not None and arm != AA and (rm < lo or rm > hi)) else ""
        print(f"| {arm} | {med:.1f} | {len(vals)} | {rm:.3f} | {min(rs):.3f}-{max(rs):.3f} | {out} |")


def counts(metric, name, label):
    """For small counts a per-round ratio means nothing: print the total
    over rounds and how many rounds had any."""
    per = defaultdict(list)
    for (rd, arm, m), d in rows.items():
        if m == metric:
            v = value(metric, name, d)
            if v is not None:
                per[arm].append(v)
    if not per:
        return
    print(f"\n{metric} {label}: total over rounds (rounds with any / rounds)")
    print("| arm | total | rounds with any |")
    print("|---|---:|---:|")
    for arm in arms:
        if arm in per:
            print(f"| {arm} | {sum(per[arm])} | {sum(1 for v in per[arm] if v > 0)}/{len(per[arm])} |")


def classes(metric):
    c = defaultdict(lambda: defaultdict(int))
    for (rd, arm, m), d in rows.items():
        if m == metric:
            c[arm][classify(d)] += 1
    if not c:
        return
    print(f"\n{metric} outcome per arm (rounds)")
    print("| arm | ok | knee | partial | failclosed |")
    print("|---|---:|---:|---:|---:|")
    for arm in arms:
        if arm in c:
            x = c[arm]
            print(f"| {arm} | {x['ok']} | {x['knee']} | {x['partial']} | {x['failclosed']} |")


def burst():
    c = defaultdict(lambda: [0, 0])
    for (rd, arm, m), d in rows.items():
        if m == "burst":
            c[arm][1] += 1
            c[arm][0] += d.get("burst") == "FAIL"
    if c:
        print("\nburst (200 streams opened at once): fails/rounds  " + "  ".join(f"{a} {c[a][0]}/{c[a][1]}" for a in arms if a in c))


metrics_present = {m for (_, _, m) in rows}


def ratio_cell(metric, name, only_complete=False):
    per_arm = defaultdict(dict)
    for (rd, arm, m), d in rows.items():
        if m == metric:
            if only_complete and not complete(d):
                continue
            v = value(metric, name, d)
            if v is not None:
                per_arm[arm][rd] = v
    ref = per_arm.get("WS", {})
    def rs(arm):
        return [per_arm[arm][r] / ref[r] for r in per_arm.get(arm, {}) if r in ref and ref[r] > 0]
    aa = rs(AA)
    lo, hi = (min(aa), max(aa)) if aa else (None, None)
    cells = {}
    for arm in arms:
        if arm in ("WS", AA):
            continue
        r = rs(arm)
        if not r:
            cells[arm] = "-"
            continue
        m = statistics.median(r)
        out = lo is not None and (m < lo or m > hi)
        cells[arm] = f"{m:.2f}{'*' if out else ''} (n={len(r)})" if os.environ.get("SHOW_N") else f"{m:.2f}{'*' if out else ''}"
    band = f"{lo:.2f}-{hi:.2f}" if aa else "-"
    base = statistics.median(ref.values()) if ref else None
    return band, base, cells


def compact(spec):
    others = [a for a in arms if a not in ("WS", AA)]
    print("| shape / metric | WS median | A/A WS2/WS range | " + " | ".join(others) + " |")
    print("|---|---:|---|" + "---:|" * len(others))
    for metric, name, label in spec:
        if metric not in metrics_present:
            continue
        if name == "classes":
            c = defaultdict(lambda: defaultdict(int))
            for (rd, arm, m), d in rows.items():
                if m == metric:
                    c[arm][classify(d)] += 1
            def kn(a):
                n = sum(c[a].values())
                word = "p50>200us" if metric.startswith("mix") else "knee"
                return f"{c[a]['knee']}/{n} {word}" + (f", {c[a]['partial']} partial" if c[a]['partial'] else "") + (f", {c[a]['failclosed']} failclosed" if c[a]['failclosed'] else "")
            print(f"| {label} | {kn('WS')} | WS2 {kn(AA)} | " + " | ".join(kn(a) for a in others) + " |")
            continue
        if name == "stopped":
            t = defaultdict(int)
            seen = False
            for (rd, arm, m), d in rows.items():
                if m == metric:
                    t[arm] += d.get("stopped", 0)
                    seen = seen or "stopped" in d
            if not seen:
                # Rows from before the client printed `stopped=`: not
                # measured, which is not the same as zero.
                print(f"| {label} | not measured | - | " + " | ".join("-" for a in others) + " |")
                continue
            print(f"| {label} | {t['WS']} | WS2 {t[AA]} | " + " | ".join(str(t[a]) for a in others) + " |")
            continue
        if name in ("heavy_sat_rounds", "place"):
            per = defaultdict(list)
            for (rd, arm, m), d in rows.items():
                if m == metric:
                    per[arm].append(d)
            def cell(a):
                ds = per.get(a, [])
                if name == "heavy_sat_rounds":
                    if not any("heavy_n" in d for d in ds):
                        return "-"
                    rounds = sum(1 for d in ds if d.get("heavy_sat", 0) > 0)
                    part = sum(1 for d in ds if not complete(d))
                    dead = sum(1 for d in ds if int(d.get("opened", 0)) > 0 and int(d.get("delivering", 0)) == 0)
                    return (f"{rounds}/{len(ds)} sat ({sum(d.get('heavy_sat', 0) for d in ds)}/{sum(d.get('heavy_n', 0) for d in ds)} conns), {part} partial"
                            + (f", {dead} ZERO-EVENT collapse" if dead else ""))
                pd = [d for d in ds if "place_frac" in d]
                if not pd:
                    return "-"
                sh = sum(d.get("heavy_shared", 0) for d in pd)
                miss = len(ds) - len(pd)
                return (f"{statistics.median(d['place_frac'] for d in pd):.2f} (uniform {pd[0]['place_uniform']:.2f}), shared {sh}"
                        + (f", {miss} round(s) with no heavy placement to report" if miss else ""))
            print(f"| {label} | {cell('WS')} | WS2 {cell(AA)} | " + " | ".join(cell(a) for a in others) + " |")
            continue
        band, base, cells = ratio_cell(metric, name)
        print(f"| {label} | {base:.1f} | {band} | " + " | ".join(cells.get(a, "-") for a in others) + " |")
        if os.environ.get("COMPLETE") and metric.startswith("mix"):
            band, base, cells = ratio_cell(metric, name, only_complete=True)
            if base is None:
                print(f"| {label}, complete-delivery pairs | no complete WS row | - | " + " | ".join("-" for a in others) + " |")
            else:
                print(f"| {label}, complete-delivery pairs | {base:.1f} | {band} | " + " | ".join(cells.get(a, "-") for a in others) + " |")


if os.environ.get("COMPACT"):
    spec = []
    for line in os.environ["COMPACT"].split(";"):
        m, n, l = line.split("|")
        spec.append((m, n, l))
    compact(spec)
    sys.exit(0)
classes("sse500")
table("sse500", "p50", "p50 us")
table("sse500", "p99", "p99 us")
table("sse500", "heavy_worst_p99us", "worst connection p99 us")
table("sse500", "ev_min", "events delivered by the slowest stream (higher is better)", False)
for w in ("e2", "e8"):
    table(f"oneshot-{w}", "rps", "req/s (higher is better)", False)
for w in ("e2", "e8"):
    for k in ("p50us", "p99us", "p999us"):
        table(f"oneshot-lat-{w}", k, f"closed-loop {k}")
burst()
for m in ("cpu200", "cpu200c10"):
    table(m, "cpu", "CPU us per event")
for m in sorted(x for x in metrics_present if x.startswith("open")):
    for k in ("p50us", "p99us", "p999us"):
        table(m, k, f"open-loop {k}")
for m in sorted(x for x in metrics_present if x.startswith("mix")):
    classes(m)
    table(m, "p50", "pooled p50 us")
    table(m, "p99", "pooled p99 us")
    table(m, "heavy_worst_p50us", "worst heavy connection p50 us")
    table(m, "heavy_worst_p99us", "worst heavy connection p99 us")
    table(m, "light_worst_p99us", "worst light connection p99 us")
    counts(m, "starved", "streams that opened but delivered nothing")
    table(m, "ev_min", "events delivered by the slowest stream (10000 expected; higher is better)", False)
    counts(m, "stopped", "streams silent for the last 1 s or more of the window")
    table(m, "churn_ok", "short-connection requests completed (higher is better)", False)
    table(m, "churn_p99", "short-connection request p99 us")
    table(m, "cpu_us_ev", "server CPU us per SSE event")
    table(m, "exec_spread", "hottest executor thread / mean executor thread")
