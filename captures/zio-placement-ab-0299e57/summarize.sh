#!/bin/sh
# The raw rows and logs this script reads or names are no longer in the repo.
# They are in the off-repo archive ~/Dropbox/starh2-captures/pr14-placement-followup-captures-3a6fad3.tar.gz
# (every file this branch added under captures/, as of 3a6fad3). Extract it at
# the repo root to restore them before rerunning.
# Print every table the placement report is built from, from the raw rows.
#
#   captures/zio-placement-ab-0299e57/summarize.sh
#
# 1. Per-arm medians, burst rates, and the paired per-round ratios of every
#    arm against WS and against A (tools/zio-arm-ab-summary.sh with REF).
# 2. Every pinned one-shot rps row (not only medians), for the bimodality
#    question.
# 3. Per-thread CPU during the one-shot latency runs: the two busiest threads
#    (the two executors at --executors 2) and their ratio, per row.
# 4. io_uring polls per SSE event (mech747-summary.sh).
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
SUM=$HERE/../../tools/zio-arm-ab-summary.sh

compact() {
  # One line per metric: the pairs summary, prefixed by the pair heading.
  awk '/^paired rounds/ { head = $0; next } /^[a-z]/ && !/^paired/ { m = $0; next }
       /pairs=/ { sub(/^ +/, ""); print head " | " m " | " $0 }'
}

echo "## 1a. per-arm medians and burst (main-rows.txt)"
REF=WS sh "$SUM" "$HERE/main-rows.txt" | sed -n '1,/^paired/p' | grep -v '^paired'
echo "## 1b. paired against WS"
REF=WS sh "$SUM" "$HERE/main-rows.txt" | compact
echo "## 1c. paired against A"
REF=A sh "$SUM" "$HERE/main-rows.txt" | compact

echo "## 2. every pinned one-shot rps row"
grep -E '^r[0-9]+ (PL|PA) oneshot-e[0-9]+ ' "$HERE/main-rows.txt" |
  awk '{ for (i = 1; i <= NF; i++) if ($(i + 1) == "req/s,") print $2, $3, $1, $i }' | sort -k1,2 -k3V

echo "## 3. per-thread CPU (ticks) in one-shot latency runs: top two threads, max/min"
for f in main-rows.txt mech718-rows.txt; do
  [ -f "$HERE/$f" ] || continue
  echo "### $f"
  grep -E '^r[0-9]+ [A-Za-z0-9]+ oneshot-lat-e2 .*threads=' "$HERE/$f" | awk '
    { for (i = 1; i <= NF; i++) if ($i ~ /^threads=/) { split(substr($i, 9), t, ","); break }
      for (i = 1; i <= NF; i++) if ($i ~ /^p99us=/) p99 = substr($i, 7)
      r = (t[2] > 0) ? t[1] / t[2] : -1
      printf "%-4s %-5s top2=%s,%s ratio=%.3f p99us=%s\n", $1, $2, t[1], t[2], r, p99
      n[$2]++; rr[$2, n[$2]] = r; if (!($2 in seen)) { seen[$2] = 1; arms[++na] = $2 } }
    END {
      for (a = 1; a <= na; a++) {
        x = arms[a]; m = n[x]
        for (i = 1; i <= m; i++) v[i] = rr[x, i]
        for (i = 1; i < m; i++) for (j = i + 1; j <= m; j++) if (v[j] < v[i]) { tt = v[i]; v[i] = v[j]; v[j] = tt }
        printf "  %-5s n=%d busiest/second-busiest thread: median %.3f range %.3f-%.3f\n", x, m, v[int((m + 1) / 2)], v[1], v[m]
      }
    }'
done

echo "## 4. io_uring polls per SSE event"
sh "$HERE/mech747-summary.sh" "$HERE/mech747-rows.txt"

echo "## 5. one-connection one-shot (oneconn-rows.txt): medians, and pairs against WS"
REF=WS sh "$SUM" "$HERE/oneconn-rows.txt" | sed -n '1,/^paired/p' | grep -v '^paired'
REF=WS sh "$SUM" "$HERE/oneconn-rows.txt" | compact

echo "## 6. starh2 tree check (trees-rows.txt): 619242f (B, U) against 1080022 (WS, WSR)"
REF=B sh "$SUM" "$HERE/trees-rows.txt" | sed -n '1,/^paired/p' | grep -v '^paired'
REF=B sh "$SUM" "$HERE/trees-rows.txt" | compact
REF=U sh "$SUM" "$HERE/trees-rows.txt" | compact | grep 'WSR/U'
