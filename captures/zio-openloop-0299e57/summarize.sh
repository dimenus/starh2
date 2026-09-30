#!/bin/sh
# Print the open-loop report tables from one session's rows.
#
#   captures/zio-openloop-0299e57/summarize.sh [rows file, default main-rows.txt]
#
# 1. Per-arm medians, with rows excluded as unproven or behind the offered
#    rate counted.
# 2. Achieved/offered for every open-loop row (the keep-up check).
# 3. Paired per-round ratios of every arm against WS, and of PL and PA
#    against WSL (their same-tree control). WS2/WS is the A/A band.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
SUM=$HERE/../../tools/zio-arm-ab-summary.sh
ROWS=${1:-$HERE/main-rows.txt}

compact() {
  awk '/^paired rounds/ { head = $0; sub(/^paired rounds, ratio = /, "", head); next }
       /^[a-z]/ && !/^paired/ { m = $0; next }
       /pairs=/ { sub(/^ +/, ""); sub(/per-round-ratio /, ""); print head " | " m " | " $0 }'
}

echo "## 1. per-arm medians"
REF=WS sh "$SUM" "$ROWS" | sed -n '1,/^burst/p' | grep -v '^burst'

echo "## 2. achieved / offered, every open-loop row"
grep -E '^r[0-9]+ [A-Z0-9]+ open[0-9]+k-e' "$ROWS" | awk '
  { for (i = 1; i <= NF; i++) { if ($i ~ /^offered=/) o = substr($i, 9); if ($(i + 1) == "req/s,") a = $i }
    k = $2 " " $3; r = a / o; n[k]++; if (!(k in lo) || r < lo[k]) lo[k] = r; if (!(k in hi) || r > hi[k]) hi[k] = r
    if (r < 0.98) behind[k]++ }
  END { for (k in n) printf "%-22s rows=%d achieved/offered %.4f-%.4f behind=%d\n", k, n[k], lo[k], hi[k], behind[k] + 0 }' | sort

echo "## 3a. paired against WS"
REF=WS sh "$SUM" "$ROWS" | compact
echo "## 3b. paired against WSL (PL, PA)"
REF=WSL sh "$SUM" "$ROWS" | compact | grep -E '^(PL|PA)/WSL'
