#!/bin/sh
# Print the recheck report tables from main-rows.txt.
#
#   captures/zio-placement-recheck-0299e57/summarize.sh
#
# 1. Per-arm medians, burst rates, excluded rows.
# 2. Paired per-round ratios against WSL (WSL2/WSL is the A/A band).
# 3. Fail-closed counts for every SSE and CPU row, per arm.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
SUM=$HERE/../../tools/zio-arm-ab-summary.sh
ROWS=${1:-$HERE/main-rows.txt}

echo "## 1. per-arm medians"
REF=WSL sh "$SUM" "$ROWS" | sed -n '1,/^paired/p' | grep -v '^paired'
echo "## 2. paired against WSL"
REF=WSL sh "$SUM" "$ROWS" | awk '
  /^paired rounds/ { head = $0; sub(/^paired rounds, ratio = /, "", head); next }
  /^[a-z]/ && !/^paired/ { m = $0; next }
  /pairs=/ { sub(/^ +/, ""); sub(/per-round-ratio /, ""); print head " | " m " | " $0 }'
echo "## 3. fail-closed rows per arm (SSE and CPU phases)"
grep -E '^[rc][0-9]+ [A-Z0-9]+ (sse|cpu)[0-9]+(c[0-9]+)? .*failed=' "$ROWS" | awk '
  { for (i = 1; i <= NF; i++) if ($i ~ /^failed=/) f = substr($i, 8) + 0
    k = $2 " " $3; n[k]++; if (f > 0) fc[k]++ }
  END { for (k in n) printf "%-16s %d/%d fail-closed\n", k, fc[k] + 0, n[k] }' | sort
