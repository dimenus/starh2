#!/bin/sh
# The raw rows and logs this script reads or names are no longer in the repo.
# They are in the off-repo archive ~/Dropbox/starh2-captures/pr14-placement-followup-captures-3a6fad3.tar.gz
# (every file this branch added under captures/, as of 3a6fad3). Extract it at
# the repo root to restore them before rerunning.
# Tables for the --conn-balance bench (fix-rows.txt).
#
#   captures/zio-placement-probe/fix-summary.sh
#
# 1. SSE 500 outcome per arm: knee (all 500 delivered, p50 > 200 us),
#    ok, failed closed.
# 2. Per-arm medians and burst rates.
# 3. Paired per-round ratios of PLB against PL, and of PL and PLB against
#    WSL.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
SUM=$HERE/../../tools/zio-arm-ab-summary.sh
ROWS=${1:-$HERE/fix-rows.txt}
compact() {
  awk '/^paired rounds/ { h = $0; sub(/^paired rounds, ratio = /, "", h); next }
       /^[a-z]/ && !/^paired/ { m = $0; next }
       /pairs=/ { sub(/^ +/, ""); sub(/per-round-ratio /, ""); print h " | " m " | " $0 }'
}
echo "## 1. SSE 500 outcome"
grep -E '^r[0-9]+ [A-Z]+ sse500 ' "$ROWS" | awk '
  / streams=/ { for (i = 1; i <= NF; i++) if ($i ~ /^failed=/) f[$1 " " $2] = substr($i, 8) + 0 }
  / latency / { for (i = 1; i <= NF; i++) if ($i ~ /^p50=/) { p = substr($i, 5)
      if (p ~ /ms$/) { sub(/ms$/, "", p); p *= 1000 } else { sub(/µs$/, "", p); p += 0 }
      lat[$1 " " $2] = p } }
  END { for (k in f) { split(k, a, " "); c = (f[k] > 0) ? "failclosed" : (lat[k] > 200 ? "knee" : "ok"); n[a[2] " " c]++ }
        for (x in n) print x, n[x] }' | sort
echo "## 2. per-arm medians"
REF=WSL sh "$SUM" "$ROWS" | sed -n '1,/^paired/p' | grep -v '^paired'
echo "## 3a. paired against PL"
REF=PL sh "$SUM" "$ROWS" | compact
echo "## 3b. paired against WSL"
REF=WSL sh "$SUM" "$ROWS" | compact
