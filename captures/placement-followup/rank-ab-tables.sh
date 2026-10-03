#!/bin/sh
# The raw rows and logs this script reads or names are no longer in the repo.
# They are in the off-repo archive ~/Dropbox/starh2-captures/pr14-placement-followup-captures-3a6fad3.tar.gz
# (every file this branch added under captures/, as of 3a6fad3). Extract it at
# the repo root to restore them before rerunning.
# The tables in rank-ab-summary.md, regenerated from the rank-ab-*.txt rows.
#   captures/placement-followup/rank-ab-tables.sh > rank-ab-tables.md
# Cells: median per-round ratio arm/WS with the number of rounds (n); `*` =
# outside this session's WS2/WS min-max. Each mix ratio row is followed by
# the same ratio over complete-delivery pairs only (both rows: every stream
# opened, delivered, never ended early or stopped) with its n.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
S() { uv run --no-project python "$HERE/summarize.py" "$@"; }
MIX='heavy_sat_rounds|heavy saturation, rounds and conns with p50 over 200us;place|churn on heavy executors, median share'
MIXR='heavy_worst_p99us|worst heavy conn p99;light_worst_p99us|worst light conn p99;cpu_us_ev|CPU/event;churn_ok|churn requests (higher better);stopped|stopped streams (sum)'
spec() { { echo "$MIX"; echo "$MIXR"; } | tr ';' '\n' | while IFS='|' read n l; do printf '%s|%s|%s %s;' "$1" "$n" "$1" "$l"; done | sed 's/;$//'; }
echo "### SSE 500 at 2 executors"; echo
COMPACT='sse500|classes|outcome;sse500|p50|p50;sse500|p99|p99;sse500|heavy_worst_p99us|worst conn p99' SHOW_N=1 S "$HERE/rank-ab-sse500.txt"
for f in mix-unpaced mixm-unpaced mix-paced mixm-paced; do
  m=${f%%-*}
  echo; echo "### $f (8 executors = e8, 12 = eprod)"; echo
  COMPACT="$(spec $m-e8);$(spec $m-eprod)" SHOW_N=1 COMPLETE=1 S "$HERE/rank-ab-$f.txt"
done
