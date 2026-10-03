#!/bin/sh
# The raw rows and logs this script reads or names are no longer in the repo.
# They are in the off-repo archive ~/Dropbox/starh2-captures/pr14-placement-followup-captures-3a6fad3.tar.gz
# (every file this branch added under captures/, as of 3a6fad3). Extract it at
# the repo root to restore them before rerunning.
# The per-OS tables in the placement report, regenerated from the rows.
#   captures/placement-followup/report-tables.sh
# Cells: median per-round ratio arm/WS (lower is better unless the row says
# higher); `*` = outside the in-session WS2/WS A/A min-max; knee rows count
# rounds. Stopped = streams silent for the last 1 s+ of the window, summed.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
S="uv run --no-project python $HERE/summarize.py"
BASE='sse500|classes|SSE 500 @2 exec: outcome;sse500|p50|SSE 500 @2: p50;sse500|p99|SSE 500 @2: p99;cpu200|cpu|CPU/event, 200 streams, 1 conn;cpu200c10|cpu|CPU/event, 200 streams, 10 conns;oneshot-e2|rps|one-shot req/s @2 (higher better);oneshot-e8|rps|one-shot req/s @8 (higher better);oneshot-lat-e2|p50us|closed-loop p50 @2;oneshot-lat-e2|p99us|closed-loop p99 @2;oneshot-lat-e8|p99us|closed-loop p99 @8'
MIX='classes|outcome;heavy_worst_p99us|worst heavy conn p99;light_worst_p99us|worst light conn p99;cpu_us_ev|CPU/event;stopped|stopped streams (sum)'
mixspec() { echo "$MIX" | tr ';' '\n' | while IFS='|' read n l; do printf '%s|%s|%s %s;' "$1" "$n" "$1" "$l"; done; }
echo "## nachos (Linux, io_uring)"
echo
COMPACT="$BASE;open320k-e2|p50us|open loop 320k @2: p50;open320k-e2|p99us|open loop 320k @2: p99;open590k-e2|p50us|open loop 590k @2: p50;open590k-e2|p99us|open loop 590k @2: p99" $S "$HERE/linux-rows.txt"
echo
echo "nachos mix, churn paced 20 ms (second session; mix: heavy = executors/2, mixm: /4; eprod = 12 executors)"
echo
COMPACT="$(mixspec mix-e8)$(mixspec mix-eprod)$(mixspec mixm-e8)$(mixspec mixm-eprod | sed 's/;$//')" $S "$HERE/linux2-mix.txt" "$HERE/linux2-mixm.txt"
echo
echo "nachos mix, churn unpaced (first session)"
echo
COMPACT="$(mixspec mix-e8)$(mixspec mix-eprod | sed 's/;$//')" $S "$HERE/linux-rows.txt"
echo
echo "## Mac (macOS, kqueue)"
echo
COMPACT="$BASE;open60k-e2|p50us|open loop 60k @2: p50;open60k-e2|p99us|open loop 60k @2: p99;open120k-e2|p50us|open loop 120k @2: p50;open120k-e2|p99us|open loop 120k @2: p99" $S "$HERE/mac-rows.txt"
echo
echo "Mac mix, churn paced 20 ms (eprod = 18 executors, overloaded for every arm)"
echo
COMPACT="$(mixspec mix-e8)$(mixspec mix-eprod)$(mixspec mixm-e8)$(mixspec mixm-eprod | sed 's/;$//')" $S "$HERE/mac-rows.txt" "$HERE/mac-rows-mixm.txt"
