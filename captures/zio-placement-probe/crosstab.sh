#!/bin/sh
# The raw rows and logs this script reads or names are no longer in the repo.
# They are in the off-repo archive ~/Dropbox/starh2-captures/pr14-placement-followup-captures-3a6fad3.tar.gz
# (every file this branch added under captures/, as of 3a6fad3). Extract it at
# the repo root to restore them before rerunning.
# Classify each run of rows.txt by where the SSE work ran, and cross-tab it
# against knee / ok / failed-closed per arm.
#
#   captures/zio-placement-probe/crosstab.sh [rows.txt]
#
# From the /trace probe of each run: the share of SSE handler loop
# iterations (handler_iter) on the busier of the two executor threads. 500
# streams arrive over two TCP connections (the server allows 256 concurrent
# streams per connection), and every task of a connection stays on the
# connection's executor under pinned + .local, so:
#   co-located  both connections' handlers on one executor (share >= 0.9)
#   split       handlers on both executors (share < 0.9)
#   none        no handler iterations counted (the run failed before SSE)
# Also printed per run: TCP connections established 5 s in, connEntry
# count per thread, and the busier thread's mean CPU.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
ROWS=${1:-$HERE/rows.txt}
grep '^k' "$ROWS" | awk '
  {
    arm = $2; cls = $3; tc = $4; sub(/tcp_conns=/, "", tc)
    probe = $0; sub(/.*probe=\[/, "", probe); sub(/\].*/, "", probe)
    n = split(probe, objs, /\},\{/)
    tot = 0; mx = 0; ce = ""
    for (i = 1; i <= n; i++) {
      o = objs[i]
      hi = o; sub(/.*"handler_iter":/, "", hi); sub(/,.*/, "", hi); hi += 0
      c = o; sub(/.*"conn_entry":/, "", c); sub(/,.*/, "", c)
      tot += hi; if (hi > mx) mx = hi; ce = ce (ce == "" ? "" : "/") c
    }
    share = (tot > 0) ? mx / tot : -1
    st = (tot == 0) ? "none" : (share >= 0.9 ? "co-located" : "split")
    cpu = $0; m1 = 0
    while (match(cpu, /mean=[0-9]+/)) { v = substr(cpu, RSTART + 5, RLENGTH - 5) + 0; if (v > m1) m1 = v; cpu = substr(cpu, RSTART + RLENGTH) }
    printf "%-4s %-2s %-10s %-10s tcp_conns=%s conn_entry=%s handler_share=%.3f busiest_thread_mean=%d\n", $1, arm, cls, st, tc, ce, share, m1
    x[arm SUBSEP st SUBSEP cls]++; arms[arm] = 1; sts[st] = 1
  }
  END {
    print ""
    for (a in arms) {
      printf "arm %s\n", a
      printf "  %-11s %5s %5s %11s\n", "", "knee", "ok", "failclosed"
      for (s in sts) {
        k = x[a, s, "knee"] + 0; o = x[a, s, "ok"] + 0; f = x[a, s, "failclosed"] + 0
        if (k + o + f) printf "  %-11s %5d %5d %11d\n", s, k, o, f
      }
    }
  }'
