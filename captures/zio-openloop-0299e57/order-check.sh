#!/bin/sh
# Does run order bias a pair in a multi-arm session?
#
# tools/zio-arm-ab.sh rotated the arm list cyclically, which keeps each
# arm's neighbours: with 7 arms, WS2 ran right after WS in every round but
# the one where WS2 opened the round. The A/A pair (WS2 against WS, one
# binary) then read about 1.2 at both open-loop loads. This splits every
# pair's per-round ratio by which arm ran first in that round, from the row
# order in the file.
#
#   captures/zio-openloop-0299e57/order-check.sh <rows> <arm1> <arm2> <metric-label> <field>
#   e.g. order-check.sh main-rows.txt WS WS2 open590k-e2 p99us
set -eu
ROWS=$1; A1=$2; A2=$3; LABEL=$4; FIELD=$5
awk -v a1="$A1" -v a2="$A2" -v lab="$LABEL" -v fld="$FIELD" '
  $3 == lab && ($2 == a1 || $2 == a2) {
    for (i = 1; i <= NF; i++) if (index($i, fld "=") == 1) v = substr($i, length(fld) + 2) + 0
    r = $1
    if (!(r in first)) first[r] = $2
    val[r, $2] = v
    if (!(r in seen)) { seen[r] = 1; order[++n] = r }
  }
  END {
    for (i = 1; i <= n; i++) {
      r = order[i]
      if (!((r, a1) in val) || !((r, a2) in val) || val[r, a1] == 0) continue
      x = val[r, a2] / val[r, a1]
      printf "%-4s first=%-4s %s=%d %s=%d ratio=%.3f\n", r, first[r], a1, val[r, a1], a2, val[r, a2], x
      if (first[r] == a1) { s1 += log(x); n1++ } else { s2 += log(x); n2++ }
    }
    if (n1) printf "%s ran first: n=%d geometric-mean ratio %s/%s = %.3f\n", a1, n1, a2, a1, exp(s1 / n1)
    if (n2) printf "%s ran first: n=%d geometric-mean ratio %s/%s = %.3f\n", a2, n2, a2, a1, exp(s2 / n2)
  }' "$ROWS"
