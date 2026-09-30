#!/bin/sh
# Check tools/zio-arm-ab.sh rotate() for pair and slot balance over 2n rounds,
# for n = 2 to 7 arms. Prints any ordered pair that does not run first in
# exactly n of 2n rounds and any arm/slot not hit exactly twice; prints
# BALANCED otherwise. Exits 1 on any imbalance.
#
#   captures/zio-openloop-0299e57/rotate-check.sh
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
eval "$(sed -n '/^rotate() {/,/^}/p' "$HERE/../../tools/zio-arm-ab.sh")"
bad=0
# rotate() sets a global n, so the arm count here is m.
for m in 2 3 4 5 6 7; do
  ARMS=""; i=1; while [ $i -le $m ]; do ARMS="$ARMS a$i"; i=$((i+1)); done
  r=1; all=""
  while [ $r -le $((2 * m)) ]; do rotate $r; all="$all|$ORDER"; r=$((r+1)); done
  n=$m
  out=$(echo "$all" | tr '|' '\n' | awk -v n=$n 'NF {
      for (i = 1; i <= NF; i++) { pos[$i, i]++; arms[$i] = 1; for (j = i + 1; j <= NF; j++) first[$i, $j]++ } }
    END {
      for (x in arms) for (y in arms) if (x != y && first[x, y] != n) print "n=" n " " x " before " y ": " first[x, y] + 0
      if (n > 2) for (x in arms) for (p = 1; p <= n; p++) if (pos[x, p] != 2) print "n=" n " " x " slot " p ": " pos[x, p] + 0
    }')
  if [ -n "$out" ]; then echo "$out"; bad=1; else echo "n=$n: BALANCED over $((2 * n)) rounds"; fi
done
exit $bad
