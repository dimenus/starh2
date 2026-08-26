#!/usr/bin/env bash
# Judge every recorded run that has no judgement yet.
#   judge-all.sh [arm ...]
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ARMS=("$@"); [ ${#ARMS[@]} -gt 0 ] || ARMS=($(ls "$HERE/runs" 2>/dev/null))
n=0; f=0
for arm in "${ARMS[@]}"; do
  for cdir in "$HERE/runs/$arm"/*/; do
    c="$(basename "$cdir")"
    [ -s "$cdir/report.json" ] || continue
    [ -s "$cdir/judgement.json" ] && { echo "skip $arm/$c"; continue; }
    echo "judge $arm/$c ..."
    if python3 "$HERE/judge.py" "$arm" "$c"; then n=$((n+1)); else echo "JUDGE FAILED $arm/$c" >&2; f=$((f+1)); fi
  done
done
echo "judged=$n failed=$f"
[ $((n+f)) -gt 0 ] || { echo "FATAL: judged nothing" >&2; exit 4; }
