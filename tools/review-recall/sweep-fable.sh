#!/usr/bin/env bash
# Run the full Fable arm: every commit, N reps, JOBS in parallel.
# A run whose out.json already exists and is non-empty is skipped, so a
# killed sweep resumes by re-running this script.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd -P)
WORK=$(cd "${1:?usage: sweep-fable.sh <workdir> [reps] [jobs]}" && pwd -P)
N=${2:-3}
JOBS=${3:-6}
COMMITS="f1681a4d e04e6e93 930921f4 bd7b9ed6 646a0b4c ea050293 16df720e 0c9271ce"
for c in $COMMITS; do
  for r in $(seq 1 "$N"); do
    if [ -s "$WORK/$c/fable-r$r/out.json" ]; then
      echo "skip $c fable-r$r (exists)" >&2
      continue
    fi
    echo "$c $r"
  done
done | xargs -P "$JOBS" -n 2 bash -c 'bash "$0" "$1" "$2" "$3"' "$HERE/run-fable.sh" "$WORK"
echo "SWEEP DONE"
