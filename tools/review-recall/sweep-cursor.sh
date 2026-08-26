#!/usr/bin/env bash
# Full cursor-agent arm. Same resume rule as sweep-fable.sh: a rep with a
# non-empty result.txt is skipped. Keep JOBS low; ask-mode runs are cheap
# but the vendor rate-limits.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd -P)
BIN=${1:?usage: sweep-cursor.sh <cursor-binary> <model> <workdir> [reps] [jobs]}
MODEL=${2:?}
WORK=$(cd "${3:?}" && pwd -P)
N=${4:-3}
JOBS=${5:-4}
COMMITS="f1681a4d e04e6e93 930921f4 bd7b9ed6 646a0b4c ea050293 16df720e 0c9271ce"
for c in $COMMITS; do
  for r in $(seq 1 "$N"); do
    if [ -s "$WORK/$c/cursor-r$r/result.txt" ]; then
      echo "skip $c cursor-r$r (exists)" >&2
      continue
    fi
    echo "$c $r"
  done
done | xargs -P "$JOBS" -n 2 bash -c 'bash "$0" "$1" "$2" "$3" "$4" "$5"' "$HERE/run-cursor.sh" "$BIN" "$MODEL" "$WORK"
echo "SWEEP DONE"
