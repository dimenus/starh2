#!/usr/bin/env bash
# Score every run of one arm that has output but no score yet. 4 judges in
# parallel.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd -P)
WORK=$(cd "${1:?usage: score-all.sh <workdir> [arm]}" && pwd -P)
ARM=${2:-fable}
COMMITS="f1681a4d e04e6e93 930921f4 bd7b9ed6 646a0b4c ea050293 16df720e 0c9271ce"
for c in $COMMITS; do
  for run in "$WORK/$c"/$ARM-r*; do
    [ -d "$run" ] || continue
    name=$(basename "$run")
    if [ -s "$run/score.json" ]; then
      echo "skip $c $name (scored)" >&2
      continue
    fi
    if [ ! -s "$run/out.json" ] && [ ! -s "$run/result.txt" ]; then
      echo "NO OUTPUT $c $name" >&2
      continue
    fi
    echo "$c $name"
  done
done | xargs -P 4 -n 2 bash -c 'uv run "$0" "$1" "$2" "$3" || echo "SCORE FAILED $2 $3"' "$HERE/score.py" "$WORK"
echo "SCORING DONE"
