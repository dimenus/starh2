#!/usr/bin/env bash
# Run one blinded Fable review of one commit. The agent gets Read/Grep/Glob
# only: no Bash and no web tools, so it has no network path. cwd is the bare
# tree from prep-trees.sh.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd -P)
WORK=${1:?usage: run-fable.sh <workdir> <commit> <rep>}
C=${2:?}
REP=${3:?}
d=$(cd "$WORK/$C" && pwd -P)
run="$d/fable-r$REP"
mkdir -p "$run"
sed "s|{DIFF}|$d/diff.patch|" "$HERE/reviewer-prompt.md" > "$run/prompt.md"
start=$(date +%s)
rc=0
( cd "$d/tree" && claude -p \
    --output-format json \
    --allowedTools "Read,Grep,Glob" \
    --disallowedTools "Bash,WebFetch,WebSearch,Task,Agent,Write,Edit,NotebookEdit" \
    < "$run/prompt.md" > "$run/out.json" 2> "$run/err.txt" ) || rc=$?
end=$(date +%s)
printf '{"wall_s": %d, "exit": %d}\n' "$((end-start))" "$rc" > "$run/wall.json"
echo "$C fable-r$REP exit=$rc wall=$((end-start))s out=$(wc -c < "$run/out.json" | tr -d ' ')B"
