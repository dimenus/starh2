#!/usr/bin/env bash
# Wait for the running sweep, add the Anthropic arm, judge everything, score.
# Sequential throughout: one cursor account, and wall time is a validity number.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
if [ -f "$HERE/sweep.pid" ]; then
  while kill -0 "$(cat "$HERE/sweep.pid")" 2>/dev/null; do sleep 20; done
fi
echo "=== stage 2: anthropic arm"
"$HERE/run-all-cursor.sh" fable-cursor=claude-fable-5-thinking-high
echo "=== stage 3: judging"
"$HERE/judge-all.sh"
echo "=== stage 4: score"
"$HERE/score.py"
echo "=== pipeline done rc=$?"
