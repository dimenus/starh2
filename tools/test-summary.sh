#!/usr/bin/env bash
# Run one `./zb build` step (default: test) with its whole output in a log
# file, and print only the result: the exit code, the build summary, the
# first lines of each failing test, and any test binary that did not run.
# Meant for agents, whose context pays for every log line they read.
#
#   tools/test-summary.sh                 # ./zb build test
#   tools/test-summary.sh test-t3562 -Doptimize=ReleaseFast
#
# The build runner caches a test run that passed, and a cached run looks like
# a pass. Each one is listed, and the exit code is 3 when any was cached:
# change an input, or run the test binary itself.
set -euo pipefail
cd "$(dirname "$0")/.."
step=${1:-test}
if [ $# -gt 0 ]; then shift; fi
dir=${TEST_LOG_DIR:-.zig-cache/test-logs}
mkdir -p "$dir"
log="$dir/$step-$(date +%Y%m%d-%H%M%S).log"
rc=0
./zb build "$step" --summary all "$@" >"$log" 2>&1 || rc=$?
echo "step=$step rc=$rc log=$log"
grep -m1 "Build Summary" "$log" || echo "no Build Summary line"
if grep -q "^error: '" "$log"; then
	grep -A6 "^error: '" "$log" | grep -v "^--$" | head -80
fi
cached=$(grep -E "^[ |+-]*run test .* cached" "$log" || true)
if [ -n "$cached" ]; then
	echo "NOT RUN (cached):"
	echo "$cached" | sed 's/^[ |+-]*/  /'
	if [ "$rc" -eq 0 ]; then exit 3; fi
fi
exit "$rc"
