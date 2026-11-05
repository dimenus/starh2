#!/bin/bash
# Run a command and fail if it left core dumps (Linux, systemd-coredump).
#
#   tools/core-dump-check.sh [--allow REGEX] -- COMMAND...
#
# Counts this user's `coredumpctl` entries before and after COMMAND, lists
# the ones made while it ran, and exits 3 if any of them came from an
# executable that does not match REGEX. A test case that is meant to die calls
# `tests/no_core.zig` `expectDeath()` first, so `./zb build ci` should leave
# none (t-3882). `--allow zio-dst` excuses the zio fork's mutant gate, which
# aborts on purpose and is fixed in the fork (t-3872).
#
# It refuses to answer without coredumpctl: a check that cannot see the dumps
# must not read as a pass. COMMAND's own exit status is printed and returned
# when no dump is found.
set -uo pipefail
allow=""
if [ "${1:-}" = "--allow" ]; then
  allow=$2
  shift 2
fi
[ "${1:-}" = "--" ] && shift
[ $# -gt 0 ] || { echo "usage: core-dump-check.sh [--allow REGEX] -- COMMAND..." >&2; exit 2; }
command -v coredumpctl > /dev/null || { echo "core-dump-check: no coredumpctl here; cannot check" >&2; exit 2; }

since=$(date '+%Y-%m-%d %H:%M:%S')
before=$(coredumpctl list --no-pager --no-legend 2>/dev/null | wc -l)
"$@"
rc=$?
# systemd-coredump records a dump a moment after the process dies.
sleep 3
after=$(coredumpctl list --no-pager --no-legend 2>/dev/null | wc -l)
during=$(coredumpctl list --no-pager --no-legend --since="$since" 2>/dev/null | awk '{print $10}')
n_during=$(printf '%s\n' "$during" | grep -c . || true)
if [ -n "$allow" ]; then
  bad=$(printf '%s\n' "$during" | grep . | grep -Ev "$allow" || true)
else
  bad=$(printf '%s\n' "$during" | grep . || true)
fi
n_bad=$(printf '%s\n' "$bad" | grep -c . || true)
echo "core-dump-check: before $before, after $after; made during the command $n_during, of them not allowed ($allow) $n_bad; command exit $rc"
if [ "$n_during" -gt 0 ]; then
  printf '%s\n' "$during" | grep . | sed 's#.*/o/[0-9a-f]*/##' | sort | uniq -c | sed 's/^/core-dump-check:   /'
fi
[ "$n_bad" -eq 0 ] || exit 3
exit $rc
