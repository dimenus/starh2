#!/bin/bash
# Gate: every .zig and .zon file of ours is in `zig fmt` form.
#
#   gate.sh <zig-exe> <repo-root>
#
# build.zig runs it as `./zb build fmt-gate`, and `ci` depends on it. It passes
# the build's own zig, so the gate formats with the version build.zig.zon pins.
#
# THE SET: every .zig and .zon file of ours, as tools/our-files.sh defines
# it (git's view in a checkout; a walk that skips zig-pkg, zig-out and dot
# directories in a `git archive` export).
#
# It prints which of the two sets it used. On a difference it prints each file
# that differs and exits 1. With an empty set it exits 2: an empty set would
# pass while checking nothing.
#
# To fix: ./zb fmt <file>...
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: gate.sh <zig-exe> <repo-root>" >&2
  exit 2
fi
zig=$1
here=$(cd "$(dirname "$0")/.." && pwd)
cd "$2"

mode=$(bash "$here/our-files.sh" --mode .)
if ! listed=$(bash "$here/our-files.sh" . '*.zig' '*.zon'); then
  echo "fmt-gate FATAL: tools/our-files.sh failed" >&2
  exit 2
fi
files=()
while IFS= read -r f; do
  [ -n "$f" ] || continue
  files+=("$f")
done <<EOF
$listed
EOF

if [ ${#files[@]} -eq 0 ]; then
  echo "fmt-gate FATAL: no .zig or .zon files found; refusing to pass on an empty set" >&2
  exit 2
fi

# zig fmt --check prints each file that differs on stdout, and exits 1.
set +e
differ=$("$zig" fmt --check "${files[@]}")
status=$?
set -e
if [ $status -eq 0 ]; then
  echo "fmt-gate: ${#files[@]} files checked, all in zig fmt form; set: $mode"
  exit 0
fi
if [ -z "$differ" ]; then
  echo "fmt-gate FATAL: zig fmt --check exited $status and named no file" >&2
  exit 2
fi
echo "fmt-gate: ${#files[@]} files checked (set: $mode); these are not in zig fmt form:" >&2
printf '%s\n' "$differ" | sed 's/^/  /' >&2
echo "fix: ./zb fmt $(printf '%s\n' "$differ" | tr '\n' ' ')" >&2
exit 1
