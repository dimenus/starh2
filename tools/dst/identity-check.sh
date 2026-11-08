#!/bin/bash
# Check that the transport witness names the commit the tree is now, after
# changes that do not touch build.zig.
#
#   tools/dst/identity-check.sh [<rev>]     (default HEAD; run inside the repo)
#
# It clones <rev> with `git clone --shared` into a temporary directory (the
# clone borrows this repository's objects and never writes to it), then
# builds `dst-transport` there after each step and reads the control's
# `BUILD=commit=` line:
#
#   T0  a fresh build at <rev>                       want <rev>
#   T1  a new commit that changes README.md only     want the new commit
#   T2  README.md changed, not committed             want the new commit + -dirty
#   T3  README.md restored                           want the new commit
#   T4  a `git archive` export of that commit        want the new commit
#
# T1-T3 are the cases a configure-time `git rev-parse` gets wrong: Zig 0.17
# caches the configure result, and git's answer is not an input to it.
# T4 is the export path (tests/dst_commit.txt).
#
# Exit 0: every step named the expected commit. Exit 1: a step was stale.
# Exit 2: a build failed or ran past its limit, or the setup failed. Every
# build has a wall-clock limit. Run time is a few minutes: two cold builds
# of the witness and its dependencies.
set -euo pipefail

limit=900
repo=$(git rev-parse --show-toplevel)
rev=$(git -C "$repo" rev-parse --verify "${1:-HEAD}^{commit}")
tmp=$(mktemp -d "${TMPDIR:-/tmp}/dst-identity.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
tree=$tmp/tree
export_dir=$tmp/export

git clone -q --shared --no-checkout "$repo" "$tree"
git -C "$tree" checkout -q --detach "$rev"

# The BUILD=commit= value the control prints, after a build in directory $1.
# The cache is the clone's for both trees, so the export does not compile
# the dependencies a second time.
identity() {
  local dir=$1 log=$tmp/build.log
  if ! (cd "$dir" && perl -e 'alarm shift; exec @ARGV' "$limit" ./zb build dst-transport --cache-dir "$tree/.zig-cache") > "$log" 2>&1; then
    echo "identity-check: the build in $dir failed or ran past ${limit}s; its last lines:" >&2
    tail -n 30 "$log" >&2
    exit 2
  fi
  local got
  got=$(sed -n 's/^BUILD=commit=\([^ ]*\) mutant=none .*/\1/p' "$log" | head -n 1)
  if [ -z "$got" ]; then
    echo "identity-check: the build in $dir printed no control BUILD line" >&2
    exit 2
  fi
  printf '%s\n' "$got"
}

stale=0
step() {
  local name=$1 dir=$2 want=$3 got
  got=$(identity "$dir")
  if [ "$got" = "$want" ]; then
    echo "identity-check: $name: $got (ok)"
  else
    echo "identity-check: $name: STALE: want $want, got $got"
    stale=1
  fi
}

step "T0 fresh build at $rev" "$tree" "$rev"

echo "identity-check $$" >> "$tree/README.md"
git -C "$tree" -c user.name=identity-check -c user.email=identity-check@localhost \
  commit -q -am "identity-check: change README.md only"
new=$(git -C "$tree" rev-parse HEAD)
step "T1 new commit, build.zig unchanged" "$tree" "$new"

echo "identity-check dirty $$" >> "$tree/README.md"
step "T2 tracked file changed" "$tree" "$new-dirty"

git -C "$tree" checkout -q -- README.md
step "T3 tracked file restored" "$tree" "$new"

mkdir "$export_dir"
git -C "$tree" archive "$new" | tar -x -C "$export_dir"
step "T4 git archive export" "$export_dir" "$new"

[ "$stale" -eq 0 ] || exit 1
echo "identity-check: PASS"
