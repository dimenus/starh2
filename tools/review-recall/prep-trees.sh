#!/usr/bin/env bash
# Extract each commit under review as a bare tree plus the diff against its
# first parent. `git archive` emits tracked files only: no .git, no refs, no
# remote. That is the blinding — the reviewer cannot enumerate later commits,
# cannot see the merged fixes, and has no remote URL to fetch from.
set -euo pipefail
ZIO=${ZIO:-$HOME/Source/oss/zio}
OUT=${1:?usage: prep-trees.sh <workdir>}
COMMITS="f1681a4d e04e6e93 930921f4 bd7b9ed6 646a0b4c ea050293 16df720e 0c9271ce"
mkdir -p "$OUT"
for c in $COMMITS; do
  d="$OUT/$c"
  rm -rf "$d/tree"
  mkdir -p "$d/tree"
  git -C "$ZIO" archive "$c" | tar -x -C "$d/tree"
  git -C "$ZIO" diff "$c^" "$c" > "$d/diff.patch"
  files=$(find "$d/tree" -type f | wc -l | tr -d ' ')
  bytes=$(wc -c < "$d/diff.patch" | tr -d ' ')
  if [ "$files" -eq 0 ] || [ "$bytes" -eq 0 ]; then
    echo "FAIL: empty tree or empty diff for $c" >&2
    exit 1
  fi
  if [ -e "$d/tree/.git" ]; then
    echo "FAIL: $c tree carries .git — blinding broken" >&2
    exit 1
  fi
  echo "$c tree_files=$files diff_bytes=$bytes"
done
