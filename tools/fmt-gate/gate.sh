#!/bin/bash
# Gate: every .zig and .zon file of ours is in `zig fmt` form.
#
#   gate.sh <zig-exe> <repo-root>
#
# build.zig runs it as `./zb build fmt-gate`, and `ci` depends on it. It passes
# the build's own zig, so the gate formats with the version build.zig.zon pins.
#
# THE SET, in a git checkout (the root is the top of a work tree): every .zig
# and .zon file that git tracks, plus every untracked one that .gitignore does
# not exclude, so a new file is checked before its first commit. .gitignore is
# the one place that says what is not ours: zig-pkg/ (vendored dependencies),
# the build caches, the install prefixes and the review-recall corpus are all
# excluded there, at any depth. A file deleted from the working tree but still
# in the index is skipped.
#
# THE SET, anywhere else (a `git archive` export, or a tree nested inside some
# other repository, whose index is not ours): every .zig and .zon file under
# the root, skipping directories named zig-pkg or zig-out and directories
# whose names start with a dot, at any depth. These are the only places a
# build puts files into a fresh export, the root's and the tools/* build
# roots' alike (std-io-gate also skips zig-pkg and zig-out by name). A
# gitignored directory that a build does not create is not in an export, so
# it needs no rule here; in a copied checkout without .git it is checked, and
# fails loudly.
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
cd "$2"

if cdup=$(git rev-parse --show-cdup 2>/dev/null) && [ -z "$cdup" ]; then
  mode="git (tracked, plus untracked and not ignored)"
  if ! listed=$(git ls-files --cached --others --exclude-standard -- '*.zig' '*.zon'); then
    echo "fmt-gate FATAL: git ls-files failed" >&2
    exit 2
  fi
else
  mode="no git work tree at the root (skipping zig-pkg, zig-out and dot directories)"
  if ! listed=$(find . -mindepth 1 \( -type d \( -name zig-pkg -o -name zig-out -o -name '.*' \) -prune \) \
      -o -type f \( -name '*.zig' -o -name '*.zon' \) -print | sed 's#^\./##'); then
    echo "fmt-gate FATAL: find failed" >&2
    exit 2
  fi
fi

# --cached and --others can list one path twice only for an unmerged file;
# sort -u keeps the count honest.
files=()
while IFS= read -r f; do
  [ -n "$f" ] || continue
  [ -e "$f" ] || continue
  files+=("$f")
done <<EOF
$(printf '%s\n' "$listed" | sort -u)
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
