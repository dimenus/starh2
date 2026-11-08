#!/bin/bash
# The repo's own files: the set every whole-tree gate checks.
#
#   tools/our-files.sh <root> <basename-glob>...   paths, one per line, relative to <root>
#   tools/our-files.sh --mode <root>               which of the two sets applies, one line
#
# A glob matches a file's basename at any depth ('*.zig', 'build.zig.zon').
#
# In a git checkout (the root is the top of a work tree): every matching file
# that git tracks, plus every untracked one that .gitignore does not exclude,
# so a new file counts before its first commit. .gitignore is the one place
# that says what is not ours: zig-pkg/ (vendored dependencies), the build
# caches, the install prefixes, captures and the review-recall corpus are all
# excluded there, at any depth. A file deleted from the working tree but still
# in the index is left out.
#
# Anywhere else (a `git archive` export, or a tree nested inside some other
# repository, whose index is not ours): every matching file under the root,
# skipping directories named zig-pkg or zig-out and directories whose names
# start with a dot, at any depth. These are the only places a build puts files
# into a fresh export, the root's and the tools/* build roots' alike
# (std-io-gate also skips zig-pkg and zig-out by name). A gitignored directory
# that a build does not create is not in an export, so it needs no rule here;
# in a copied checkout without .git it is included, and a gate fails loudly.
#
# Exits 2 if git or find fails. An empty list is not an error here; each gate
# refuses an empty set itself.
set -euo pipefail

in_git_root() {
  local cdup
  cdup=$(git rev-parse --show-cdup 2>/dev/null) && [ -z "$cdup" ]
}

if [ "${1:-}" = "--mode" ]; then
  [ $# -eq 2 ] || { echo "usage: our-files.sh --mode <root>" >&2; exit 2; }
  cd "$2"
  if in_git_root; then
    echo "git (tracked, plus untracked and not ignored)"
  else
    echo "no git work tree at the root (skipping zig-pkg, zig-out and dot directories)"
  fi
  exit 0
fi

[ $# -ge 2 ] || { echo "usage: our-files.sh <root> <basename-glob>..." >&2; exit 2; }
cd "$1"
shift

if in_git_root; then
  specs=()
  for g in "$@"; do specs+=(":(glob)**/$g"); done
  if ! listed=$(git ls-files --cached --others --exclude-standard -- "${specs[@]}"); then
    echo "our-files FATAL: git ls-files failed" >&2
    exit 2
  fi
else
  names=()
  for g in "$@"; do
    [ ${#names[@]} -eq 0 ] || names+=(-o)
    names+=(-name "$g")
  done
  if ! listed=$(find . -mindepth 1 \( -type d \( -name zig-pkg -o -name zig-out -o -name '.*' \) -prune \) \
      -o -type f \( "${names[@]}" \) -print | sed 's#^\./##'); then
    echo "our-files FATAL: find failed" >&2
    exit 2
  fi
fi

# --cached and --others can list one path twice only for an unmerged file.
printf '%s\n' "$listed" | sort -u | while IFS= read -r f; do
  if [ -n "$f" ] && [ -e "$f" ]; then printf '%s\n' "$f"; fi
done
