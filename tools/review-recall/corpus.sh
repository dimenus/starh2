#!/usr/bin/env bash
# Build the isolated review corpus for the review-recall harness.
#
# PROTOCOL
# For each commit under review this writes corpus/<commit>/ holding:
#   tree/     the full source tree at that commit, extracted with `git archive`
#   diff.patch  the diff of that commit against its first parent
#   subject.txt the commit subject and body
#
# `git archive` is the isolation mechanism, and it is the reason this does NOT
# use `git worktree`. A worktree shares the .git directory, so an arm can reach
# the merged fixes, the later commits and the upstream refs with one `git log`.
# An extracted tree has no .git at all: there is no history to reach, no remote
# to fetch, and `git` itself reports "not a repository" inside it.
#
# THE TRAP THIS GUARDS
# An arm that reads the answer scores 100% and proves nothing. So this script
# refuses to publish a corpus directory that fails any of these:
#   1. no .git anywhere in the extracted tree
#   2. no later commit's abbreviated sha appears anywhere in the tree
#   3. no "coderabbit" or PR-thread marker appears anywhere in the tree
# A failure aborts and leaves the corpus incomplete on purpose. A partial
# corpus is loud; a leaky corpus is silent.
#
# Coverage is printed. Extracting zero commits exits non-zero.
set -euo pipefail

ZIO="${ZIO_REPO:-$HOME/Source/oss/zio}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
OUT="${CORPUS_DIR:-$HERE/corpus}"

# The commits CodeRabbit reviewed, in push order. Keep in step with findings.json.
COMMITS=(f1681a4d e04e6e93 930921f4 bd7b9ed6 646a0b4c ea050293 16df720e 0c9271ce)

[ -d "$ZIO/.git" ] || { echo "FATAL: no git repo at $ZIO" >&2; exit 2; }

# Every commit named in findings.json must be in COMMITS, or the corpus silently
# stops covering a finding that the scorer still counts against recall.
missing="$(python3 - "$HERE/findings.json" "${COMMITS[@]}" <<'PY'
import json,sys
want={f["commit"] for f in json.load(open(sys.argv[1]))["findings"]}
print(" ".join(sorted(want - set(sys.argv[2:]))))
PY
)"
[ -z "$missing" ] || { echo "FATAL: findings.json names commits the corpus does not build: $missing" >&2; exit 2; }

rm -rf "$OUT"
mkdir -p "$OUT"

built=0
for c in "${COMMITS[@]}"; do
  full="$(git -C "$ZIO" rev-parse "$c^{commit}")"
  d="$OUT/$c"
  mkdir -p "$d/tree"
  git -C "$ZIO" archive --format=tar "$full" | tar -x -C "$d/tree"
  git -C "$ZIO" diff "$full^" "$full" > "$d/diff.patch"
  git -C "$ZIO" log -1 --format='%s%n%n%b' "$full" > "$d/subject.txt"
  echo "$full" > "$d/sha.txt"

  # Strip the review-tool config. It carries no finding, but it names the vendor
  # and is a search lead for an arm with web access. The strip is recorded so a
  # reader knows the arm saw a tree the reviewed commit did not have exactly.
  : > "$d/stripped.txt"
  for junk in .coderabbit.yaml; do
    if [ -e "$d/tree/$junk" ]; then
      rm -rf "$d/tree/$junk"
      echo "$junk" >> "$d/stripped.txt"
    fi
  done

  # ---- isolation assertions -------------------------------------------------
  if find "$d/tree" -name .git -print -quit | grep -q .; then
    echo "FATAL $c: a .git exists inside the extracted tree" >&2; exit 3
  fi
  # Any commit later in the push order is an answer key. So is anything on
  # the current branch of the source repo.
  later="$(git -C "$ZIO" rev-list --abbrev-commit --abbrev=8 "$full"..HEAD 2>/dev/null || true)"
  for s in $later; do
    if grep -rqI -- "$s" "$d/tree" "$d/diff.patch" 2>/dev/null; then
      echo "FATAL $c: the corpus mentions later commit $s" >&2; exit 3
    fi
  done
  for marker in coderabbit cr-comment 'pull request' 'pull/71'; do
    if grep -rqiI -- "$marker" "$d/tree" "$d/diff.patch" 2>/dev/null; then
      echo "FATAL $c: the corpus mentions '$marker'" >&2; exit 3
    fi
  done

  files="$(find "$d/tree" -type f | wc -l | tr -d ' ')"
  hunks="$(grep -c '^@@' "$d/diff.patch" || true)"
  echo "built $c  files=$files  diff_hunks=$hunks  subject=$(head -1 "$d/subject.txt")"
  built=$((built + 1))
done

# Fail on zero: a corpus that covered nothing must not read as a clean build.
[ "$built" -gt 0 ] || { echo "FATAL: built 0 commits" >&2; exit 4; }
echo "corpus ok: $built commits at $OUT"
