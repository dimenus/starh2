#!/bin/sh
# The commit this tree is, as the Zig source of the `dst_options` module.
#
#   tools/dst/identity.sh        (cwd: the build root)
#
# Prints `pub const git_commit = "<identity>";` on stdout. build.zig runs it
# as a Run step with side effects, so it runs on every build that needs the
# transport witness, and the witness's seeds and `BUILD=commit=` line follow
# the tree as it is now. Running git in build.zig itself is the wrong time:
# Zig 0.17 caches the configure result and git's answer is not an input to
# that cache, so the identity stayed at whatever commit configured last.
#
# The identity, first match wins:
#   1. tests/dst_commit.txt holds a commit: a `git archive` export, which
#      writes it there (.gitattributes `export-subst`).
#   2. The build root is the top of a git work tree: `git rev-parse HEAD`,
#      with `-dirty` appended when a tracked file differs from HEAD.
#      A tree nested inside some other repository is not ours to ask.
#   3. Otherwise the empty string; the witness then exits 2, "no commit
#      identity".
set -u

id=""
substituted=$(tr -d ' \r\n\t' < tests/dst_commit.txt)
case $substituted in
  *[!0-9a-f]*) ;;
  *) [ ${#substituted} -eq 40 ] && id=$substituted ;;
esac
if [ -z "$id" ] && cdup=$(git rev-parse --show-cdup 2>/dev/null) && [ -z "$cdup" ]; then
  if head=$(git rev-parse HEAD 2>/dev/null); then
    id=$head
    git diff --quiet HEAD -- 2>/dev/null || id="$head-dirty"
  fi
fi
printf 'pub const git_commit = "%s";\n' "$id"
