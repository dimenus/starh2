#!/bin/sh
# Rebuild the patched zio this branch pins, and put it in the zig cache.
#
#   tools/zio-prefer-local/pack.sh
#
# The branch pins zio 0299e57 plus prefer-local.diff (a `.prefer_local`
# placement: start on the caller's executor, may migrate under
# work_stealing). It is a local experiment, never pushed anywhere, so the
# package is a tarball at a fixed path. build.zig.zon cannot name a
# relative tarball (zig rejects it as an invalid URI), so the path is
# absolute and this script writes exactly that file.
#
# zig resolves a dependency by hash from its cache first, so the tarball
# only has to exist the first time a machine builds this branch. If it is
# missing and not cached, the build fails loudly; it never falls back to
# stock zio.
#
# The package hash is over file contents, not tarball bytes, so any tar of
# the same tree gives the same hash. The script stops if it does not.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
ZIO=${ZIO:-$HOME/Source/mine/zio}
BASE=0299e5771b8c042ca42077af0748771fca27d38e
OUT=/tmp/starh2-zio-prefer-local.tar.gz
WANT=$(sed -n 's/.*\.hash = "\(zio-[^"]*\)".*/\1/p' "$HERE/../../build.zig.zon")
[ -n "$WANT" ] || { echo "no zio hash in build.zig.zon" >&2; exit 1; }
T=$(mktemp -d)
mkdir "$T/zio"
git -C "$ZIO" archive "$BASE" | tar -x -C "$T/zio"
(cd "$T/zio" && git apply "$HERE/prefer-local.diff")
# Both details change the package hash (measured): a tar of "." gives
# another hash than a tar of one top directory, and without COPYFILE_DISABLE
# macOS tar adds a ._zio AppleDouble entry, so zig no longer strips the top
# directory at all.
(cd "$T" && COPYFILE_DISABLE=1 tar -czf "$OUT" zio)
rm -rf "$T"
GOT=$(cd "$HERE/../.." && "${ZIG:-zig}" fetch "$OUT")
[ "$GOT" = "$WANT" ] || { echo "packed $GOT, build.zig.zon pins $WANT" >&2; exit 1; }
echo "$OUT -> $GOT"
