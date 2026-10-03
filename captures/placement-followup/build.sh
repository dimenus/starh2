#!/bin/sh
# Build every binary of the placement follow-up session from ONE clean tree.
#
#   WORK=<dir> TARGET=x86_64-linux-musl captures/placement-followup/build.sh
#   WORK=<dir> TARGET=native            captures/placement-followup/build.sh
#
# Two binaries per target: -Dzio-scheduling=work_stealing and =pinned. Every
# arm is one of the two plus run arguments, so all arms share one starh2
# commit and one zio package (0299e57 + prefer-local.diff).
# Prints the commit, the embedded zio package and each sha256.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}
TARGET=${TARGET:?TARGET is required}
BSSL=${BSSL:-$HOME/Source/oss/http2-zig-hendrik/boringssl}
cd "$REPO"
[ -z "$(git status --porcelain -- src examples build.zig build.zig.zon)" ] || { echo "source tree is dirty; commit first" >&2; exit 1; }
echo "commit $(git rev-parse HEAD)"
tflag="-Dtarget=$TARGET"; [ "$TARGET" = native ] && tflag=""
for s in work_stealing pinned; do
  out="$WORK/$TARGET-$s"
  rm -rf "$out"
  ./zb build starh2-bench-server -Doptimize=ReleaseFast -Dzio-scheduling=$s $tflag \
    -Dboringssl-source-path="$BSSL" --prefix "$out" > /dev/null
  b="$out/bin/starh2-bench-server"
  z=$(strings -a "$b" | grep -oE 'zio-[0-9]+\.[0-9]+\.[0-9]+-[A-Za-z0-9_-]{20,}' | sort -u | tr '\n' ' ')
  echo "$TARGET $s zio=$z sha256=$(shasum -a 256 "$b" | cut -d' ' -f1)"
done
