#!/bin/sh
# Build and benchmark hendriknielaender/http2.zig as an attributable opponent.
set -eu

REPO=$(cd "$(dirname "$0")/.." && pwd -P)
DEFAULT_ROOT="$REPO/../../oss/http2-zig-hendrik"
HENDRIK_ROOT=${HENDRIK_ROOT:-$DEFAULT_ROOT}
OUT=${OUT:-/tmp/starh2-hendrik-bench}

if [ ! -f "$HENDRIK_ROOT/build.zig" ]; then
  echo "http2.zig checkout not found at $HENDRIK_ROOT" >&2
  echo "set HENDRIK_ROOT=/path/to/http2.zig" >&2
  exit 1
fi
HENDRIK_ROOT=$(cd "$HENDRIK_ROOT" && pwd -P)

if [ ! -f "$HENDRIK_ROOT/boringssl/CMakeLists.txt" ]; then
  echo "http2.zig's BoringSSL submodule is missing" >&2
  echo "run: git -C \"$HENDRIK_ROOT\" submodule update --init boringssl" >&2
  exit 1
fi

revision=$(git -C "$HENDRIK_ROOT" rev-parse HEAD)
if [ -n "$(git -C "$HENDRIK_ROOT" status --porcelain)" ]; then
  revision="${revision}-dirty"
fi

mkdir -p "$OUT"
(
  cd "$HENDRIK_ROOT"
  "$REPO/zb" build -Doptimize=ReleaseFast --prefix "$OUT/http2-zig"
)

opponent="$OUT/http2-zig/bin/benchmark"
if [ ! -x "$opponent" ]; then
  echo "http2.zig build did not produce $opponent" >&2
  exit 1
fi

exec "$REPO/zb" build bench -Doptimize=ReleaseFast -- \
  --opponent "$opponent" \
  --opponent-name "http2.zig tls" \
  --opponent-revision "$revision" \
  --opponent-cwd "$HENDRIK_ROOT" \
  "$@"
