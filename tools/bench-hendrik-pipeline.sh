#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HENDRIK_ROOT="${HENDRIK_ROOT:-"$ROOT/../../oss/http2-zig-hendrik"}"

if [[ ! -d "$HENDRIK_ROOT/.git" ]]; then
  echo "bench-hendrik-pipeline: checkout not found at $HENDRIK_ROOT" >&2
  echo "set HENDRIK_ROOT=/path/to/http2.zig" >&2
  exit 1
fi

REVISION="$(git -C "$HENDRIK_ROOT" rev-parse HEAD)"
OUT="${TMPDIR:-/tmp}/starh2-hendrik-pipeline-${REVISION}"

echo "bench-hendrik-pipeline: http2.zig revision $REVISION"
"$ROOT/zb" build-exe \
  -OReleaseFast \
  -lc \
  --dep http2 \
  "-Mroot=$ROOT/tools/hendrik_pipeline_bench.zig" \
  "-Mhttp2=$HENDRIK_ROOT/src/http2.zig" \
  "-femit-bin=$OUT"

exec "$OUT" "$@"
