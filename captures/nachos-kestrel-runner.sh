#!/bin/sh
# Nachos mixed + SSE-200 with the Kestrel arm overlaid on 461071b.
# Throwaway tree: /home/ryan/src/starh2-kestrel. Do not touch master/t844/base.
set -u
export PATH="$HOME/.local/bin:$PATH"
REPO=/home/ryan/src/starh2-kestrel
OUT=/home/ryan/src/starh2-kestrel-out
SHA_WANT=461071b

mkdir -p "$OUT/logs"
exec >"$OUT/run.log" 2>&1

fail=0
note() { echo; echo "======== $* ========"; date -Is; uptime; }

cd "$REPO" || exit 2
SHA=$(git rev-parse --short HEAD)
echo "host=$(uname -n)"
echo "sha=$SHA want=$SHA_WANT"
echo "zig=$($HOME/.zvm/bin/zig version)"
echo "dotnet=$(dotnet --version)"
echo "go=$(go version)"
uname -a
if [ "$SHA" != "$SHA_WANT" ]; then
  echo "SHA mismatch" >&2
  exit 2
fi
if [ "$(uname -n)" != nachos ]; then
  echo "not nachos" >&2
  exit 2
fi
test -f "$REPO/tools/sse_bench/kestrel/Program.cs"
test -f "$REPO/tools/sse_bench/kestrel_publish.sh"
test -x "$REPO/zb"
grep -q x86_64-linux-gnu "$REPO/zb"

pkill -f "starh2-bench-serve[r]" 2>/dev/null || true
pkill -f "sse-serve[r]" 2>/dev/null || true
pkill -f "sse-kestrel" 2>/dev/null || true
sleep 1

run_phase() {
  name=$1
  shift
  note "$name"
  log="$OUT/logs/$name.log"
  set +e
  "$@" >"$log" 2>&1
  rc=$?
  echo "$name rc=$rc" | tee -a "$OUT/summary.txt"
  echo "---- $name ----"
  cat "$log"
  if [ "$rc" -ne 0 ]; then
    fail=1
  fi
  return 0
}

run_phase mixed \
  env STREAMS=32 INTERVAL=10 SECONDS_RUN=5 WARMUP=1 ONESHOT_WORKERS=8 \
      STARH2_EXECUTORS=2 ROUNDS=3 STALL=1 OUT="$OUT/mixed" \
      "$REPO/tools/sse_bench/mixed.sh"

run_phase sse200 \
  env STREAMS=200 SECONDS_RUN=10 INTERVAL=10 ROUNDS=4 WARMUP=1 \
      STARH2_EXECUTORS=2 OUT="$OUT/sse" \
      "$REPO/tools/sse_bench/run.sh"

note "done"
echo "NACHOS-KESTREL-DONE fail=$fail sha=$SHA" | tee -a "$OUT/summary.txt"
exit 0
