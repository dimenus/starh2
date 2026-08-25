#!/bin/sh
# Nachos mixed + SSE-200 + conns50 oneshot with the hyper (Rust) arm added.
# Tree: /home/ryan/src/starh2-hyper (rsync of the laptop working tree, so
# the SHA is the base and the diff is uncommitted work). Do not touch
# master/t844/base/kestrel trees.
set -u
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
REPO=/home/ryan/src/starh2-hyper
OUT=/home/ryan/src/starh2-hyper-out

mkdir -p "$OUT/logs"
exec >"$OUT/run.log" 2>&1

fail=0
note() { echo; echo "======== $* ========"; date -Is; uptime; }

cd "$REPO" || exit 2
SHA=$(git rev-parse --short HEAD)
echo "host=$(uname -n)"
echo "sha=$SHA (+ uncommitted diff: $(git status --short | grep -vc '^??') modified files)"
echo "zig=$($HOME/.zvm/bin/zig version)"
echo "dotnet=$(dotnet --version)"
echo "go=$(go version)"
echo "cargo=$(cargo --version)"
uname -a
nproc
if [ "$(uname -n)" != nachos ]; then
  echo "not nachos" >&2
  exit 2
fi
test -f "$REPO/tools/sse_bench/hyper/Cargo.lock"
test -f "$REPO/tools/sse_bench/hyper_build.sh"
test -x "$REPO/zb"
grep -q x86_64-linux-gnu "$REPO/zb"

pkill -f "starh2-bench-serve[r]" 2>/dev/null || true
pkill -f "sse-serve[r]" 2>/dev/null || true
pkill -f "sse-kestre[l]" 2>/dev/null || true
pkill -f "sse-hype[r]" 2>/dev/null || true
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

run_phase conns50 \
  env CONNS=50 ONESHOT_WORKERS=500 STREAMS=0 SECONDS_RUN=5 WARMUP=1 \
      STARH2_EXECUTORS=auto ROUNDS=3 OUT="$OUT/conns50" \
      "$REPO/tools/sse_bench/mixed.sh"

note "done"
echo "NACHOS-HYPER-DONE fail=$fail sha=$SHA" | tee -a "$OUT/summary.txt"
exit 0
