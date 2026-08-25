#!/bin/sh
# Nachos: perf-arms (starh2 vs hyper, conns50 shape) then TRACE=1 mixed.sh
# for the starh2 counters. Tree: /home/ryan/src/starh2-hyper.
set -u
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
REPO=/home/ryan/src/starh2-hyper
OUT=/home/ryan/src/starh2-perf-out
mkdir -p "$OUT/logs"
exec >"$OUT/run.log" 2>&1
fail=0
note() { echo; echo "======== $* ========"; date -Is; uptime; }
cd "$REPO" || exit 2
echo "host=$(uname -n) sha=$(git rev-parse --short HEAD)"
[ "$(uname -n)" = nachos ] || exit 2
pkill -f "starh2-bench-serve[r]" 2>/dev/null || true
pkill -f "sse-serve[r]" 2>/dev/null || true
pkill -f "sse-kestre[l]" 2>/dev/null || true
pkill -f "sse-hype[r]" 2>/dev/null || true
sleep 1
run_phase() {
  name=$1; shift
  note "$name"
  log="$OUT/logs/$name.log"
  set +e
  "$@" >"$log" 2>&1
  rc=$?
  echo "$name rc=$rc" | tee -a "$OUT/summary.txt"
  echo "---- $name ----"; cat "$log"
  [ "$rc" -ne 0 ] && fail=1
  return 0
}
run_phase perf-arms env STARH2_EXECUTORS=auto OUT="$OUT/perf" "$REPO/tools/sse_bench/perf-arms.sh"
run_phase perf-arms-w2 env STARH2_EXECUTORS=2 OUT="$OUT/perf-w2" "$REPO/tools/sse_bench/perf-arms.sh"
run_phase trace-mixed env TRACE=1 STREAMS=32 INTERVAL=10 SECONDS_RUN=5 WARMUP=1 ONESHOT_WORKERS=8 \
  STARH2_EXECUTORS=2 ROUNDS=1 STALL=1 OUT="$OUT/trace-mixed" "$REPO/tools/sse_bench/mixed.sh"
note done
echo "NACHOS-PERF-DONE fail=$fail" | tee -a "$OUT/summary.txt"
