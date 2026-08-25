#!/bin/sh
# 50-conn oneshot with physical-core auto (12 on nachos, not 24 SMT).
set -u
export PATH="$HOME/.local/bin:$PATH"
REPO=/home/ryan/src/starh2-kestrel
OUT=/home/ryan/src/starh2-kestrel-out
SHA_WANT=461071b

mkdir -p "$OUT/logs"
exec >"$OUT/conns50-phys12.log" 2>&1

cd "$REPO" || exit 2
SHA=$(git rev-parse --short HEAD)
echo "host=$(uname -n) sha=$SHA nproc=$(nproc)"
test -f "$REPO/src/physical_cpus.zig" || { echo missing physical_cpus.zig >&2; exit 2; }
grep -q physical_cpus "$REPO/examples/bench_server.zig" || { echo bench_server not wired >&2; exit 2; }
if [ "$SHA" != "$SHA_WANT" ] || [ "$(uname -n)" != nachos ]; then
  echo "host/sha mismatch" >&2
  exit 2
fi

pkill -f "starh2-bench-serve[r]" 2>/dev/null || true
pkill -f "sse-serve[r]" 2>/dev/null || true
pkill -f "sse-kestrel" 2>/dev/null || true
sleep 1

note() { echo; echo "======== $* ========"; date -Is; uptime; }

note "conns50-phys12"
log="$OUT/logs/conns50-phys12.log"
set +e
CONNS=50 ONESHOT_WORKERS=500 STREAMS=0 STALL=0 \
  SECONDS_RUN=5 WARMUP=1 ROUNDS=3 \
  STARH2_EXECUTORS=auto OUT="$OUT/conns50-phys12" \
  "$REPO/tools/sse_bench/mixed.sh" >"$log" 2>&1
rc=$?
set -e
echo "conns50-phys12 rc=$rc" | tee -a "$OUT/conns50-phys12-summary.txt"
echo "---- ready line (executors) ----"
grep -E 'ready|executors' "$OUT/conns50-phys12/starh2.log" | head -5 || true
echo "---- conns50-phys12 ----"
cat "$log"
echo "NACHOS-CONNS50-PHYS12-DONE rc=$rc sha=$SHA" | tee -a "$OUT/conns50-phys12-summary.txt"
exit 0
