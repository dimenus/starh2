#!/bin/sh
# Official h2load shape (-c 50 -m 10) through the three-arm mixed client.
# STREAMS=0: oneshot-only. Auto executors vs pinned-2, same recipe.
set -u
export PATH="$HOME/.local/bin:$PATH"
REPO=/home/ryan/src/starh2-kestrel
OUT=/home/ryan/src/starh2-kestrel-out
SHA_WANT=461071b

mkdir -p "$OUT/logs"
exec >"$OUT/conns50.log" 2>&1

fail=0
note() { echo; echo "======== $* ========"; date -Is; uptime; nproc; }

cd "$REPO" || exit 2
SHA=$(git rev-parse --short HEAD)
echo "host=$(uname -n) sha=$SHA nproc=$(nproc)"
if [ "$SHA" != "$SHA_WANT" ] || [ "$(uname -n)" != nachos ]; then
  echo "host/sha mismatch" >&2
  exit 2
fi

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
  echo "$name rc=$rc" | tee -a "$OUT/conns50-summary.txt"
  echo "---- $name ----"
  cat "$log"
  if [ "$rc" -ne 0 ]; then
    fail=1
  fi
  return 0
}

# h2load -c 50 -m 10: 50 TLS connections, 10 in-flight oneshots each.
run_phase conns50-auto \
  env CONNS=50 ONESHOT_WORKERS=500 STREAMS=0 STALL=0 \
      SECONDS_RUN=5 WARMUP=1 ROUNDS=3 \
      STARH2_EXECUTORS=auto OUT="$OUT/conns50-auto" \
      "$REPO/tools/sse_bench/mixed.sh"

run_phase conns50-exec2 \
  env CONNS=50 ONESHOT_WORKERS=500 STREAMS=0 STALL=0 \
      SECONDS_RUN=5 WARMUP=1 ROUNDS=3 \
      STARH2_EXECUTORS=2 OUT="$OUT/conns50-exec2" \
      "$REPO/tools/sse_bench/mixed.sh"

note "done"
echo "NACHOS-CONNS50-DONE fail=$fail sha=$SHA nproc=$(nproc)" | tee -a "$OUT/conns50-summary.txt"
exit 0
