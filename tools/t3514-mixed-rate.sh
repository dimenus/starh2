#!/bin/sh
# Measure how often the mixed complete+task shape fails requests (t-3514).
#
# The shape: one h2load run over /fragment?kb=4 (complete handler) and
# /fragment-task?kb=64 (task handler) on the same connections. On TLS it
# fails thousands of requests in some runs and none in the rest, so one
# green run proves nothing. This script runs it RUNS times, one fresh server
# per run, and prints one line per run plus a total.
#
# A run counts only when it did work: h2load must print a "requests:" line
# with started > 0. A run that did no work is INVALID and the script exits 2,
# because a probe that measured nothing must not read as clean.
#
# Usage (absolute paths; macOS /bin/sh is fine, no bash arrays):
#   MODE=tls RUNS=20 tools/t3514-mixed-rate.sh
#   MODE=h2c RUNS=20 tools/t3514-mixed-rate.sh
#   BIN=/abs/starh2-bench-server  skip the build and use this server
#   H2LOAD_EXTRA='--log-file=...' extra h2load flags (splitting intended)
#   SERVER_ARGS='...'             extra bench-server flags (splitting intended)
#   SHAPE=complete                only /fragment?kb=COMPLETE_KB, the
#                                 throughput control beside the mixed shape
#
# Exit: 0 no run failed a request, 1 some run failed, 2 harness error.
set -eu

REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
. "$REPO/tools/bench_lock.sh"

MODE=${MODE:-tls}
RUNS=${RUNS:-20}
REQUESTS=${REQUESTS:-250000}
CLIENTS=${CLIENTS:-64}
MAXCONC=${MAXCONC:-16}
THREADS=${THREADS:-8}
EXECUTORS=${EXECUTORS:-8}
COMPLETE_KB=${COMPLETE_KB:-4}
TASK_KB=${TASK_KB:-64}
SHAPE=${SHAPE:-mixed}
WATCHDOG=${WATCHDOG:-120}
OUT=${OUT:-/tmp/starh2-t3514-$MODE}

case "$SHAPE" in
  mixed | complete) ;;
  *) echo "t3514-mixed-rate: SHAPE must be mixed or complete, got $SHAPE" >&2; exit 2 ;;
esac

case "$MODE" in
  tls) SCHEME=https ;;
  h2c) SCHEME=http ;;
  *) echo "t3514-mixed-rate: MODE must be tls or h2c, got $MODE" >&2; exit 2 ;;
esac

command -v h2load >/dev/null 2>&1 || { echo "t3514-mixed-rate: h2load is required" >&2; exit 2; }
mkdir -p "$OUT"

if [ -n "${BIN:-}" ]; then
  STARH2=$BIN
else
  (cd "$REPO" && ./zb build starh2-bench-server -Doptimize=fast --prefix "$OUT/starh2") || exit 2
  STARH2="$OUT/starh2/bin/starh2-bench-server"
fi
[ -x "$STARH2" ] || { echo "t3514-mixed-rate: no server at $STARH2" >&2; exit 2; }

S_PID=
H_PID=
cleanup() {
  if [ -n "$H_PID" ]; then kill "$H_PID" 2>/dev/null || true; wait "$H_PID" 2>/dev/null || true; H_PID=; fi
  if [ -n "$S_PID" ]; then kill "$S_PID" 2>/dev/null || true; wait "$S_PID" 2>/dev/null || true; S_PID=; fi
  bench_unlock
}
trap cleanup EXIT INT TERM

echo "t3514-mixed-rate: sha=$(git -C "$REPO" rev-parse --short HEAD) mode=$MODE shape=$SHAPE runs=$RUNS n=$REQUESTS c=$CLIENTS m=$MAXCONC t=$THREADS executors=$EXECUTORS complete_kb=$COMPLETE_KB task_kb=$TASK_KB msl=$(sysctl -n net.inet.tcp.msl 2>/dev/null || echo ?) server=$STARH2"

failed_runs=0
valid=0
run=1
while [ "$run" -le "$RUNS" ]; do
  bench_lock
  SLOG="$OUT/server-$run.log"
  HLOG="$OUT/h2load-$run.txt"
  : >"$SLOG"
  if [ "$MODE" = tls ]; then
    "$STARH2" --mode tls --port 0 --executors "$EXECUTORS" \
      --cert "$REPO/testdata/cert.pem" --key "$REPO/testdata/key.pem" \
      ${SERVER_ARGS:-} >"$SLOG" 2>&1 &
  else
    "$STARH2" --mode h2c --port 0 --executors "$EXECUTORS" ${SERVER_ARGS:-} >"$SLOG" 2>&1 &
  fi
  S_PID=$!

  PORT=
  i=0
  while [ "$i" -lt 100 ]; do
    if grep -q '"ready":true' "$SLOG" 2>/dev/null; then
      PORT=$(sed -n 's/.*"port":\([0-9][0-9]*\).*/\1/p' "$SLOG" | head -1)
      [ -n "$PORT" ] && break
    fi
    kill -0 "$S_PID" 2>/dev/null || { echo "t3514-mixed-rate: server died at bind (run $run)" >&2; cat "$SLOG" >&2; exit 2; }
    i=$((i + 1))
    sleep 0.1
  done
  [ -n "$PORT" ] || { echo "t3514-mixed-rate: no ready line (run $run)" >&2; exit 2; }

  B="$SCHEME://127.0.0.1:$PORT"
  TASK_URL="$B/fragment-task?kb=$TASK_KB"
  [ "$SHAPE" = complete ] && TASK_URL=
  h2load -t "$THREADS" -c "$CLIENTS" -m "$MAXCONC" -n "$REQUESTS" ${H2LOAD_EXTRA:-} \
    "$B/fragment?kb=$COMPLETE_KB" $TASK_URL >"$HLOG" 2>&1 &
  H_PID=$!
  waited=0
  while kill -0 "$H_PID" 2>/dev/null; do
    [ "$waited" -ge "$WATCHDOG" ] && break
    sleep 1
    waited=$((waited + 1))
  done
  verdict=
  if kill -0 "$H_PID" 2>/dev/null; then
    verdict=WEDGE
    kill "$H_PID" 2>/dev/null || true
  fi
  wait "$H_PID" 2>/dev/null || true
  H_PID=
  kill "$S_PID" 2>/dev/null || true
  wait "$S_PID" 2>/dev/null || true
  S_PID=
  bench_unlock

  # requests: 250000 total, 250000 started, 250000 done, 250000 succeeded, 0 failed, 0 errored, 0 timeout
  REQ=$(grep '^requests:' "$HLOG" | head -1 || true)
  started=$(echo "$REQ" | sed -n 's/.* \([0-9][0-9]*\) started.*/\1/p')
  failed=$(echo "$REQ" | sed -n 's/.* \([0-9][0-9]*\) failed.*/\1/p')
  errored=$(echo "$REQ" | sed -n 's/.* \([0-9][0-9]*\) errored.*/\1/p')
  if [ -z "$started" ] || [ "$started" -eq 0 ]; then
    echo "run $run INVALID: h2load did no work (see $HLOG)"
    cat "$HLOG" >&2
    exit 2
  fi
  valid=$((valid + 1))
  if [ -n "$verdict" ]; then
    failed_runs=$((failed_runs + 1))
  elif [ "$failed" -gt 0 ]; then
    verdict=FAIL
    failed_runs=$((failed_runs + 1))
  else
    verdict=PASS
  fi
  rps=$(sed -n 's/^finished in [^,]*, \([0-9.]*\) req\/s.*/\1/p' "$HLOG")
  echo "run $run $verdict started=$started failed=$failed errored=$errored secs=$waited req_s=$rps log=$HLOG"
  run=$((run + 1))
done

echo "t3514-mixed-rate: mode=$MODE failing_runs=$failed_runs/$valid"
[ "$failed_runs" -eq 0 ] || exit 1
