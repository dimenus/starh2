#!/bin/sh
# Measure the per-run wedge probability of the h2load specimen (t-866).
#
# The second wedge specimen (MEMBIO_WEDGE_WORKLOG.md, 2026-08-19 EOD) is
# plain `h2load -n 400000 -c 50 -m 10 -t 4` against the TLS bench server:
# one of the 50 connections parks near its end and h2load never exits.
# The per-run probability is low, so a single green run proves nothing.
# This script runs the shape ROUNDS times under a watchdog and reports
# wedges/rounds. It exits non-zero when any round wedged, and also when
# zero rounds ran (a probe that measures nothing must not read as clean).
#
# Per round:
#   PASS  - h2load exits within WATCHDOG seconds.
#   WEDGE - h2load is still running after WATCHDOG seconds. Evidence
#           (server sample, thread states, partial h2load output, server
#           log) is saved under $OUT/wedge-<round>/ before the kill.
#
# The watchdog classifies; it does not fix. WATCHDOG=60 is ~20x the
# healthy duration of this shape on the M3 Pro, so a timeout is a wedge,
# not a slow run.
set -eu

REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
. "$REPO/tools/bench_lock.sh"
cd "$REPO"

OUT=${OUT:-/tmp/starh2-h2load-wedge-rate}
ROUNDS=${ROUNDS:-10}
WATCHDOG=${WATCHDOG:-60}
REQUESTS=${REQUESTS:-400000}
CLIENTS=${CLIENTS:-50}
MAXCONC=${MAXCONC:-10}
THREADS=${THREADS:-4}
STARH2_EXECUTORS=${STARH2_EXECUTORS:-2}

command -v h2load >/dev/null 2>&1 || {
  echo "h2load-wedge-rate: h2load is required" >&2
  exit 2
}
[ -f testdata/cert.pem ] || { echo "h2load-wedge-rate: testdata/cert.pem missing" >&2; exit 2; }
[ -f testdata/key.pem ] || { echo "h2load-wedge-rate: testdata/key.pem missing" >&2; exit 2; }

mkdir -p "$OUT"
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/starh2" || exit 2
STARH2="$OUT/starh2/bin/starh2-bench-server"
[ -x "$STARH2" ] || { echo "h2load-wedge-rate: build did not install $STARH2" >&2; exit 2; }

S_PID=
H_PID=
cleanup() {
  if [ -n "$H_PID" ]; then kill "$H_PID" 2>/dev/null || true; wait "$H_PID" 2>/dev/null || true; H_PID=; fi
  if [ -n "$S_PID" ]; then kill "$S_PID" 2>/dev/null || true; wait "$S_PID" 2>/dev/null || true; S_PID=; fi
  bench_unlock
}
trap cleanup EXIT INT TERM

SHA=$(git -C "$REPO" rev-parse --short HEAD)
echo "h2load-wedge-rate: sha=$SHA host=$(uname -s) rounds=$ROUNDS watchdog=${WATCHDOG}s n=$REQUESTS c=$CLIENTS m=$MAXCONC t=$THREADS executors=$STARH2_EXECUTORS server_args='${SERVER_ARGS:-}'"

wedges=0
ran=0
round=1
while [ "$round" -le "$ROUNDS" ]; do
  # Lock per round, not around the whole loop, so a concurrent harness
  # interleaves between rounds instead of starving for the full matrix.
  bench_lock
  : >"$OUT/server-$round.log"
  # SERVER_ARGS: extra bench-server flags for A/B arms. Splitting intended.
  "$STARH2" --mode tls --port 0 --executors "$STARH2_EXECUTORS" \
    --cert "$REPO/testdata/cert.pem" --key "$REPO/testdata/key.pem" \
    ${SERVER_ARGS:-} \
    >"$OUT/server-$round.log" 2>&1 &
  S_PID=$!

  PORT=
  i=0
  while [ "$i" -lt 100 ]; do
    if grep -q '"ready":true' "$OUT/server-$round.log" 2>/dev/null; then
      PORT=$(sed -n 's/.*"port":\([0-9][0-9]*\).*/\1/p' "$OUT/server-$round.log" | head -1)
      [ -n "$PORT" ] && [ "$PORT" -gt 0 ] && break
    fi
    kill -0 "$S_PID" 2>/dev/null || {
      echo "h2load-wedge-rate: server died during bind (round $round)" >&2
      cat "$OUT/server-$round.log" >&2
      exit 2
    }
    i=$((i + 1))
    sleep 0.1
  done
  [ -n "$PORT" ] && [ "$PORT" -gt 0 ] || {
    echo "h2load-wedge-rate: no ready line (round $round)" >&2
    exit 2
  }

  h2load -n "$REQUESTS" -c "$CLIENTS" -m "$MAXCONC" -t "$THREADS" \
    "https://127.0.0.1:${PORT}/" >"$OUT/h2load-$round.txt" 2>&1 &
  H_PID=$!

  waited=0
  verdict=PASS
  while kill -0 "$H_PID" 2>/dev/null; do
    if [ "$waited" -ge "$WATCHDOG" ]; then
      verdict=WEDGE
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done

  if [ "$verdict" = WEDGE ]; then
    wedges=$((wedges + 1))
    EV="$OUT/wedge-$round"
    mkdir -p "$EV"
    echo "h2load-wedge-rate: round $round WEDGE after ${WATCHDOG}s - capturing evidence in $EV"
    if command -v sample >/dev/null 2>&1; then
      sample "$S_PID" 2 -file "$EV/server-sample.txt" >/dev/null 2>&1 || true
      sample "$H_PID" 2 -file "$EV/h2load-sample.txt" >/dev/null 2>&1 || true
    fi
    ps -M -p "$S_PID" >"$EV/server-threads.txt" 2>&1 || true
    cp "$OUT/h2load-$round.txt" "$EV/h2load-partial.txt" 2>/dev/null || true
    cp "$OUT/server-$round.log" "$EV/server.log" 2>/dev/null || true
    kill "$H_PID" 2>/dev/null || true
    wait "$H_PID" 2>/dev/null || true
  else
    wait "$H_PID" 2>/dev/null || true
    if ! grep -q "requests: $REQUESTS total, $REQUESTS started, $REQUESTS done, $REQUESTS succeeded" "$OUT/h2load-$round.txt"; then
      # An exit without full success is neither a wedge nor a clean pass;
      # fail loudly instead of counting it as either.
      echo "h2load-wedge-rate: round $round exited but did not fully succeed" >&2
      grep -E 'finished in|requests:' "$OUT/h2load-$round.txt" >&2 || true
      exit 2
    fi
    echo "h2load-wedge-rate: round $round PASS ($(grep -o 'finished in [^,]*' "$OUT/h2load-$round.txt" | head -1))"
  fi
  H_PID=

  kill "$S_PID" 2>/dev/null || true
  wait "$S_PID" 2>/dev/null || true
  S_PID=
  bench_unlock

  ran=$((ran + 1))
  round=$((round + 1))
done

echo "h2load-wedge-rate: rounds_run=$ran wedges=$wedges"
[ "$ran" -gt 0 ] || { echo "h2load-wedge-rate: zero rounds ran" >&2; exit 2; }
if [ "$wedges" -gt 0 ]; then
  echo "h2load-wedge-rate: WEDGED $wedges/$ran"
  exit 1
fi
echo "h2load-wedge-rate: CLEAN 0/$ran"
exit 0
