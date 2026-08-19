#!/bin/sh
# Mixed SSE + oneshot on ONE TLS connection, starh2 against Go net/http.
#
# A oneshot bench cannot value the connection split: complete handlers never
# occupy a task, so ingest is never at risk of parking. This run keeps SSE
# handlers live on the socket and fires oneshots on the same Transports.
# Both numbers have to stay up. Great oneshot rps with zero SSE events is
# not a win; SSE events with zero oneshots means ingest parked.
#
# http2.zig is not an arm. Its handler API is one-shot (`setBody`, no flush),
# so it cannot occupy the socket the way this measurement needs. That absence
# is printed rather than omitted.
#
# Traps carried from run.sh: sleep to a deadline (server side), alternate
# arm order, print every round, install the bench server to a known prefix.
set -u

REPO=$(cd "$(dirname "$0")/../.." && pwd -P)
. "$REPO/tools/bench_lock.sh"
STREAMS=${STREAMS:-32}
SECONDS_RUN=${SECONDS_RUN:-5}
INTERVAL=${INTERVAL:-10}
ROUNDS=${ROUNDS:-3}
WARMUP=${WARMUP:-1}
ONESHOT_WORKERS=${ONESHOT_WORKERS:-8}
STARH2_EXECUTORS=${STARH2_EXECUTORS:-2}
OUT=${OUT:-/tmp/starh2-sse-mixed}

command -v go >/dev/null 2>&1 || {
  echo "go is required for the opponent arm; it is not installed" >&2
  exit 1
}

mkdir -p "$OUT"
cd "$REPO/tools/sse_bench"
[ -f go.mod ] || printf 'module ssebench\n\ngo 1.26\n' > go.mod
go build -o "$OUT/sse-client" ./client.go || exit 1

cd "$REPO"
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/starh2" || exit 1
STARH2="$OUT/starh2/bin/starh2-bench-server"

cd "$REPO/tools/sse_bench"
go build -o "$OUT/sse-server" ./server.go || exit 1
cd "$REPO"

TRACE_ARGS=
if [ "${TRACE:-0}" != 0 ]; then
  TRACE_ARGS=--trace
fi
# STARH2_TASK_MIGRATION=1 turns zio task migration on. The server default is
# off, so every historical row of this harness is a migration-off row.
MIGRATION_ARGS=
if [ "${STARH2_TASK_MIGRATION:-0}" != 0 ]; then
  MIGRATION_ARGS=--task-migration
fi
bench_lock
"$STARH2" --mode tls --port 19450 --sse-interval-ms "$INTERVAL" \
  --executors "$STARH2_EXECUTORS" $MIGRATION_ARGS $TRACE_ARGS > "$OUT/starh2.log" 2>&1 &
S_PID=$!
"$OUT/sse-server" -port 19451 -sse-interval-ms "$INTERVAL" \
  -cert testdata/cert.pem -key testdata/key.pem > "$OUT/go.log" 2>&1 &
G_PID=$!
sleep 2

cleanup() {
  kill $S_PID $G_PID 2>/dev/null
  wait 2>/dev/null
  bench_unlock
}
trap cleanup EXIT INT TERM

for port in 19450 19451; do
  if ! "$OUT/sse-client" -url "https://127.0.0.1:$port/sse" -streams 1 -seconds 1 -warmup 0 -label probe >/dev/null 2>&1; then
    echo "the server on port $port did not deliver an SSE event — refusing to report" >&2
    exit 1
  fi
  if ! "$OUT/sse-client" -streams 0 -oneshot-url "https://127.0.0.1:$port/" -oneshot-workers 1 -seconds 1 -warmup 0 -label probe >/dev/null 2>&1; then
    echo "the server on port $port did not answer GET / — refusing to report" >&2
    exit 1
  fi
done

cpu_seconds() {
  ps -o time= -p "$1" | awk '{
    gsub(/[[:space:]]/, "", $0)
    fraction_parts = split($0, fraction, ".")
    clock_parts = split(fraction[1], clock, ":")
    total = 0
    for (i = 1; i <= clock_parts; i++) total = (total * 60) + clock[i]
    if (fraction_parts > 1) total += ("0." fraction[2])
    printf "%.2f", total
  }'
}

sse_target=$(( SECONDS_RUN * 1000 / INTERVAL * STREAMS ))
echo "== mixed SSE + oneshot, one TLS connection per arm"
echo "== $STREAMS SSE streams, ${SECONDS_RUN}s + ${WARMUP}s warmup, one event per ${INTERVAL}ms, $ONESHOT_WORKERS oneshot workers, $ROUNDS rounds"
echo "== starh2 executors=$STARH2_EXECUTORS  SSE target $sse_target events/arm/round"
echo "== http2.zig is not an arm: one-shot handler API, no flush"

i=1
while [ "$i" -le "$ROUNDS" ]; do
  echo "round $i"
  if [ $(( i % 2 )) -eq 1 ]; then
    order="starh2:19450 go-net/http:19451"
  else
    order="go-net/http:19451 starh2:19450"
  fi
  for arm in $order; do
    name=${arm%%:*}; port=${arm##*:}
    if [ "$name" = starh2 ]; then pid=$S_PID; else pid=$G_PID; fi
    echo "  --- $name oneshot-only ---"
    cpu_before=$(cpu_seconds "$pid")
    "$OUT/sse-client" -streams 0 \
      -oneshot-url "https://127.0.0.1:$port/" \
      -oneshot-workers "$ONESHOT_WORKERS" \
      -seconds "$SECONDS_RUN" -warmup "$WARMUP" -label "$name" | sed 's/^/  /'
    echo "  --- $name mixed ---"
    "$OUT/sse-client" -url "https://127.0.0.1:$port/sse" -streams "$STREAMS" \
      -oneshot-url "https://127.0.0.1:$port/" \
      -oneshot-workers "$ONESHOT_WORKERS" \
      -seconds "$SECONDS_RUN" -warmup "$WARMUP" -label "$name" | sed 's/^/  /'
    cpu_after=$(cpu_seconds "$pid")
    cpu_used=$(awk -v before="$cpu_before" -v after="$cpu_after" 'BEGIN { printf "%.2f", after - before }')
    echo "  $name server CPU=${cpu_used}s (oneshot-only + mixed, including warmup)"
  done
  i=$((i + 1))
done

if [ "${STALL:-1}" != 0 ]; then
  echo "stall: one SSE stream stops reading; p99 of the others + oneshot rps"
  echo "  --- starh2 mixed + stall ---"
  "$OUT/sse-client" -url "https://127.0.0.1:19450/sse" -streams "$STREAMS" -stall \
    -oneshot-url "https://127.0.0.1:19450/" -oneshot-workers "$ONESHOT_WORKERS" \
    -seconds "$SECONDS_RUN" -warmup "$WARMUP" -label starh2 | sed 's/^/  /'
  echo "  --- go-net/http mixed + stall ---"
  "$OUT/sse-client" -url "https://127.0.0.1:19451/sse" -streams "$STREAMS" -stall \
    -oneshot-url "https://127.0.0.1:19451/" -oneshot-workers "$ONESHOT_WORKERS" \
    -seconds "$SECONDS_RUN" -warmup "$WARMUP" -label go-net/http | sed 's/^/  /'
fi

if [ "${TRACE:-0}" != 0 ]; then
  echo "== /trace inbound leftover (after oneshot-only + mixed + stall)"
  curl -sk --http2 "https://127.0.0.1:19450/trace" > "$OUT/trace.json"
  python3 - "$OUT/trace.json" <<'PY'
import json, sys
b = json.load(open(sys.argv[1]))
def d(k):
    return b.get(k, 0)
take = d("read_take_n")
left0 = d("read_left_0")
print(
    "  takes=%d  leftover: 0=%d 1=%d 2=%d 3=%d >=4=%d"
    % (take, left0, d("read_left_1"), d("read_left_2"), d("read_left_3"), d("read_left_ge4"))
)
print(
    "  mean leftover=%.2f  max=%d  backlog_rate=%.2f%%"
    % (d("read_left_sum") / max(take, 1), b.get("read_left_max", 0), 100.0 * (take - left0) / max(take, 1))
)
b1 = d("inbound_batch_1"); b2 = d("inbound_batch_2"); b3 = d("inbound_batch_3"); b4 = d("inbound_batch_4")
turns = b1 + b2 + b3 + b4
print(
    "  ingest turns: 1=%d 2=%d 3=%d 4=%d  mean chunks/turn=%.2f"
    % (b1, b2, b3, b4, (b1 + 2 * b2 + 3 * b3 + 4 * b4) / max(turns, 1))
)
PY
fi

cleanup
trap - EXIT INT TERM
