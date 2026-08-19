#!/bin/sh
# Concurrent long-lived SSE: starh2 against Go's net/http, over TLS h2.
#
# # Why Go is the opponent
#
# No other Zig HTTP/2 server can stream. The only one that builds and serves h2
# on 0.16 (hendriknielaender/http2.zig) has a one-shot handler API — fn(ctx)
# !Response with setBody — no flush, no event-stream. So the SSE arm has no Zig
# opponent, and Go's net/http is used instead: a mature h2 server that shares no
# code with anything here.
#
# # Why this is not h2load
#
# h2load measures request throughput. A long-lived stream is ONE request that
# then behaves for minutes. What matters is whether event k arrives on time
# while many streams are open, so the server stamps each event with its own
# clock and the client subtracts. Both ends are local and read the same clock.
#
# # Two traps this harness exists to avoid, both hit while writing it
#
# 1. SLEEP TO A DEADLINE, NOT FOR A DURATION. A handler that sleeps for the
#    interval has a period of interval-plus-work, so it emits below the
#    requested rate and looks like it dropped events when it only ticked slowly.
#    The first version of the starh2 arm did this and read 11 percent low.
# 2. ONE RUN IS NOISE. The first pass read p50=40us for Go, then p50=531us for
#    the same unchanged binary. Worse, the arm that runs SECOND in a round wins
#    on every ordering, so a fixed order manufactures a result. This script
#    alternates the order each round and prints every round.
#
# # Reading the output
#
# The signal is not latency alone, it is latency TOGETHER WITH the delivered
# count. A server that keeps up delivers ~100 percent of the target. A saturated
# one pins its delivered count and its p50 grows in proportion to the overshoot.
set -u

REPO=$(cd "$(dirname "$0")/../.." && pwd -P)
. "$REPO/tools/bench_lock.sh"
STREAMS=${STREAMS:-200}
SECONDS_RUN=${SECONDS_RUN:-10}
INTERVAL=${INTERVAL:-10}
ROUNDS=${ROUNDS:-4}
WARMUP=${WARMUP:-1}
STARH2_EXECUTORS=${STARH2_EXECUTORS:-2}
GO_MAX_PROCS=${GO_MAX_PROCS:-auto}
OUT=${OUT:-/tmp/starh2-sse-bench}

command -v go >/dev/null 2>&1 || {
  echo "go is required for the opponent arm; it is not installed" >&2
  exit 1
}

mkdir -p "$OUT"
cd "$REPO/tools/sse_bench"
[ -f go.mod ] || printf 'module ssebench\n\ngo 1.26\n' > go.mod
go build -o "$OUT/sse-server" ./server.go || exit 1
go build -o "$OUT/sse-client" ./client.go || exit 1

cd "$REPO"
# Install to a known prefix and use THAT path. Picking the newest binary in
# .zig-cache by mtime measures whatever the last build step happened to write:
# after `zig build ci`, that is an example at a different optimize level, and
# the run then reports a number for a binary nobody asked for. Observed: a
# 4.7x improvement read back as no change at all.
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/starh2" || exit 1
STARH2="$OUT/starh2/bin/starh2-bench-server"

bench_lock
if [ "$STARH2_EXECUTORS" = auto ]; then
  "$STARH2" --mode tls --port 19446 --sse-interval-ms "$INTERVAL" > "$OUT/starh2.log" 2>&1 &
else
  "$STARH2" --mode tls --port 19446 --sse-interval-ms "$INTERVAL" \
    --executors "$STARH2_EXECUTORS" > "$OUT/starh2.log" 2>&1 &
fi
S_PID=$!
if [ "$GO_MAX_PROCS" = auto ]; then
  "$OUT/sse-server" -port 19447 -sse-interval-ms "$INTERVAL" -cert testdata/cert.pem -key testdata/key.pem > "$OUT/go.log" 2>&1 &
else
  GOMAXPROCS="$GO_MAX_PROCS" "$OUT/sse-server" -port 19447 -sse-interval-ms "$INTERVAL" -cert testdata/cert.pem -key testdata/key.pem > "$OUT/go.log" 2>&1 &
fi
G_PID=$!
sleep 2

cleanup() {
  kill ${S_RSS_PID:-} ${G_RSS_PID:-} $S_PID $G_PID 2>/dev/null
  wait 2>/dev/null
  bench_unlock
}
trap cleanup EXIT INT TERM

# An arm that never answered must not be reported as a slow arm.
for port in 19446 19447; do
  if ! "$OUT/sse-client" -url "https://127.0.0.1:$port/sse" -streams 1 -seconds 2 -warmup "$WARMUP" -label probe >/dev/null 2>&1; then
    echo "the server on port $port did not deliver an event — refusing to report" >&2
    kill $S_PID $G_PID 2>/dev/null
    exit 1
  fi
done

sample_rss() {
  pid=$1
  out=$2
  : > "$out"
  while kill -0 "$pid" 2>/dev/null; do
    ps -o rss= -p "$pid" >> "$out"
    sleep 1
  done
}

sample_rss "$S_PID" "$OUT/starh2-rss.txt" &
S_RSS_PID=$!
sample_rss "$G_PID" "$OUT/go-rss.txt" &
G_RSS_PID=$!

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

target=$(( SECONDS_RUN * 1000 / INTERVAL * STREAMS ))
echo "== $STREAMS streams, ${SECONDS_RUN}s + ${WARMUP}s warmup, one event per ${INTERVAL}ms, $ROUNDS rounds"
echo "== runtime: starh2 executors=$STARH2_EXECUTORS  go GOMAXPROCS=$GO_MAX_PROCS"
echo "== target $target events per arm per round ($(( STREAMS * 1000 / INTERVAL )) events/s)"

i=1
while [ "$i" -le "$ROUNDS" ]; do
  echo "round $i"
  if [ $(( i % 2 )) -eq 1 ]; then
    order="starh2:19446 go-net/http:19447"
  else
    order="go-net/http:19447 starh2:19446"
  fi
  for arm in $order; do
    name=${arm%%:*}; port=${arm##*:}
    if [ "$name" = starh2 ]; then pid=$S_PID; else pid=$G_PID; fi
    cpu_before=$(cpu_seconds "$pid")
    "$OUT/sse-client" -url "https://127.0.0.1:$port/sse" -streams "$STREAMS" \
      -seconds "$SECONDS_RUN" -warmup "$WARMUP" -label "$name" | sed 's/^/  /'
    cpu_after=$(cpu_seconds "$pid")
    cpu_used=$(awk -v before="$cpu_before" -v after="$cpu_after" 'BEGIN { printf "%.2f", after - before }')
    echo "  $name server CPU=${cpu_used}s (opening + warmup + measurement)"
  done
  i=$((i + 1))
done

starh2_retained=$(ps -o rss= -p $S_PID)
go_retained=$(ps -o rss= -p $G_PID)
kill $S_RSS_PID $G_RSS_PID 2>/dev/null
wait $S_RSS_PID $G_RSS_PID 2>/dev/null
starh2_peak=$(awk 'BEGIN { m=0 } $1 > m { m=$1 } END { print m }' "$OUT/starh2-rss.txt")
go_peak=$(awk 'BEGIN { m=0 } $1 > m { m=$1 } END { print m }' "$OUT/go-rss.txt")
echo "== RSS sampled during benchmark rounds"
echo "  peak:     starh2=${starh2_peak}KB  go=${go_peak}KB"
echo "  retained: starh2=${starh2_retained}KB  go=${go_retained}KB"
cleanup
trap - EXIT INT TERM
