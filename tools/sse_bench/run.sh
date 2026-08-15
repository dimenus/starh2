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
STREAMS=${STREAMS:-200}
SECONDS_RUN=${SECONDS_RUN:-10}
INTERVAL=${INTERVAL:-10}
ROUNDS=${ROUNDS:-4}
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

"$STARH2" --mode tls --port 19446 --sse-interval-ms "$INTERVAL" > "$OUT/starh2.log" 2>&1 &
S_PID=$!
"$OUT/sse-server" -port 19447 -sse-interval-ms "$INTERVAL" -cert testdata/cert.pem -key testdata/key.pem > "$OUT/go.log" 2>&1 &
G_PID=$!
sleep 2

# An arm that never answered must not be reported as a slow arm.
for port in 19446 19447; do
  if ! "$OUT/sse-client" -url "https://127.0.0.1:$port/sse" -streams 1 -seconds 2 -label probe >/dev/null 2>&1; then
    echo "the server on port $port did not deliver an event — refusing to report" >&2
    kill $S_PID $G_PID 2>/dev/null
    exit 1
  fi
done

target=$(( SECONDS_RUN * 1000 / INTERVAL * STREAMS ))
echo "== $STREAMS streams, ${SECONDS_RUN}s, one event per ${INTERVAL}ms, $ROUNDS rounds"
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
    "$OUT/sse-client" -url "https://127.0.0.1:$port/sse" -streams "$STREAMS" \
      -seconds "$SECONDS_RUN" -label "$name" | sed 's/^/  /'
  done
  i=$((i + 1))
done

echo "== RSS while $STREAMS streams were open"
echo "  starh2=$(ps -o rss= -p $S_PID)KB  go=$(ps -o rss= -p $G_PID)KB"
kill $S_PID $G_PID 2>/dev/null
wait 2>/dev/null
