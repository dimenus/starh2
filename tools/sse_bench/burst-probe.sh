#!/bin/sh
# Open N SSE streams at once on ONE TLS connection, R times, and refuse any
# round that does not open and deliver every stream. This is the check for
# the class found while grading the actor/driver merge (2026-08-21): 200
# streams opening together made one actor gap emit more HEADERS than the
# TLS write stash could hold, and the connection failed closed SILENTLY
# (`opened=146..175 failed=25..54`, nothing logged). The unit suite cannot
# reach the TLS edge and tls-smoke drives curl one request at a time, so
# neither saw it; this probe does. It also reads the bench server's /trace
# and fails if the named fail-close counters moved.
#
#   STREAMS=200 ROUNDS=10 SECONDS_RUN=2 tools/sse_bench/burst-probe.sh
#
# Exit 0 only when every round prints opened=STREAMS delivering=STREAMS
# failed=0 AND tls_write_overflow == tls_stage_failed == 0.
#
# Mutation-proven against the pre-fix tree (42454d6): the defect showed in
# 1 of 10 idle rounds on a laptop (4 of 5 under build load), so ROUNDS=10 is
# the floor; raise it for a stronger check. The counters are the second
# oracle: a tree without them cannot pass.
set -u

REPO=$(cd "$(dirname "$0")/../.." && pwd -P)
. "$REPO/tools/bench_lock.sh"
STREAMS=${STREAMS:-200}
ROUNDS=${ROUNDS:-10}
SECONDS_RUN=${SECONDS_RUN:-2}
STARH2_EXECUTORS=${STARH2_EXECUTORS:-2}
PORT=${PORT:-19472}
OUT=${OUT:-/tmp/starh2-burst-probe}

for f in testdata/cert.pem testdata/key.pem; do
  [ -f "$REPO/$f" ] || { echo "$f is missing (see tools/README.md, TLS section)" >&2; exit 1; }
done
command -v go >/dev/null 2>&1 || { echo "go is required for the client" >&2; exit 1; }
mkdir -p "$OUT"
cd "$REPO/tools/sse_bench" && go build -o "$OUT/sse-client" ./client.go || exit 1
cd "$REPO" && ./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/starh2" || exit 1

bench_lock
"$OUT/starh2/bin/starh2-bench-server" --mode tls --port "$PORT" --sse-interval-ms 10 \
  --executors "$STARH2_EXECUTORS" > "$OUT/starh2.log" 2>&1 &
S_PID=$!
cleanup() { kill $S_PID 2>/dev/null; wait 2>/dev/null; bench_unlock; }
trap cleanup EXIT INT TERM
i=0
while [ "$i" -lt 50 ]; do grep -q '"ready":true' "$OUT/starh2.log" 2>/dev/null && break; sleep 0.1; i=$((i + 1)); done
grep -q '"ready":true' "$OUT/starh2.log" || { echo "bench server did not start" >&2; exit 1; }

fail=0
r=1
while [ "$r" -le "$ROUNDS" ]; do
  line=$("$OUT/sse-client" -url "https://127.0.0.1:$PORT/sse" -streams "$STREAMS" -seconds "$SECONDS_RUN" -warmup 0 -label burst 2>&1 | grep 'streams=')
  echo "round $r: $line"
  case "$line" in
    *"opened=$STREAMS "*"delivering=$STREAMS "*"failed=0 "*) ;;
    *) fail=1 ;;
  esac
  r=$((r + 1))
done
trace=$(curl -sk --http2 "https://127.0.0.1:$PORT/trace" 2>/dev/null)
overflow=$(echo "$trace" | sed -n 's/.*"tls_write_overflow":\([0-9]*\).*/\1/p')
staged=$(echo "$trace" | sed -n 's/.*"tls_stage_failed":\([0-9]*\).*/\1/p')
echo "trace: tls_write_overflow=${overflow:-absent} tls_stage_failed=${staged:-absent}"
if [ -z "$overflow" ] || [ -z "$staged" ]; then
  echo "burst-probe: /trace did not report the fail-close counters — refusing to pass" >&2
  fail=1
elif [ "$overflow" != 0 ] || [ "$staged" != 0 ]; then
  fail=1
fi
if [ "$fail" -ne 0 ]; then
  echo "burst-probe: FAIL ($STREAMS streams x $ROUNDS rounds)" >&2
  exit 1
fi
echo "burst-probe: PASS $STREAMS streams x $ROUNDS rounds, every stream opened and delivered, counters 0"
