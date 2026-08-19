#!/bin/sh
# Deterministic reproducer for the mem-BIO oneshot wedge (302c0d7).
#
# The wedge is the Go client's 8-worker oneshot-only shape on ONE TLS
# connection (tools/sse_bench/client.go -streams 0 -oneshot-workers 8).
# h2load -c 1 -m 10 does not reproduce it. Healthy is tens of thousands
# of rps; wedged is 0-32. Exit non-zero below THRESHOLD (default 5000).
#
# Warmup defaults to 1s because the wedge arrives after a ~0.7s burst:
# rps = completed / SECONDS_RUN, so a 5s window that includes the burst
# can average above the threshold while the connection is already dead.
# mixed.sh oneshot-only uses the same warmup; keep it unless you are
# measuring something else.
#
# SECONDS_RUN=5 is load-bearing. 1s and 2s false-green on 302c0d7: the
# pre-wedge burst still fills the average. WORKERS defaults to 8 (the
# mixed.sh shape). WORKERS=2 is a tighter Darwin wedge (0 rps 3/3);
# WORKERS=1 flickers.
set -eu

REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
. "$REPO/tools/bench_lock.sh"
cd "$REPO"

OUT=${OUT:-/tmp/starh2-wedge-probe}
SECONDS_RUN=${SECONDS_RUN:-5}
WARMUP=${WARMUP:-1}
WORKERS=${WORKERS:-8}
THRESHOLD=${THRESHOLD:-5000}
STARH2_EXECUTORS=${STARH2_EXECUTORS:-2}
LABEL=${LABEL:-wedge}

command -v go >/dev/null 2>&1 || {
  echo "wedge-probe: go is required to build tools/sse_bench/client.go" >&2
  exit 2
}
[ -f testdata/cert.pem ] || {
  echo "wedge-probe: testdata/cert.pem is missing" >&2
  exit 2
}
[ -f testdata/key.pem ] || {
  echo "wedge-probe: testdata/key.pem is missing" >&2
  exit 2
}

mkdir -p "$OUT"
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/starh2" || exit 2
STARH2="$OUT/starh2/bin/starh2-bench-server"
[ -x "$STARH2" ] || {
  echo "wedge-probe: build did not install $STARH2" >&2
  exit 2
}

(
  cd "$REPO/tools/sse_bench"
  [ -f go.mod ] || printf 'module ssebench\n\ngo 1.26\n' > go.mod
  go build -o "$OUT/sse-client" ./client.go
) || exit 2

S_PID=
cleanup() {
  if [ -n "$S_PID" ]; then
    kill "$S_PID" 2>/dev/null || true
    wait "$S_PID" 2>/dev/null || true
    S_PID=
  fi
  bench_unlock
}
trap cleanup EXIT INT TERM

bench_lock
: >"$OUT/server.log"
"$STARH2" --mode tls --port 0 --executors "$STARH2_EXECUTORS" \
  --cert "$REPO/testdata/cert.pem" --key "$REPO/testdata/key.pem" \
  >"$OUT/server.log" 2>&1 &
S_PID=$!

PORT=
i=0
while [ "$i" -lt 100 ]; do
  if grep -q '"ready":true' "$OUT/server.log" 2>/dev/null; then
    PORT=$(sed -n 's/.*"port":\([0-9][0-9]*\).*/\1/p' "$OUT/server.log" | head -1)
    if [ -n "$PORT" ] && [ "$PORT" -gt 0 ]; then
      break
    fi
  fi
  if ! kill -0 "$S_PID" 2>/dev/null; then
    echo "wedge-probe: server died during bind" >&2
    cat "$OUT/server.log" >&2
    exit 2
  fi
  i=$((i + 1))
  sleep 0.1
done
if [ -z "$PORT" ] || [ "$PORT" -le 0 ]; then
  echo "wedge-probe: server never printed a ready line with a port" >&2
  cat "$OUT/server.log" >&2
  exit 2
fi

SHA=$(git -C "$REPO" rev-parse --short HEAD)
echo "wedge-probe: sha=$SHA host=$(uname -s) workers=$WORKERS seconds=$SECONDS_RUN warmup=$WARMUP threshold=$THRESHOLD executors=$STARH2_EXECUTORS port=$PORT"
echo "wedge-probe: bin=$STARH2 out=$OUT"

set +e
"$OUT/sse-client" -streams 0 \
  -oneshot-url "https://127.0.0.1:${PORT}/" \
  -oneshot-workers "$WORKERS" \
  -seconds "$SECONDS_RUN" -warmup "$WARMUP" \
  -label "$LABEL" >"$OUT/client.log" 2>&1
CLIENT_RC=$?
set -e
cat "$OUT/client.log"

RPS=$(sed -n 's/.* rps=\([0-9][0-9]*\).*/\1/p' "$OUT/client.log" | tail -1)
if [ -z "$RPS" ]; then
  echo "wedge-probe: rps=MISSING client_rc=$CLIENT_RC (no rps= line in client output)"
  exit 1
fi
echo "wedge-probe: rps=$RPS"

if [ "$RPS" -lt "$THRESHOLD" ]; then
  echo "wedge-probe: FAIL rps $RPS < $THRESHOLD"
  exit 1
fi
echo "wedge-probe: PASS rps $RPS >= $THRESHOLD"
exit 0
