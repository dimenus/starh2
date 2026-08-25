#!/bin/sh
set -eu
REPO=/tmp/starh2-full
OUT=/tmp/nachos-full-461071b/oneshot-w2
cd "$REPO"
mkdir -p "$OUT"
cd tools/sse_bench
[ -f go.mod ] || printf 'module ssebench\n\ngo 1.26\n' > go.mod
go build -o "$OUT/sse-client" ./client.go
go build -o "$OUT/sse-server" ./server.go
cd "$REPO"
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/starh2"
STARH2="$OUT/starh2/bin/starh2-bench-server"
"$STARH2" --mode tls --port 19450 --executors 2 > "$OUT/starh2.log" 2>&1 &
S_PID=$!
"$OUT/sse-server" -port 19451 -sse-interval-ms 10 \
  -cert testdata/cert.pem -key testdata/key.pem > "$OUT/go.log" 2>&1 &
G_PID=$!
sleep 2
cleanup() { kill $S_PID $G_PID 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT INT TERM
r=1
while [ "$r" -le 3 ]; do
  echo "round $r"
  if [ $((r % 2)) -eq 1 ]; then
    order="starh2:19450 go-net/http:19451"
  else
    order="go-net/http:19451 starh2:19450"
  fi
  for arm in $order; do
    name=${arm%%:*}
    port=${arm##*:}
    echo "  --- $name w2 ---"
    "$OUT/sse-client" -streams 0 \
      -oneshot-url "https://127.0.0.1:$port/" \
      -oneshot-workers 2 -seconds 5 -warmup 1 -label "$name"
  done
  r=$((r + 1))
done
