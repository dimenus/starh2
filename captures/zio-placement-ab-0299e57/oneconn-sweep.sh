#!/bin/sh
# Is the one-connection one-shot row server-bound, and at how many workers?
#
# The first oneconn session (oneconn-rows.txt, 8 workers) read 183-191k rps
# with p50 37 us for EVERY arm, A included: a flat line that could not show a
# placement difference, which is what a client limit looks like. This sweeps
# the worker count on WS and PL at 2 executors, 3 runs each, so the next
# session can use a count where rps stops rising with more workers.
#
#   HOST=ryan@100.113.184.27 captures/zio-placement-ab-0299e57/oneconn-sweep.sh
#
# Needs WS-server, PL-server and the client already shipped to $REMOTE_DIR.
set -eu
HOST=${HOST:-ryan@100.113.184.27}
REMOTE_DIR=${REMOTE_DIR:-/tmp/zio-placement-ab}
ssh "$HOST" "D='$REMOTE_DIR' sh -s" <<'REMOTE'
set -u
uptime
sha256sum $D/WS-server $D/PL-server
for run in 1 2 3; do
  for W in 8 32 128 512; do
    for ARM in WS PL; do
      extra=""; [ $ARM = PL ] && extra="--spawn-placement local"
      $D/$ARM-server --mode tls --port 0 --executors 2 --sse-interval-ms 1 \
        --cert $D/cert.pem --key $D/key.pem $extra > $D/sweep.log 2>&1 &
      PID=$!
      i=0; PORT=
      while [ $i -lt 200 ]; do
        PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' $D/sweep.log | head -1)
        [ -n "$PORT" ] && break
        i=$((i+1)); sleep 0.05
      done
      line=$(timeout 60 $D/client -streams 0 -conns 1 -oneshot-url https://127.0.0.1:$PORT/ \
        -oneshot-workers $W -seconds 5 -warmup 1 -label $ARM 2>&1 | grep 'oneshot ok=')
      echo "run$run w$W $line"
      kill $PID; wait $PID 2>/dev/null
    done
  done
done
REMOTE
