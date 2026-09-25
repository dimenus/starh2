#!/bin/sh
# Is the user-space h2load (nghttp2 1.70.0) on nachos client-bound at -t 4?
#
# tools/zio-arm-ab.sh says its one-shot rows are server-bound because an
# h2load thread sweep was flat. That sweep was of the h2load nachos had before
# its reinstall. This one repeats it for the h2load this A/B uses, on arm B,
# at both widths: if -t 8 or -t 12 is clearly faster than -t 4, the -t 4 rows
# are measuring the client and would hide a server difference.
#
#   HOST=ryan@100.113.184.27 captures/zio-pin-ab-0299e57/h2load-sweep.sh
#
# Needs the arm B server already shipped to $REMOTE_DIR/B-server by run.sh.
set -eu
HOST=${HOST:-ryan@100.113.184.27}
H2LOAD=${H2LOAD:-/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load}
REMOTE_DIR=${REMOTE_DIR:-/tmp/zio-pin-ab}
ssh "$HOST" "H2LOAD='$H2LOAD' D='$REMOTE_DIR' sh -s" <<'REMOTE'
set -u
uptime
sha256sum $D/B-server
for EXEC in 2 8; do
  for round in 1 2 3; do
    for T in 2 4 8 12; do
      $D/B-server --mode tls --port 0 --executors $EXEC --sse-interval-ms 1 \
        --cert $D/cert.pem --key $D/key.pem > $D/sweep.log 2>&1 &
      PID=$!
      i=0; PORT=
      while [ $i -lt 200 ]; do
        PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' $D/sweep.log | head -1)
        [ -n "$PORT" ] && break
        i=$((i+1)); sleep 0.05
      done
      out=$(timeout 180 "$H2LOAD" -n 2000000 -c 50 -m 10 -t $T https://127.0.0.1:$PORT/ 2>&1)
      echo "e$EXEC round$round t$T $(echo "$out" | grep -E 'finished in|requests:' | tr '\n' ' ')"
      kill $PID; wait $PID 2>/dev/null
    done
  done
done
REMOTE
