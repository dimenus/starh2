#!/bin/sh
# Instrument check for the open-loop rows: does h2load's own thread count set
# the latency floor at a fixed offered load?
#
# In an open-loop run the offered rate is fixed, so a latency percentile is
# only a server property if the client adds the same small delay to every
# arm. h2load measures from when it SENDS a request, and its 4 threads also
# run 50 TLS clients; a busy client thread delays both sends and response
# reads. If p50/p99 fall when -t rises at the same offered load, the -t 4
# rows would be measuring the client.
#
# Offered total 320k and 590k req/s (about 40% and 75% of WS's saturated
# ~790k at 2 executors), -c 50 -m 10, --rps = total / 50 per client,
# -D 6 --warm-up-time 1. Runs A and WS, 2 runs each, -t 4, 8 and 12.
#
#   HOST=ryan@100.113.184.27 captures/zio-openloop-0299e57/client-probe.sh
#
# Needs A-server and WS-server shipped to $REMOTE_DIR (run.sh does that).
set -eu
HOST=${HOST:-ryan@100.113.184.27}
REMOTE_DIR=${REMOTE_DIR:-/tmp/zio-openloop}
H2LOAD=${H2LOAD:-/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load}
ssh "$HOST" "D='$REMOTE_DIR' H='$H2LOAD' sh -s" <<'REMOTE'
set -u
uptime
sha256sum $D/A-server $D/WS-server
for run in 1 2; do
  for total in 320000 590000; do
    for T in 4 8 12; do
      for ARM in A WS; do
        $D/$ARM-server --mode tls --port 0 --executors 2 --sse-interval-ms 1 \
          --cert $D/cert.pem --key $D/key.pem > $D/probe.log 2>&1 &
        PID=$!
        i=0; PORT=
        while [ $i -lt 200 ]; do
          PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' $D/probe.log | head -1)
          [ -n "$PORT" ] && break
          i=$((i+1)); sleep 0.05
        done
        rm -f $D/probe.tsv
        out=$($H -c 50 -m 10 -t $T --rps=$((total / 50)) -D 6 --warm-up-time 1 \
          --log-file=$D/probe.tsv https://127.0.0.1:$PORT/ 2>&1)
        pct=$(awk -F'\t' '$2 == 200 {print $3}' $D/probe.tsv | sort -n | awk '{a[NR]=$1}
          END {printf "n=%d p50us=%d p99us=%d p999us=%d", NR, a[int(NR*.5)], a[int(NR*.99)], a[int(NR*.999)]}')
        echo "run$run offered=$total t$T $ARM $pct $(echo "$out" | grep -E '^finished in' | tr '\n' ' ')"
        kill $PID; wait $PID 2>/dev/null
      done
    done
  done
done
rm -f $D/probe.tsv $D/probe.log
REMOTE
