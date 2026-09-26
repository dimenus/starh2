#!/bin/sh
# How many TCP connections does the Go SSE client open for 500 streams, and
# how long does each live? The probe counted 3 connEntry calls in most runs
# (2 SSE connections + the /trace curl) and 4-5 in every natural knee run.
#
# Samples `ss -tan` every 20 ms for the first 3 s of a 500-stream client run
# against the release PL binary, and prints each client port seen with the
# first and last sample it was seen in. 10 runs.
#
#   HOST=ryan@100.113.184.27 captures/zio-placement-probe/dial-count.sh
set -eu
HOST=${HOST:-ryan@100.113.184.27}
ssh "$HOST" 'sh -s' <<'REMOTE'
D=/tmp/zio-placement-recheck
cd $D
for run in 1 2 3 4 5 6 7 8 9 10; do
  ./PL-server --mode tls --port 0 --executors 2 --sse-interval-ms 1 --cert cert.pem --key key.pem --spawn-placement local > dc.log 2>&1 &
  P=$!; sleep 0.5
  PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' dc.log)
  ./client -url https://127.0.0.1:$PORT/sse -streams 500 -seconds 3 -warmup 1 -label x > dc.out 2>&1 &
  C=$!
  i=0
  while [ $i -lt 150 ]; do
    ss -tanH "( sport = :$PORT )" | awk -v i=$i '{ n = split($5, a, ":"); print i, $1, a[n] }'
    i=$((i+1)); sleep 0.02
  done > dc.ss
  wait $C; kill $P; wait $P 2>/dev/null
  echo "run$run $(grep 'events=' dc.out | sed 's/^ *x *//') ports: $(awk '$2 != "LISTEN" { if (!($3 in f)) { f[$3] = $1; o[++n] = $3 } l[$3] = $1; s[$3] = $2 }
    END { for (i = 1; i <= n; i++) printf "%s[%d-%d,%s] ", o[i], f[o[i]], l[o[i]], s[o[i]] }' dc.ss)"
done
rm -f dc.ss dc.out dc.log
REMOTE
