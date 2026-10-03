#!/bin/sh
# Control for the collapse hunt: the same client and mix shape against Go
# net/http (GOMAXPROCS=12), which shares no code with starh2. A stuck stream
# open here would put the cause in the client.
set -u
cd /tmp/starh2-collapse
for r in $(seq 1 ${ROUNDS:-25}); do
  GOMAXPROCS=12 ./goserver -port 18555 -sse-interval-ms 1 -cert cert.pem -key key.pem > go.log 2>&1 &
  G=$!; sleep 0.5
  ./client -url https://127.0.0.1:18555/sse -streams 1500 -conns 6 -light-conns 12 -light-streams 10 \
    -churn-workers 4 -churn-url https://127.0.0.1:18555/ -stagger-ms 50 -interval-ms 1 -seconds 10 -warmup 1 -label g$r > gclient.out 2>&1 &
  C=$!; sleep 20
  if kill -0 $C 2>/dev/null; then echo "g$r: STUCK (client still waiting at 20 s)"; wait $C; grep -E "failed:|streams=" gclient.out | head -8; else wait $C; echo "g$r: normal $(grep streams= gclient.out | sed 's/.*opened=/opened=/')"; fi
  kill $G; wait $G 2>/dev/null
done
