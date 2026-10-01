#!/bin/sh
# Hunt the zero-event collapse (t-2655) on nachos and capture it live.
#
#   ssh ryan@nachos.trex-elevator.ts.net 'bash -s' < collapse-hunt.sh
#   (env: ROUNDS, RANKS, D)
#
# Shape: the one that collapsed (unpaced churn mix at production width, 12
# executors: 6 heavy x 250 streams, 12 light x 10, 4 churn workers). The
# server runs with --allow-ptrace, so gdb can attach AFTER it wedges without
# being its parent (yama ptrace_scope=1) and without changing timing before.
# A normal run ends in ~12 s; a client still running at 20 s means streams
# stopped delivering. Then: per-thread CPU, `thread apply all bt`, and stop.
set -u
D=${D:-/tmp/starh2-collapse}
ROUNDS=${ROUNDS:-12}
RANKS=${RANKS:-handlers_first sum}
cd $D
uptime
for r in $(seq 1 $ROUNDS); do
  for rank in $RANKS; do
    extra="--spawn-placement local"
    [ "$rank" = none ] || extra="$extra --conn-balance --balance-rank $rank"
    ./server --mode tls --port 0 --sse-interval-ms 1 --cert cert.pem --key key.pem --allow-ptrace $extra > srv.log 2>&1 &
    P=$!
    for i in $(seq 100); do PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' srv.log | head -1); [ -n "$PORT" ] && break; sleep 0.05; done
    E=$(sed -n 's/.*"executors":\([0-9]*\).*/\1/p' srv.log | head -1)
    ./client -url https://127.0.0.1:$PORT/sse -streams $((E / 2 * 250)) -conns $((E / 2)) -light-conns $E -light-streams 10 \
      -churn-workers 4 -churn-url https://127.0.0.1:$PORT/ -stagger-ms 50 -interval-ms 1 -seconds 10 -warmup 1 -label r$r > client.out 2>&1 &
    C=$!
    sleep 20
    if kill -0 $C 2>/dev/null; then
      echo "r$r rank=$rank: COLLAPSE (client still waiting at 20 s)"
      echo "threads=$(for t in /proc/$P/task/*; do echo "$(basename $t):$(sed 's/^.*) //' $t/stat | awk '{print $12+$13}')"; done | sort -t: -k2 -rn | head -14 | tr '\n' ' ')"
      # Handlers that got past startSse (all streams, light and heavy): if it
      # is short of the opened count, the stuck streams' handlers never
      # produced HEADERS; if it is complete, the HEADERS were produced and
      # never left the server.
      echo "cadence-1: $(curl -sk --http2 --max-time 5 https://127.0.0.1:$PORT/sse-cadence)"
      sleep 2
      echo "cadence-2: $(curl -sk --http2 --max-time 5 https://127.0.0.1:$PORT/sse-cadence)"
      gdb -batch -p $P -ex "set pagination off" -ex "thread apply all bt 40" > stacks-r$r-$rank.txt 2>&1
      echo "stacks: $D/stacks-r$r-$rank.txt ($(grep -c '^Thread ' stacks-r$r-$rank.txt) threads)"
      sleep 25; wait $C 2>/dev/null; grep -E "streams=|NO EVENTS" client.out
      kill -9 $P; wait $P 2>/dev/null
      exit 0
    fi
    wait $C
    echo "r$r rank=$rank: normal $(grep -E 'streams=' client.out | sed 's/.*opened=/opened=/')"
    kill $P; wait $P 2>/dev/null
  done
done
echo "no collapse in $ROUNDS rounds"
