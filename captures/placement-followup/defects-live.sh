#!/bin/sh
# Live checks of two --conn-balance defects on nachos, with the session's
# pinned binary (PL and PLB are the same binary; only --conn-balance differs).
#
#   ssh ryan@nachos.trex-elevator.ts.net 'bash -s' < captures/placement-followup/defects-live.sh > defects-live.txt
#
# 1. Width cap. 20 executors, 20 heavy connections (100 streams each, opened
#    50 ms apart so each is visible to the balancer before the next arrives).
#    With a 16-slot table, PLB can place connections only on executors 0..15,
#    so 4 executor threads should stay idle; PL (round-robin) uses all 20.
#    Per-thread utime+stime is read before the kill; executor threads are the
#    busiest 20.
# 2. Inline pile-up. 2 executors, --sse-interval-ms 1000: one SSE stream is a
#    handler that sleeps almost all the time. Then h2load opens 50 one-shot
#    connections. The balancer ranks by live task handlers first, so under
#    PLB every one-shot connection should go to the executor WITHOUT the SSE
#    handler, halving one-shot capacity against PL.
set -u
D=/tmp/starh2-placement-followup
cd $D
H2LOAD=/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load
uptime
threads() { for t in /proc/$1/task/*; do sed 's/^.*) //' $t/stat 2>/dev/null | awk '{print $12+$13}'; done | sort -rn | tr '\n' ',' | sed 's/,$//'; }
start() { ./PL-server --mode tls --port 0 --cert cert.pem --key key.pem "$@" > live.log 2>&1 & P=$!; sleep 0.5; PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' live.log | head -1); }

echo "== 1. width cap: 20 executors, 20 heavy connections x 100 streams =="
for r in 1 2 3; do
  for arm in PL PLB; do
    extra="--spawn-placement local"; [ $arm = PLB ] && extra="$extra --conn-balance"
    start --executors 20 --sse-interval-ms 1 $extra
    ./client-linux-v2 -url https://127.0.0.1:$PORT/sse -streams 2000 -conns 20 -stagger-ms 50 -interval-ms 1 -seconds 8 -warmup 1 -label $arm 2>&1 | grep -E "streams=" | sed "s/^/w$r /"
    th=$(threads $P)
    idle=$(echo "$th" | tr ',' '\n' | head -20 | awk '$1 < 50 { n++ } END { print n + 0 }')
    echo "w$r $arm threads=$th executor_threads_under_0.5s_cpu=$idle"
    kill $P; wait $P 2>/dev/null
  done
done

echo "== 2. inline pile-up: 2 executors, one sleeping SSE handler, then 50 one-shot connections =="
for r in 1 2 3; do
  for arm in PL PLB; do
    extra="--spawn-placement local"; [ $arm = PLB ] && extra="$extra --conn-balance"
    start --executors 2 --sse-interval-ms 1000 $extra
    ./client-linux-v2 -url https://127.0.0.1:$PORT/sse -streams 1 -seconds 12 -warmup 0 -label sse > sse.out 2>&1 &
    C=$!; sleep 1
    before=$(threads $P)
    out=$($H2LOAD -n 2000000 -c 50 -m 10 -t 4 https://127.0.0.1:$PORT/ 2>&1 | grep -E "finished in|requests:" | tr '\n' ' ')
    echo "i$r $arm oneshot-with-idle-sse $out threads_before=$before threads_after=$(threads $P)"
    kill $C 2>/dev/null; kill $P; wait $P 2>/dev/null
  done
done
