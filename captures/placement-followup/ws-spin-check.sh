#!/bin/sh
# Work-stealing spin check for the t-2655 TLS actor spin, on nachos.
#
#   ssh ryan@nachos.trex-elevator.ts.net 'ROUNDS=120 bash -s' < ws-spin-check.sh
#
# Two work-stealing builds, both with the c9ef010 spin detector and
# --diag-stuck: PRE = c9ef010 (before the 082e794 fix), FIX = bb1e475. Same
# collapse shape as collapse-hunt.sh (12 executors, TLS, 6 heavy x 250
# streams, 12 light x 10, 4 unpaced churn workers). Rank none only: the
# balancer refuses work-stealing builds (ConnBalanceNeedsPinnedBuild). The
# build order alternates every round, so host drift hits both builds alike. Unlike the hunt, it
# never stops on a stall: a client still running at 25 s is killed and
# counted. Per row: build, rank, NOPARK lines (100k / 10M turns without a
# park), server CPU seconds (utime+stime), the client's stream, latency and
# churn lines.
set -u
D=${D:-/tmp/starh2-collapse}
ROUNDS=${ROUNDS:-120}
PRE=${PRE:-/tmp/starh2-wsspin/out-c9ef010/bin/starh2-bench-server}
FIX=${FIX:-/tmp/starh2-wsspin/out-bb1e475/bin/starh2-bench-server}
cd $D
uptime
echo "PRE $(sha256sum $PRE)"; echo "FIX $(sha256sum $FIX)"
tck=$(getconf CLK_TCK)
for r in $(seq 1 $ROUNDS); do
  if [ $((r % 2)) = 1 ]; then order="PRE FIX"; else order="FIX PRE"; fi
  for build in $order; do
    for rank in none; do
      bin=$PRE; [ $build = FIX ] && bin=$FIX
      extra="--spawn-placement auto"
      [ "$rank" = none ] || extra="$extra --conn-balance --balance-rank $rank"
      $bin --mode tls --port 0 --sse-interval-ms 1 --cert cert.pem --key key.pem --diag-stuck $extra > srv.log 2>&1 &
      P=$!
      for i in $(seq 100); do PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' srv.log | head -1); [ -n "$PORT" ] && break; sleep 0.05; done
      sched=$(sed -n 's/.*"zio_scheduling":"\([a-z_]*\)".*/\1/p' srv.log | head -1)
      E=$(sed -n 's/.*"executors":\([0-9]*\).*/\1/p' srv.log | head -1)
      timeout 25 ./client -url https://127.0.0.1:$PORT/sse -streams $((E / 2 * 250)) -conns $((E / 2)) -light-conns $E -light-streams 10 \
        -churn-workers 4 -churn-url https://127.0.0.1:$PORT/ -stagger-ms 50 -interval-ms 1 -seconds 10 -warmup 1 -label r$r > client.out 2>&1
      crc=$?
      cpu=$(awk -v t=$tck '{ printf "%.1f", ($14 + $15) / t }' /proc/$P/stat 2>/dev/null)
      np100k=$(grep -c "NOPARK.*run=100000 " srv.log); np10m=$(grep -c "NOPARK.*run=10000000 " srv.log)
      kill -9 $P 2>/dev/null; wait $P 2>/dev/null
      stalled=0; [ $crc = 124 ] && stalled=1
      echo "ROW r=$r build=$build sched=$sched rank=$rank stalled=$stalled nopark100k=$np100k nopark10m=$np10m server_cpu_s=$cpu"
      grep NOPARK srv.log | head -4 | sed 's/^/  /'
      grep -E "streams=|sse latency|heavy_worst|churn conns" client.out | sed -E 's/\[port:kind.*//; s/^/  /'
    done
  done
done
echo "done $ROUNDS rounds"
