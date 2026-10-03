#!/bin/sh
# Live inline pile-up check per balancer rank, on nachos, after
# run-rank-ab.sh has shipped its binaries to /tmp/starh2-rank-ab.
#
#   ssh ryan@nachos.trex-elevator.ts.net 'bash -s' < captures/placement-followup/defects-live-ranks.sh
#
# 2 executors, --sse-interval-ms 1000 (one mostly-sleeping SSE handler),
# then h2load opens 50 one-shot connections. A rank that ranks handlers first
# puts every one-shot connection on the executor WITHOUT the SSE handler and
# halves throughput. PL (no balancing) is the reference. 3 rounds, order
# rotated. Per-thread utime+stime shows where the one-shot work ran.
set -u
D=/tmp/starh2-rank-ab
cd $D
H2LOAD=/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load
uptime
threads() { for t in /proc/$1/task/*; do sed 's/^.*) //' $t/stat 2>/dev/null | awk '{print $12+$13}'; done | sort -rn | head -3 | tr '\n' ',' | sed 's/,$//'; }
for r in 1 2 3; do
  case $r in 1) order="PL PLC PLH PLS";; 2) order="PLS PLH PLC PL";; 3) order="PLH PL PLS PLC";; esac
  for arm in $order; do
    case $arm in
      PL) extra="--spawn-placement local";;
      PLC) extra="--spawn-placement local --conn-balance --balance-rank connections_first";;
      PLH) extra="--spawn-placement local --conn-balance --balance-rank handlers_first";;
      PLS) extra="--spawn-placement local --conn-balance --balance-rank sum";;
    esac
    ./PLC-server --mode tls --port 0 --executors 2 --sse-interval-ms 1000 --cert cert.pem --key key.pem $extra > live.log 2>&1 &
    P=$!; sleep 0.5
    PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' live.log | head -1)
    rank=$(sed -n 's/.*"balance_rank":"\([a-z_]*\)".*/\1/p' live.log | head -1)
    ./client -url https://127.0.0.1:$PORT/sse -streams 1 -seconds 12 -warmup 0 -label sse > sse.out 2>&1 &
    C=$!; sleep 1
    out=$($H2LOAD -n 2000000 -c 50 -m 10 -t 4 https://127.0.0.1:$PORT/ 2>&1 | grep -E "finished in|requests:" | tr '\n' ' ')
    echo "i$r $arm rank=$rank $out threads=$(threads $P)"
    kill $C 2>/dev/null; kill $P; wait $P 2>/dev/null
  done
done
