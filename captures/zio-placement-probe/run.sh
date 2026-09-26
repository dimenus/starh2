#!/bin/sh
# Placement probe: which executor thread runs each kind of server task while
# SSE 500 streams run on ONE TLS connection (2 executors, 1 ms, 10 s), under
# pinned scheduling with --spawn-placement local.
#
#   BIN=<probe binary> captures/zio-placement-probe/run.sh > rows.txt
#   MODE=balance BIN=<probe binary> ... > balance-rows.txt   # N against B
#     (B = N plus --conn-balance, the load-aware placement experiment)
#
# The probe binary is the starh2/zio-placement-probe tree built with
#   -Doptimize=ReleaseFast -Dzio-scheduling=pinned -Dobserve=true
#   -Dtarget=x86_64-linux-musl
# so connection.placement_check (and the probe counters) are compiled in.
# Every observe-only counter it adds is also in that build, so it is not the
# release PL binary; its knee rate is compared with t-2501's below.
#
# # Arms
#
#   N  natural: --spawn-placement local (30 rounds)
#   S  forced co-location: SSE handlers spawned on the actor's executor
#      (--probe-handler-placement same), 10 rounds
#   O  forced separation: SSE handlers on the other executor
#      (--probe-handler-placement other), 10 rounds
# N runs every round; S and O run in every third round, stepping through all
# 6 orders of the three arms.
#
# # Per run
#
# - /trace, fetched just before the kill: per kernel tid, how many times each
#   task kind ran there (accept loop turn, connEntry, actor start, actor loop
#   turn, handler start, SSE handler loop iteration, reaper job, TLS recv and
#   send submits). tid == probe_pid is executor 0 (zio's main executor).
# - Per-thread CPU every 200 ms from /proc/<pid>/task/*/stat (t-2501 method).
# - SSE events, failed, p50/p99. Knee: all 500 delivered and p50 > 200 us.
set -eu
HOST=${HOST:-ryan@100.113.184.27}
D=${D:-/tmp/zio-placement-probe}
ROUNDS=${ROUNDS:-30}
BIN=${BIN:?BIN is required}
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
OUT=${OUT:-$HERE/series}
SHA=$(shasum -a 256 "$BIN" | cut -d' ' -f1)
mkdir -p "$OUT"
ssh "$HOST" "mkdir -p $D"
scp -q "$BIN" "$HOST:$D/PROBE-server"
ssh "$HOST" "cp /tmp/zio-placement-recheck/client /tmp/zio-placement-recheck/cert.pem /tmp/zio-placement-recheck/key.pem $D/"
echo "local sha256 $SHA"
ssh "$HOST" "D='$D' ROUNDS=$ROUNDS SHA=$SHA MODE=${MODE:-probe} sh -s" <<'REMOTE'
set -u
chmod +x $D/PROBE-server $D/client
echo "== host"; uname -r; uptime
got=$(sha256sum $D/PROBE-server | cut -d' ' -f1)
[ "$got" = "$SHA" ] || { echo "sha mismatch $got" >&2; exit 1; }
echo "sha PROBE $got"
force() { case $1 in N|B) echo none;; C) echo same;; P) echo split;; esac; }
bal() { case $1 in B) echo "--conn-balance";; *) echo "";; esac; }

host_ok() {
  w=0
  while :; do
    s1=$(awk '/^cpu /{print $2+$3+$4+$7+$8+$9, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat); sleep 5
    s2=$(awk '/^cpu /{print $2+$3+$4+$7+$8+$9, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat)
    busy=$(echo "$s1 $s2" | awk -v n=$(nproc) '{printf "%.2f", ($3-$1)/($4-$2)*n}')
    blk=$(pgrep -l -x 'WowB.exe|Wow.exe|cc1plus|cc1|mod-tests|zig' | tr '\n' ' ')
    echo "host busy_cores=$busy blocking=[$blk] before $1"
    if [ -z "$blk" ] && awk -v b=$busy 'BEGIN{exit !(b<=0.5)}'; then return 0; fi
    [ $w -ge 14400 ] && { echo "HOST-BUSY stop" >&2; exit 3; }
    echo "HOST-BUSY-WAIT before $1"; sleep 30; w=$((w+30))
  done
}

sample() {
  while kill -0 $1 2>/dev/null; do
    t=$(date +%s.%N)
    for f in /proc/$1/task/*/stat; do
      l=$(cat $f 2>/dev/null) || continue
      tid=${l%% *}; comm=${l#*(}; comm=${comm%)*}; rest=${l##*) }
      echo "$t $tid $comm $(echo "$rest" | awk '{print $12+$13}')"
    done
    sleep 0.2
  done > $2
}

run_one() {
  R=$1; A=$2; F=$(force $A); rm -f $D/$A.log
  $D/PROBE-server --mode tls --port 0 --executors 2 --sse-interval-ms 1 \
    --cert $D/cert.pem --key $D/key.pem --spawn-placement local --probe-conn-placement $F $(bal $A) > $D/$A.log 2>&1 &
  P=$!; i=0; PORT=
  while [ $i -lt 200 ]; do PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' $D/$A.log | head -1); [ -n "$PORT" ] && break; i=$((i+1)); sleep 0.05; done
  ready=$(grep '"ready"' $D/$A.log | head -1)
  want="\"zio_scheduling\":\"pinned\",\"spawn_placement\":\"local\",\"probe\":1,\"probe_handler_placement\":\"none\",\"probe_conn_placement\":\"$F\""
  case "$ready" in *"$want"*) ;; *) echo "READY-MISMATCH $A: $ready" >&2; kill $P; exit 4;; esac
  cb='"conn_balance":0'; [ $A = B ] && cb='"conn_balance":1'
  case "$ready" in *"$cb"*) ;; *) echo "READY-MISMATCH $A (conn_balance): $ready" >&2; kill $P; exit 4;; esac
  S=$D/series-$R-$A
  sample $P $S & SP=$!
  timeout 180 $D/client -url https://127.0.0.1:$PORT/sse -streams 500 -seconds 10 -warmup 1 -label $A > $D/client.out 2>&1 &
  CP=$!
  # The server limits a connection to 256 concurrent streams, so the Go
  # client opens a second TCP connection for streams 257-500. Count them.
  sleep 5
  nconn=$(ss -tnH state established "( sport = :$PORT )" | wc -l)
  wait $CP
  out=$(cat $D/client.out)
  tr=$(curl -sk --http2 https://127.0.0.1:$PORT/trace 2>/dev/null | sed -n 's/.*"probe_pid":\([0-9]*\),"probe_overflow":\([0-9]*\),"probe":\(\[[^]]*\]\).*/pid=\1 overflow=\2 probe=\3/p')
  kill $P; wait $P 2>/dev/null; wait $SP 2>/dev/null
  line=$(echo "$out" | grep 'events=' | sed 's/^ *[A-Z]* *//')
  lat=$(echo "$out" | grep 'sse latency' | sed 's/.*latency //')
  thr=$(awk -v pid=$P '
    { t = $1; tid = $2; name = $3; v = $4
      if (tid in last) { dt = t - lt[tid]; if (dt > 0) { pc = (v - last[tid]) / 100 / dt * 100
          n[tid]++; s[tid] += pc; if (pc > pk[tid]) pk[tid] = pc; if (pc >= 95) hi[tid]++ } }
      last[tid] = v; lt[tid] = t; nm[tid] = name }
    END { for (x in nm) { if (n[x] == 0) continue
        tag = (x == pid) ? "main" : nm[x]
        printf "%s:%s:mean=%.0f:peak=%.0f:ge95=%d/%d ", x, tag, s[x]/n[x], pk[x], hi[x]+0, n[x] } }' $S)
  f=$(echo "$line" | sed -n 's/.*failed=\([0-9]*\).*/\1/p')
  p50=$(echo "$lat" | sed -n 's/p50=\([^ ]*\).*/\1/p')
  p50us=$(echo "$p50" | awk '/ms$/{sub(/ms/,"");print $0*1000;next} /µs$/{sub(/µs/,"");print $0+0;next} /s$/{sub(/s/,"");print $0*1e6;next} {print -1}')
  cls=ok; [ "${f:-1}" != 0 ] && cls=failclosed
  [ "$cls" = ok ] && awk -v p=$p50us 'BEGIN{exit !(p>200)}' && cls=knee
  echo "k$R $A $cls tcp_conns=$nconn $line | $lat | threads $thr | ${tr:-NO-PROBE}"
}

r=1
while [ $r -le $ROUNDS ]; do
  host_ok "round $r"
  if [ $((r % 3)) -eq 0 ]; then
    case $(( (r / 3 - 1) % 6 )) in
      0) O="N C P";; 1) O="P C N";; 2) O="C P N";; 3) O="N P C";; 4) O="P N C";; 5) O="C N P";; esac
  else O="N"; fi
  # MODE=balance: natural (N) against --conn-balance (B), alternating order.
  if [ "$MODE" = balance ]; then if [ $((r % 2)) -eq 1 ]; then O="N B"; else O="B N"; fi; fi
  for a in $O; do run_one $r $a; done
  r=$((r+1))
done
echo "== done"
REMOTE
rc=$?
scp -q "$HOST:$D/series-*" "$OUT/" && echo "series copied: $(ls "$OUT" | wc -l)"
exit $rc
