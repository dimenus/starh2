#!/bin/sh
# t-2501: per-thread CPU during SSE at 500 streams on ONE TLS connection, 2
# executors, 1 ms interval, 10 s (the recheck's sse500/cpu500 shape), for PL
# (0299e57 pinned, --spawn-placement local), PA (same binary, auto) and WSL
# (0299e57 work_stealing), all on the 1080022 tree.
#
#   captures/zio-t2501-perthread/run.sh > rows.txt
#
# Uses the binaries already shipped to $D by
# captures/zio-placement-recheck-0299e57 (sha256 re-checked here).
#
# # Rounds and order
#
# 30 rounds. PL and PA run in every round; WSL runs in every third. Two-arm
# rounds alternate PL,PA / PA,PL; three-arm rounds step through all 6 orders
# (so over rounds 3..30 each order of the three runs twice... 10 WSL rounds
# cover the 6 orders once and 4 of them twice).
#
# # What is sampled
#
# Every 200 ms while the client runs, every task of the server process:
# tid, comm, utime+stime (/proc/<pid>/task/<tid>/stat, fields counted after
# the ") <state> " that ends comm). The executors are the two threads named
# like the binary; tid == pid is the process main thread. io_uring worker
# threads (iou-wrk-*) belong to the process and are counted too. Each row
# prints, per thread: mean and peak % of one core over the window, and the
# share of 200 ms samples at >= 95%. Raw series go to $D/series-<round>-<arm>
# and are copied back.
#
# Knee: all 500 streams delivered, and SSE p50 above 200 us.
set -eu
HOST=${HOST:-ryan@100.113.184.27}
D=${D:-/tmp/zio-placement-recheck}
ROUNDS=${ROUNDS:-30}
OUT=${OUT:-$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)/series}
mkdir -p "$OUT"
ssh "$HOST" "D='$D' ROUNDS=$ROUNDS sh -s" <<'REMOTE'
set -u
echo "== host"; uname -r; uptime
for pair in PL=dda3fd21a77d233ae3114192ade156dc1ff2822db31b6a01722ecdb5bf6c6806 \
            PA=dda3fd21a77d233ae3114192ade156dc1ff2822db31b6a01722ecdb5bf6c6806 \
            WSL=73b2a7c1fc88e6769975f19a6f5612dd4d43e6581f6aa9be57374ba8bf0be915; do
  a=${pair%%=*}; want=${pair#*=}; got=$(sha256sum $D/$a-server | cut -d' ' -f1)
  [ "$got" = "$want" ] || { echo "sha mismatch $a $got" >&2; exit 1; }
  echo "sha $a $got"
done
args() { case $1 in PL) echo "--spawn-placement local";; PA) echo "--spawn-placement auto";; *) echo "";; esac; }
expect() { case $1 in
  PL) echo '"zio_scheduling":"pinned","spawn_placement":"local"';;
  PA) echo '"zio_scheduling":"pinned","spawn_placement":"auto"';;
  WSL) echo '"zio_scheduling":"work_stealing","spawn_placement":"auto"';; esac; }

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

sample() { # pid outfile
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

run_one() { # round arm
  R=$1; A=$2; rm -f $D/$A.log
  $D/$A-server --mode tls --port 0 --executors 2 --sse-interval-ms 1 \
    --cert $D/cert.pem --key $D/key.pem $(args $A) > $D/$A.log 2>&1 &
  P=$!; i=0; PORT=
  while [ $i -lt 200 ]; do PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' $D/$A.log | head -1); [ -n "$PORT" ] && break; i=$((i+1)); sleep 0.05; done
  ready=$(grep '"ready"' $D/$A.log | head -1)
  case "$ready" in *"$(expect $A)"*) ;; *) echo "READY-MISMATCH $A: $ready" >&2; kill $P; exit 4;; esac
  S=$D/series-$R-$A
  sample $P $S & SP=$!
  out=$(timeout 180 $D/client -url https://127.0.0.1:$PORT/sse -streams 500 -seconds 10 -warmup 1 -label $A 2>&1)
  kill $P; wait $P 2>/dev/null; wait $SP 2>/dev/null
  line=$(echo "$out" | grep 'events=' | sed 's/^ *[A-Z]* *//')
  lat=$(echo "$out" | grep 'sse latency' | sed 's/.*latency //')
  thr=$(awk -v pid=$P '
    { t = $1; tid = $2; name = $3; v = $4
      if (tid in last) { dt = t - lt[tid]; if (dt > 0) { pc = (v - last[tid]) / 100 / dt * 100
          n[tid]++; s[tid] += pc; if (pc > pk[tid]) pk[tid] = pc; if (pc >= 95) hi[tid]++ } }
      last[tid] = v; lt[tid] = t; nm[tid] = name; if (!(tid in first)) { first[tid] = v; ft[tid] = t } }
    END { for (x in nm) { if (n[x] == 0) continue
        tag = (x == pid) ? "main" : nm[x]
        printf "%s:%s:mean=%.0f:peak=%.0f:ge95=%d/%d ", x, tag, s[x]/n[x], pk[x], hi[x]+0, n[x] } }' $S)
  f=$(echo "$line" | sed -n 's/.*failed=\([0-9]*\).*/\1/p')
  p50=$(echo "$lat" | sed -n 's/p50=\([^ ]*\).*/\1/p')
  p50us=$(echo "$p50" | awk '/ms$/{sub(/ms/,"");print $0*1000;next} /µs$/{sub(/µs/,"");print $0+0;next} /s$/{sub(/s/,"");print $0*1e6;next} {print -1}')
  cls=ok; [ "${f:-1}" != 0 ] && cls=failclosed
  [ "$cls" = ok ] && awk -v p=$p50us 'BEGIN{exit !(p>200)}' && cls=knee
  echo "k$R $A $cls $line | $lat | threads $thr"
}

r=1
while [ $r -le $ROUNDS ]; do
  host_ok "round $r"
  if [ $((r % 3)) -eq 0 ]; then
    case $(( (r / 3 - 1) % 6 )) in
      0) O="PL PA WSL";; 1) O="WSL PA PL";; 2) O="PA WSL PL";; 3) O="PL WSL PA";; 4) O="WSL PL PA";; 5) O="PA PL WSL";; esac
  elif [ $((r % 2)) -eq 1 ]; then O="PL PA"; else O="PA PL"; fi
  for a in $O; do run_one $r $a; done
  r=$((r+1))
done
echo "== done"
REMOTE
rc=$?
scp -q "$HOST:$D/series-*" "$OUT/" && echo "series copied: $(ls "$OUT" | wc -l)"
exit $rc
