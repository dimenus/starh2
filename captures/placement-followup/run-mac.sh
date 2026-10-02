#!/bin/sh
# The placement follow-up session on this Mac (kqueue), t-2502.
#
#   WORK=<dir from build.sh with TARGET=native> captures/placement-followup/run-mac.sh > mac-rows.txt
#
# Same arms, phases and row format as run-linux.sh (tools/zio-arm-ab.sh), so
# summarize.py reads both. What differs, and why:
# - It runs locally: client, h2load and server share this machine, as they
#   share nachos there.
# - CPU comes from `ps`, not /proc: process `cputime` (converted to 1/100 s
#   ticks, tck=100) and, for the mix rows, per-thread utime+stime from
#   `ps -M`.
# - The idle check reads the busy cores from two `top -l` samples, and the
#   busiest other process from `ps`. The Mac is a working machine, so every
#   round checks, and a busy host is waited out (every wait is printed).
# - kqueue: on this backend a socket is registered with the loop that first
#   touches it (sockreg), which io_uring does not do. That is one reason the
#   placement story may differ here.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}
T=native
ARMS=${ARMS:-WS WS2 WSH PA PL PLB}
PHASES=${PHASES:-1 2 3 5 6}
PERF_ROUNDS=${PERF_ROUNDS:-12}
BURST_ROUNDS=${BURST_ROUNDS:-24}
CPU_ROUNDS=${CPU_ROUNDS:-12}
OPEN_ROUNDS=${OPEN_ROUNDS:-12}
MIX_ROUNDS=${MIX_ROUNDS:-12}
OPEN_RATES=${OPEN_RATES:?OPEN_RATES is required: offered totals at 2 executors, from a pilot}
MIX_WIDTHS=${MIX_WIDTHS:-8 prod}
# heavy connections = executors / MIX_HEAVY_DIV; MIX_LABEL names the rows, so
# two levels (mix = /2, mixm = /4) can share one rows file.
MIX_HEAVY_DIV=${MIX_HEAVY_DIV:-2}
MIX_LABEL=${MIX_LABEL:-mix}
# h2load with --log-file is client-bound here: 280k req/s at -t 4 and 140k
# at -t 12, against a 666k server (measured). So open loop runs at -t 4 and at
# rates the client holds (p50 104 us at 100k; 544 us at 200k is the client).
OPEN_THREADS=${OPEN_THREADS:-4}
IDLE_MAX_CORES=${IDLE_MAX_CORES:-1.5}
# A round starts only when the 1-minute load average is also under LOAD_MAX.
LOAD_MAX=${LOAD_MAX:-1.5}
# During each round a sampler sums the CPU of every process that is not the
# bench server, the client or h2load, every 3 s; above OTHER_MAX_PCT in total
# or PROC_MAX_PCT for one process the round is dirty: its rows are dropped
# and the same round runs again, so only idle rounds are kept and the
# rotation stays balanced.
OTHER_MAX_PCT=${OTHER_MAX_PCT:-150}
PROC_MAX_PCT=${PROC_MAX_PCT:-50}
HOST_WAIT_MAX=${HOST_WAIT_MAX:-14400}
SECONDS_RUN=10
INTERVAL=1
H2LOAD=${H2LOAD:-/opt/homebrew/bin/h2load}
D=${D:-$WORK/mac-run}

ARGS_WS='' ARGS_WS2='' ARGS_WSH='--spawn-placement prefer_local' ARGS_PA='--spawn-placement auto'
ARGS_PL='--spawn-placement local' ARGS_PLB='--spawn-placement local --conn-balance'
tail_='"probe":0,"probe_handler_placement":"none","probe_conn_placement":"none"'
# Ready lines of the current tree end with balance_rank and placement_log;
# PLB runs the default rank (sum).
nb='"conn_balance":0,"balance_rank":"none","placement_log":0}'
EXPECT_WS='"zio_scheduling":"work_stealing","spawn_placement":"auto",'"$tail_,$nb"
EXPECT_WS2=$EXPECT_WS
EXPECT_WSH='"zio_scheduling":"work_stealing","spawn_placement":"prefer_local",'"$tail_,$nb"
EXPECT_PA='"zio_scheduling":"pinned","spawn_placement":"auto",'"$tail_,$nb"
EXPECT_PL='"zio_scheduling":"pinned","spawn_placement":"local",'"$tail_,$nb"
EXPECT_PLB='"zio_scheduling":"pinned","spawn_placement":"local",'"$tail_"',"conn_balance":1,"balance_rank":"sum","placement_log":0}'
bin_for() { case "$1" in WS|WS2|WSH) echo "$WORK/$T-work_stealing/bin/starh2-bench-server";; *) echo "$WORK/$T-pinned/bin/starh2-bench-server";; esac; }

mkdir -p "$D"
command -v "$H2LOAD" > /dev/null || { echo "h2load missing at $H2LOAD; a missing client reads as a server wedge" >&2; exit 1; }
(cd "$REPO/tools/sse_bench" && go build -o "$D/client" ./client.go)
echo "== arm identity =="
for arm in $ARMS; do
  b=$(bin_for $arm)
  # A Mach-O ReleaseFast binary embeds no zio package path, so the Linux
  # package check cannot run here. The ready line is the proof instead: it
  # must name the arm's scheduling and placement on every start, and
  # "prefer_local" exists only in the patched zio this tree pins.
  eval "a=\$ARGS_$arm"
  echo "  $arm sha256=$(shasum -a 256 "$b" | cut -d' ' -f1) args='$a'"
done
echo "== host =="
uname -r; echo "physical_cpus=$(sysctl -n hw.physicalcpu) $(sysctl -n machdep.cpu.brand_string)"; uptime
echo "h2load: $H2LOAD $("$H2LOAD" --version 2>&1 | head -1)"

# busy cores over a 2 s top sample (the second sample; the first is since boot)
host_check() {
  waited=0
  while :; do
    idle=$(top -l 2 -s 2 -n 0 | awk '/^CPU usage/ { v = $7 } END { sub(/%/, "", v); print v }')
    ncpu=$(sysctl -n hw.logicalcpu)
    busy=$(awk -v i="$idle" -v n="$ncpu" 'BEGIN { printf "%.2f", (100 - i) / 100 * n }')
    top1=$(ps -Ao pcpu=,pid=,comm= -r | head -1 | awk '{ printf "%s %s %s", $1, $2, $3 }')
    tp=${top1%% *}
    load1=$(sysctl -n vm.loadavg | awk '{ print $2 }')
    echo "busy_cores=$busy limit=$IDLE_MAX_CORES top_process=$top1 limit_pct=$PROC_MAX_PCT load1=$load1 limit=$LOAD_MAX before $1"
    if ! awk -v b="$busy" -v m="$IDLE_MAX_CORES" -v p="$tp" -v pm="$PROC_MAX_PCT" -v l="$load1" -v lm="$LOAD_MAX" 'BEGIN { exit !(b > m || p > pm || l > lm) }'; then return 0; fi
    if [ $waited -ge $HOST_WAIT_MAX ]; then echo "HOST-BUSY before $1; stopping, not measuring" >&2; exit 3; fi
    echo "HOST-BUSY-WAIT before $1: waiting 30 s (waited ${waited}s so far)"
    sleep 30; waited=$((waited + 30))
  done
}
# Background sampler of the CPU used by everything except this benchmark.
# "This benchmark" is this script's process group: the server, the client,
# h2load, and the helpers the harness runs mid-round (the percentile
# `sort`, `ps`, `awk`). Matching helpers by name instead missed `sort`,
# which then discarded nearly every round as busy, and would have hidden
# another job's `sh` or `awk`. The group is only ours when the script runs
# in its own session (start it with
# `perl -e 'use POSIX; setsid(); exec @ARGV' sh run-mac.sh`), so a group
# shared with anything else stops the run instead of hiding that load.
OWN_PGID=$(ps -o pgid= -p $$ | tr -d ' ')
if ps -Ao pgid=,comm= | awk -v g="$OWN_PGID" '$1 == g && $2 !~ /(^|\/)(sh|bash|zsh|run-mac[.]sh|overnight[.]sh|perl|ps|awk|tr|nohup)$/ { found = 1 } END { exit !found }'; then
  echo "run-mac.sh: process group $OWN_PGID holds other processes; start it in its own session (setsid)" >&2
  ps -Ao pid=,pgid=,comm= | awk -v g="$OWN_PGID" '$2 == g' >&2
  exit 4
fi
other_cpu() {
  ps -Ao pcpu=,pgid=,comm= | awk -v g="$OWN_PGID" '
    $2 == g { next }
    { t += $1; if ($1 > m) { m = $1; who = $3 } }
    END { printf "%.0f %.0f %s\n", t, m, who }'
}
round_begin() {
  host_check "$1"
  : > "$D/round.dirty"
  touch "$D/sampling"
  (
    while [ -f "$D/sampling" ]; do
      set -- $(other_cpu)
      if [ "${1%.*}" -gt "$OTHER_MAX_PCT" ] || [ "${2%.*}" -gt "$PROC_MAX_PCT" ]; then echo "other_cpu=$1% top=$2% ($3)" >> "$D/round.dirty"; fi
      sleep 3
    done
  ) &
  SAMPLER=$!
}
# True when the round stayed idle; otherwise reports and asks for a rerun.
round_end() {
  rm -f "$D/sampling"; wait $SAMPLER 2>/dev/null
  if [ -s "$D/round.dirty" ]; then
    echo "DISCARDED-ROUND $1: $(head -1 "$D/round.dirty") (rows dropped; running it again)"
    return 1
  fi
  return 0
}
has_phase() { case " $PHASES " in *" $1 "*) return 0;; esac; return 1; }

start_srv() {
  ARM=$1; EXEC=$2
  eval "extra=\$ARGS_$ARM; expect=\$EXPECT_$ARM"
  execarg="--executors $EXEC"; [ "$EXEC" = prod ] && execarg=""
  rm -f "$D/$ARM.log"
  # shellcheck disable=SC2086
  "$(bin_for $ARM)" --mode tls --port 0 $execarg --sse-interval-ms $INTERVAL \
    --cert "$REPO/testdata/cert.pem" --key "$REPO/testdata/key.pem" $extra > "$D/$ARM.log" 2>&1 &
  SRV_PID=$!
  i=0; SRV_PORT=
  while [ $i -lt 200 ]; do
    SRV_PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' "$D/$ARM.log" 2>/dev/null | head -1)
    [ -n "$SRV_PORT" ] && break
    i=$((i + 1)); sleep 0.05
  done
  [ -n "$SRV_PORT" ] || { echo "NO-READY-LINE arm=$ARM"; return 0; }
  ready=$(grep '"ready"' "$D/$ARM.log" | head -1)
  case "$ready" in *"$expect"*) ;; *) echo "READY-MISMATCH arm=$ARM expected '$expect' in: $ready" >&2; kill $SRV_PID; exit 4;; esac
  eval "shown=\${SHOWN_$ARM:-}"
  [ -n "$shown" ] || { echo "  ready $ARM args='$extra' $ready"; eval "SHOWN_$ARM=1"; }
}
stop_srv() { kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null || true; }
# ps cputime is [dd-]hh:mm:ss.ss or mm:ss.ss; print 1/100 s ticks.
cpu_ticks() { ps -o cputime= -p "$1" 2>/dev/null | awk '{ n = split($1, p, ":"); s = 0; for (i = 1; i <= n; i++) s = s * 60 + p[i]; printf "%d", s * 100 }'; }
# per-thread utime+stime in 1/100 s ticks, largest first
thread_ticks() {
  # Thread lines after the first carry no USER or COMMAND, so fields are
  # found by shape: the first two m:ss.ss fields are STIME and UTIME.
  ps -M -p "$1" 2>/dev/null | awk 'NR > 1 { t = 0; k = 0; for (f = 1; f <= NF && k < 2; f++) if ($f ~ /^[0-9]+:[0-9:]*[0-9]+\.[0-9]+$/) { n = split($f, p, ":"); s = 0; for (i = 1; i <= n; i++) s = s * 60 + p[i]; t += s; k++ } printf "%d\n", t * 100 }' | sort -rn | tr '\n' ',' | sed 's/,$//'
}

rotate() {
  R=$1; LIST=${2:-$ARMS}; set -- $LIST; n=$#; k=$(( (R - 1) % n )); i=0; ORDER=""
  while [ $i -lt $n ]; do
    idx=$(( (k + i) % n + 1 )); j=1
    for a in $LIST; do [ $j -eq $idx ] && ORDER="$ORDER $a"; j=$((j + 1)); done
    i=$((i + 1))
  done
  if [ $n -gt 2 ] && [ $(( ((R - 1) / n) % 2 )) -eq 1 ]; then
    rev=""; for a in $ORDER; do rev="$a $rev"; done; ORDER=$rev
  fi
}

oneshot() {
  ARM=$1; EXEC=$2; N=$3
  start_srv $ARM $EXEC
  if [ -n "$SRV_PORT" ]; then
    out=$("$H2LOAD" -n $N -c 50 -m 10 -t 4 https://127.0.0.1:$SRV_PORT/ 2>&1) && rc=0 || rc=$?
    if [ $rc -ne 0 ]; then echo "r$r $ARM oneshot-e$EXEC WEDGE-OR-FAIL rc=$rc"
    else echo "$out" | grep -E 'finished in|requests:' | tr '\n' ' ' | sed "s/^/r$r $ARM oneshot-e$EXEC /"; echo; fi
  fi
  stop_srv
}
h2lat() {
  ARM=$1; EXEC=$2; LABEL=$3; EXTRA=$4; shift 4
  start_srv $ARM $EXEC
  if [ -n "$SRV_PORT" ]; then
    rm -f "$D/lat.tsv"
    out=$("$H2LOAD" "$@" --log-file="$D/lat.tsv" https://127.0.0.1:$SRV_PORT/ 2>&1) && rc=0 || rc=$?
    if [ $rc -ne 0 ]; then echo "r$r $ARM $LABEL WEDGE-OR-FAIL rc=$rc"
    else
      non200=$(awk -F'\t' '$2 != 200' "$D/lat.tsv" | wc -l | tr -d ' ')
      pct=$(awk -F'\t' '$2 == 200 {print $3}' "$D/lat.tsv" | sort -n | awk '
        { a[NR] = $1 }
        END {
          if (NR == 0) { print "n=0"; exit }
          i50 = int(NR * 0.50); if (i50 < 1) i50 = 1
          i99 = int(NR * 0.99); if (i99 < 1) i99 = 1
          i999 = int(NR * 0.999); if (i999 < 1) i999 = 1
          printf "n=%d p50us=%d p99us=%d p999us=%d maxus=%d", NR, a[i50], a[i99], a[i999], a[NR]
        }')
      echo "r$r $ARM $LABEL $pct non200=$non200$EXTRA $(echo "$out" | grep -E 'finished in|requests:' | tr '\n' ' ')"
    fi
    rm -f "$D/lat.tsv"
  fi
  stop_srv
}

if has_phase 1; then
echo "== phase 1: perf =="
r=1
while [ $r -le $PERF_ROUNDS ]; do
  round_begin "perf round $r"
  rotate $r
  { for arm in $ORDER; do
    start_srv $arm 2
    if [ -n "$SRV_PORT" ]; then
      out=$("$D/client" -url https://127.0.0.1:$SRV_PORT/sse -streams 500 -seconds $SECONDS_RUN -warmup 1 -interval-ms $INTERVAL -label $arm 2>&1) || true
      echo "$out" | grep -E 'streams=|sse latency|NO EVENTS|sse fair|sse conns|ended early|failed:' | sed "s/^/r$r $arm sse500 /"
    fi
    stop_srv
    oneshot $arm 2 ${ONESHOT_N:-1000000}
    oneshot $arm 8 ${ONESHOT_WIDE_N:-2000000}
    h2lat $arm 2 oneshot-lat-e2 "" -n ${ONESHOT_N:-1000000} -c 50 -m 10 -t 4
    h2lat $arm 8 oneshot-lat-e8 "" -n ${ONESHOT_WIDE_N:-2000000} -c 50 -m 10 -t 4
  done; } > "$D/round.out"
  if round_end "perf round $r"; then cat "$D/round.out"; r=$((r + 1)); fi
done
fi

if has_phase 2; then
echo "== phase 2: burst =="
b=1
while [ $b -le $BURST_ROUNDS ]; do
  round_begin "burst round $b"
  rotate $b
  { for arm in $ORDER; do
    start_srv $arm 2
    if [ -n "$SRV_PORT" ]; then
      line=$("$D/client" -url https://127.0.0.1:$SRV_PORT/sse -streams 200 -seconds 2 -warmup 1 -label burst 2>&1 | grep 'streams=') || true
      tr=$(curl -sk --http2 https://127.0.0.1:$SRV_PORT/trace 2>/dev/null) || true
      ov=$(echo "$tr" | sed -n 's/.*"tls_write_overflow":\([0-9]*\).*/\1/p')
      case "$line" in
        *"opened=200 "*"delivering=200 "*"failed=0 "*"ended_early=0 "*) echo "b$b $arm burst OK overflow=${ov:-absent}" ;;
        *) echo "b$b $arm burst FAIL $line overflow=${ov:-absent}" ;;
      esac
    fi
    stop_srv
  done; } > "$D/round.out"
  if round_end "burst round $b"; then cat "$D/round.out"; b=$((b + 1)); fi
done
fi

if has_phase 3; then
echo "== phase 3: server CPU per event at a FIXED offered load =="
c=1
while [ $c -le $CPU_ROUNDS ]; do
  round_begin "cpu round $c"
  rotate $c
  { for arm in $ORDER; do
    for spec in 200 200x10; do
      S=${spec%%x*}; C=1; lbl=cpu$S
      case "$spec" in *x*) C=${spec#*x}; lbl=cpu${S}c$C ;; esac
      start_srv $arm 2
      if [ -n "$SRV_PORT" ]; then
        line=$("$D/client" -url https://127.0.0.1:$SRV_PORT/sse -streams $S -conns $C -seconds $SECONDS_RUN -warmup 1 -label $arm 2>&1 | grep 'events=') || true
        echo "c$c $arm $lbl ticks=$(cpu_ticks $SRV_PID) tck=100 $line"
      fi
      stop_srv
    done
  done; } > "$D/round.out"
  if round_end "cpu round $c"; then cat "$D/round.out"; c=$((c + 1)); fi
done
fi

if has_phase 5; then
echo "== phase 5: open-loop latency at 2 executors, offered totals: $OPEN_RATES =="
r=1
while [ $r -le $OPEN_ROUNDS ]; do
  round_begin "open round $r"
  rotate $r
  { for arm in $ORDER; do
    for total in $OPEN_RATES; do
      per=$((total / 50))
      h2lat $arm 2 "open$((total / 1000))k-e2" " offered=$((per * 50))" -c 50 -m 10 -t $OPEN_THREADS --rps=$per -D 6 --warm-up-time 1
    done
  done; } > "$D/round.out"
  if round_end "open round $r"; then cat "$D/round.out"; r=$((r + 1)); fi
done
fi

if has_phase 6; then
echo "== phase 6: heavy/light/churn mix ($MIX_LABEL, heavy = executors / $MIX_HEAVY_DIV, churn pause ${MIX_CHURN_PAUSE_MS:-20} ms), widths: $MIX_WIDTHS =="
x=1
while [ $x -le $MIX_ROUNDS ]; do
  round_begin "mix round $x"
  rotate $x
  { for arm in $ORDER; do
    for W in $MIX_WIDTHS; do
      start_srv $arm $W
      if [ -n "$SRV_PORT" ]; then
        E=$(grep '"ready"' "$D/$arm.log" | head -1 | sed -n 's/.*"executors":\([0-9]*\).*/\1/p')
        H=$((E / MIX_HEAVY_DIV)); [ $H -lt 1 ] && H=1
        out=$("$D/client" -url https://127.0.0.1:$SRV_PORT/sse -streams $((H * 250)) -conns $H \
          -light-conns $E -light-streams 10 -churn-workers 4 -churn-pause-ms ${MIX_CHURN_PAUSE_MS:-20} -churn-url https://127.0.0.1:$SRV_PORT/ \
          -stagger-ms 50 -interval-ms $INTERVAL -seconds $SECONDS_RUN -warmup 1 -label $arm 2>&1) || true
        thr=$(thread_ticks $SRV_PID)
        echo "$out" | grep -E 'streams=|sse latency|NO EVENTS|sse fair|sse conns|churn conns|ended early|failed:' | sed "s/^/x$x $arm $MIX_LABEL-e$W /"
        echo "x$x $arm $MIX_LABEL-e$W cpu executors=$E heavy=$H light=$E ticks=$(cpu_ticks $SRV_PID) threads=$thr"
      fi
      stop_srv
    done
  done; } > "$D/round.out"
  if round_end "mix round $x"; then cat "$D/round.out"; x=$((x + 1)); fi
done
fi
echo "== done =="
