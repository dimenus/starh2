#!/bin/sh
# N-way A/B of zio PINS under one starh2 tree, run on nachos (linux, io_uring).
#
# # Why this exists
#
# starh2 carries a zio fork. Upstream is replacing those patches with its own
# (PR #709 supersedes our #702), and #709's own microbenchmarks report a 7-23%
# regression on select paths. starh2's actor parks in ONE `zio.select` per turn,
# so that cost lands on every SSE event. A microbenchmark cannot say what it
# costs here; this does.
#
# # What varies, and what is held constant
#
# ONE thing varies: the `.zio` pin in build.zig.zon. Each arm is a worktree of
# the SAME starh2 commit, built by the same compiler for the same target, so a
# difference between arms is a difference in zio.
#
# Every arm also carries the fork-only `runtime: poll timeouts on a dedicated
# executor` patch. Nothing upstream replaces it, so an arm without it would
# measure a missing timer fix instead of the select protocol.
#
# # Why nachos and not the laptop
#
# nachos runs the io_uring backend; the laptop runs kqueue. The CompletionQueue
# path #709 rewrites is backend-specific. nachos cannot build (toolchain drift,
# t-880), so arms are cross-built here as static x86_64-linux-musl binaries and
# shipped, as tools/cq-nachos-ab.sh does.
#
# # The two phases, and why the second needs its own denominator
#
# PERF rows are throughput and latency. BURST rows are a rate: opening 200 SSE
# streams at once on one TLS connection fails closed sometimes, and on nachos
# the current pin failed 1 round in 10 once and 0 in 20 twice. A rate that low
# cannot be compared from three rounds, so the burst phase runs BURST_ROUNDS
# per arm and reports fails/rounds per arm. Two arms are only different when
# their rates are, against that denominator.
#
# # The instrument checks, before any number is read
#
# 1. Each binary must embed EXACTLY ONE `zio-0.17.0-<hash>` package path, and
#    it must be that arm's. A stale cache silently linking another arm's zio
#    would produce four numbers for one build.
# 2. All arm hashes must differ; two arms resolving to one package measure the
#    same thing twice and read as "no difference".
# 3. Zero rows is a FAILURE, not a pass.
# 4. The h2load thread count was swept (t=2,4,8,12) at both widths and the rate
#    is flat, so the one-shot rows are server-bound rather than client-bound.
#
# # Order
#
# The arm that runs second in a round wins on every ordering (see the header of
# tools/sse_bench/run.sh). The arm order ROTATES every round, so each arm takes
# every slot and the ordering cancels.
#
#   ARMS="current main 702 709" BIN_ROOT=/tmp/scratch PERF_ROUNDS=5 \
#     BURST_ROUNDS=50 tools/zio-arm-ab.sh > rows.txt
#
# BIN_ROOT holds out-<arm>/bin/starh2-bench-server for every arm in ARMS.
set -eu

REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
ARMS=${ARMS:?ARMS is required, e.g. ARMS="current main 702 709"}
BIN_ROOT=${BIN_ROOT:?BIN_ROOT is required}
PERF_ROUNDS=${PERF_ROUNDS:-5}
BURST_ROUNDS=${BURST_ROUNDS:-50}
CPU_ROUNDS=${CPU_ROUNDS:-6}
HOST=${HOST:-nachos}
SECONDS_RUN=${SECONDS_RUN:-10}
INTERVAL=${INTERVAL:-1}
EXECUTORS=${EXECUTORS:-2}
WIDE_EXECUTORS=${WIDE_EXECUTORS:-8}
# Sized from the sweeps: 820k req/s at 2 executors and 2.3M at 8, so these give
# runs of roughly 2.4 s and 1.7 s. A 100k run finished in 28 ms and measured
# start-up, not steady state.
ONESHOT_N=${ONESHOT_N:-2000000}
ONESHOT_WIDE_N=${ONESHOT_WIDE_N:-4000000}
# 200 streams delivers 100% at p50 ~11us; 500 sits at the knee (p50 ~750us,
# still 100% delivered) and is where a per-event cost shows.
SSE_LOW=${SSE_LOW:-200}
SSE_HIGH=${SSE_HIGH:-500}
REMOTE_DIR=${REMOTE_DIR:-/tmp/zioab}

SOCK=$(ls /private/tmp/com.apple.launchd.*/Listeners 2>/dev/null | head -1) || true
export SSH_AUTH_SOCK=${SSH_AUTH_SOCK:-$SOCK}

echo "== arm identity (embedded zio package hash) =="
hashes=""
for arm in $ARMS; do
  bin="$BIN_ROOT/out-$arm/bin/starh2-bench-server"
  [ -f "$bin" ] || { echo "missing binary for arm $arm: $bin" >&2; exit 1; }
  h=$(strings -a "$bin" | grep -oE 'zio-0\.17\.0-[A-Za-z0-9_-]{20,}' | sort -u)
  n=$(printf '%s\n' "$h" | grep -c .)
  [ "$n" = 1 ] || { echo "arm $arm embeds $n zio package paths, expected 1" >&2; exit 1; }
  echo "  $arm  $h"
  hashes="$hashes$h
"
done
dupes=$(printf '%s' "$hashes" | sort | uniq -d)
[ -z "$dupes" ] || { echo "two arms share one zio package: $dupes" >&2; exit 1; }

for f in testdata/cert.pem testdata/key.pem; do
  [ -f "$REPO/$f" ] || { echo "$f is missing; generate it (see tools/README.md)" >&2; exit 1; }
done
(cd "$REPO/tools/sse_bench" && GOOS=linux GOARCH=amd64 go build -o /tmp/zioab-client ./client.go)
ssh "$HOST" "mkdir -p $REMOTE_DIR"
for arm in $ARMS; do
  scp -q "$BIN_ROOT/out-$arm/bin/starh2-bench-server" "$HOST:$REMOTE_DIR/$arm-server"
done
scp -q /tmp/zioab-client "$HOST:$REMOTE_DIR/client"
scp -q "$REPO/testdata/cert.pem" "$REPO/testdata/key.pem" "$HOST:$REMOTE_DIR/"

ssh "$HOST" "ARMS='$ARMS' PERF_ROUNDS=$PERF_ROUNDS BURST_ROUNDS=$BURST_ROUNDS CPU_ROUNDS=$CPU_ROUNDS \
  SECONDS_RUN=$SECONDS_RUN INTERVAL=$INTERVAL EXECUTORS=$EXECUTORS \
  WIDE_EXECUTORS=$WIDE_EXECUTORS ONESHOT_N=$ONESHOT_N ONESHOT_WIDE_N=$ONESHOT_WIDE_N \
  SSE_LOW=$SSE_LOW SSE_HIGH=$SSE_HIGH D=$REMOTE_DIR sh -s" <<'REMOTE'
set -u
chmod +x $D/*-server $D/client
echo "== host =="
uname -r; echo "cores=$(nproc)"; uptime

start_srv() {
  ARM=$1; EXEC=$2
  rm -f $D/$ARM.log
  $D/$ARM-server --mode tls --port 0 --executors $EXEC --sse-interval-ms $INTERVAL \
    --cert $D/cert.pem --key $D/key.pem > $D/$ARM.log 2>&1 &
  SRV_PID=$!
  i=0; SRV_PORT=
  while [ $i -lt 200 ]; do
    SRV_PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' $D/$ARM.log 2>/dev/null | head -1)
    [ -n "$SRV_PORT" ] && break
    i=$((i+1)); sleep 0.05
  done
  [ -n "$SRV_PORT" ] || echo "NO-READY-LINE arm=$ARM"
}
stop_srv() { kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null; }

# rotate the arm list by (round-1) so each arm takes every slot
rotate() {
  R=$1; set -- $ARMS; n=$#; k=$(( (R - 1) % n )); i=0; ORDER=""
  while [ $i -lt $n ]; do
    idx=$(( (k + i) % n + 1 )); j=1
    for a in $ARMS; do [ $j -eq $idx ] && ORDER="$ORDER $a"; j=$((j+1)); done
    i=$((i+1))
  done
}

rows=0
echo "== phase 1: perf =="
r=1
while [ $r -le $PERF_ROUNDS ]; do
  rotate $r
  for arm in $ORDER; do
    for S in $SSE_LOW $SSE_HIGH; do
      start_srv $arm $EXECUTORS
      if [ -n "$SRV_PORT" ]; then
        out=$(timeout 180 $D/client -url https://127.0.0.1:$SRV_PORT/sse -streams $S \
          -seconds $SECONDS_RUN -warmup 1 -label $arm 2>&1)
        echo "$out" | grep -E 'streams=|sse latency|NO EVENTS' | sed "s/^/r$r $arm sse$S /"
        rows=$((rows+1))
      fi
      stop_srv
    done

    start_srv $arm $EXECUTORS
    if [ -n "$SRV_PORT" ]; then
      out=$(timeout 180 h2load -n $ONESHOT_N -c 50 -m 10 -t 4 https://127.0.0.1:$SRV_PORT/ 2>&1)
      rc=$?
      if [ $rc -ne 0 ]; then echo "r$r $arm oneshot-e$EXECUTORS WEDGE-OR-FAIL rc=$rc"
      else echo "$out" | grep -E 'finished in|0 failed' | tr '\n' ' ' | sed "s/^/r$r $arm oneshot-e$EXECUTORS /"; echo; fi
      rows=$((rows+1))
    fi
    stop_srv

    start_srv $arm $WIDE_EXECUTORS
    if [ -n "$SRV_PORT" ]; then
      out=$(timeout 180 h2load -n $ONESHOT_WIDE_N -c 50 -m 10 -t 4 https://127.0.0.1:$SRV_PORT/ 2>&1)
      rc=$?
      if [ $rc -ne 0 ]; then echo "r$r $arm oneshot-e$WIDE_EXECUTORS WEDGE-OR-FAIL rc=$rc"
      else echo "$out" | grep -E 'finished in|0 failed' | tr '\n' ' ' | sed "s/^/r$r $arm oneshot-e$WIDE_EXECUTORS /"; echo; fi
      rows=$((rows+1))
    fi
    stop_srv
  done
  r=$((r+1))
done

echo "== phase 2: burst fail-close rate ($BURST_ROUNDS rounds per arm) =="
b=1
while [ $b -le $BURST_ROUNDS ]; do
  rotate $b
  for arm in $ORDER; do
    start_srv $arm $EXECUTORS
    if [ -n "$SRV_PORT" ]; then
      line=$(timeout 60 $D/client -url https://127.0.0.1:$SRV_PORT/sse -streams $SSE_LOW \
        -seconds 2 -warmup 1 -label burst 2>&1 | grep 'streams=')
      tr=$(curl -sk --http2 https://127.0.0.1:$SRV_PORT/trace 2>/dev/null)
      ov=$(echo "$tr" | sed -n 's/.*"tls_write_overflow":\([0-9]*\).*/\1/p')
      st=$(echo "$tr" | sed -n 's/.*"tls_stage_failed":\([0-9]*\).*/\1/p')
      case "$line" in
        *"opened=$SSE_LOW "*"delivering=$SSE_LOW "*"failed=0 "*)
          echo "b$b $arm burst OK overflow=${ov:-absent} stage_failed=${st:-absent}" ;;
        *)
          echo "b$b $arm burst FAIL $line overflow=${ov:-absent} stage_failed=${st:-absent}"
          echo "b$b $arm burst-log $(grep -v '"ready"' $D/$arm.log | head -3)" ;;
      esac
      rows=$((rows+1))
    fi
    stop_srv
  done
  b=$((b+1))
done

echo "== phase 3: server CPU per event at a FIXED offered load =="
# Every arm delivers 100% of a 200-stream 1ms offering, so a latency compare
# puts two servers side by side that are both keeping up. CPU per event does
# not: the client fixes the work, so the only thing that varies is what the
# server spends to do it. utime+stime come from /proc/<pid>/stat fields 14 and
# 15, read BEFORE the kill, in clock ticks.
TCK=$(getconf CLK_TCK)
c=1
while [ $c -le $CPU_ROUNDS ]; do
  rotate $c
  for arm in $ORDER; do
    for S in $SSE_LOW $SSE_HIGH; do
      start_srv $arm $EXECUTORS
      if [ -n "$SRV_PORT" ]; then
        line=$(timeout 180 $D/client -url https://127.0.0.1:$SRV_PORT/sse -streams $S \
          -seconds $SECONDS_RUN -warmup 1 -label $arm 2>&1 | grep 'events=')
        cpu=$(awk '{print $14+$15}' /proc/$SRV_PID/stat 2>/dev/null)
        echo "c$c $arm cpu$S ticks=${cpu:-absent} tck=$TCK $line"
        rows=$((rows+1))
      fi
      stop_srv
    done
  done
  c=$((c+1))
done

echo "== rows=$rows =="
[ $rows -gt 0 ] || { echo "zio-arm-ab: produced no rows; that is a failure, not a pass" >&2; exit 1; }
REMOTE
