#!/bin/sh
# The placement follow-up session on nachos (Linux, io_uring), t-2502.
#
#   WORK=<dir from build.sh> captures/placement-followup/run-linux.sh > linux-rows.txt
#
# Every arm is one of two binaries from ONE tree (build.sh), so arms differ
# only by scheduling and run arguments. The TLS stash fix (bc74f46) is in.
#
# Arms:
#   WS    work_stealing, spawn placement .auto              (the control)
#   WS2   the same binary and args again                     (in-session A/A)
#   WSH   work_stealing, --spawn-placement prefer_local      (start local, may migrate)
#   PA    pinned, --spawn-placement auto
#   PL    pinned, --spawn-placement local
#   PLB   pinned, --spawn-placement local --conn-balance
# Every ready line must name the scheduling and placement the arm is for.
#
# Rounds are multiples of 2n = 12, so the balanced rotation gives every arm
# every slot equally. Phases (PHASES picks):
#   1  SSE 500 streams at 2 executors (the knee shape), one-shot rps and
#      closed-loop one-shot latency at 2 and 8 executors
#   2  burst: 200 streams opened at once, fail-close rate
#   3  CPU per event, 200 streams on 1 and on 10 connections, 2 executors
#   5  open-loop one-shot latency at fixed offered rates, 2 executors
#   6  heavy/light/churn SSE mix at 8 executors and production width
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}
T=x86_64-linux-musl
for spec in WS:work_stealing WS2:work_stealing WSH:work_stealing PA:pinned PL:pinned PLB:pinned; do
  a=${spec%%:*}; src=$T-${spec#*:}
  rm -rf "$WORK/bins/out-$a"; mkdir -p "$WORK/bins/out-$a/bin"
  cp "$WORK/$src/bin/starh2-bench-server" "$WORK/bins/out-$a/bin/"
done
tail='"probe":0,"probe_handler_placement":"none","probe_conn_placement":"none"'
export EXPECT_WS='"zio_scheduling":"work_stealing","spawn_placement":"auto",'"$tail"',"conn_balance":0}'
export EXPECT_WS2="$EXPECT_WS"
export EXPECT_WSH='"zio_scheduling":"work_stealing","spawn_placement":"prefer_local",'"$tail"',"conn_balance":0}'
export EXPECT_PA='"zio_scheduling":"pinned","spawn_placement":"auto",'"$tail"',"conn_balance":0}'
export EXPECT_PL='"zio_scheduling":"pinned","spawn_placement":"local",'"$tail"',"conn_balance":0}'
export EXPECT_PLB='"zio_scheduling":"pinned","spawn_placement":"local",'"$tail"',"conn_balance":1}'
export ARGS_WSH='--spawn-placement prefer_local' ARGS_PA='--spawn-placement auto'
export ARGS_PL='--spawn-placement local' ARGS_PLB='--spawn-placement local --conn-balance'
ARMS=${ARMS:-WS WS2 WSH PA PL PLB} SAME_ZIO_OK=1 BIN_ROOT="$WORK/bins" \
  HOST=${HOST:-ryan@nachos.trex-elevator.ts.net} \
  H2LOAD=${H2LOAD:-/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load} \
  REMOTE_DIR=${REMOTE_DIR:-/tmp/starh2-placement-followup} CLIENT_BIN="$WORK/client-linux" \
  IDLE_MAX_CORES=${IDLE_MAX_CORES:-0.5} PROC_MAX_CORES=${PROC_MAX_CORES:-0.3} \
  HOST_WAIT_MAX=${HOST_WAIT_MAX:-14400} ROUND_CHECK=1 \
  HOST_BLOCK_RE='^(WowB?[.]exe|cc1plus|cc1|mod-tests|zig)$' \
  PHASES=${PHASES:-1 2 3 5 6} EXECUTORS=2 WIDE_EXECUTORS=8 \
  PERF_ROUNDS=${PERF_ROUNDS:-24} PERF_SSE_STREAMS="500" ONESHOT_WIDTHS="2 8" ONESHOT_PLAIN=1 \
  ONESHOT_LAT=1 ONESHOT_LAT_WIDTHS="2 8" ONESHOT_N=2000000 ONESHOT_WIDE_N=4000000 \
  BURST_ROUNDS=${BURST_ROUNDS:-36} \
  CPU_ROUNDS=${CPU_ROUNDS:-12} CPU_STREAMS="200 200x10" \
  OPEN_ROUNDS=${OPEN_ROUNDS:-12} OPEN_RATES="320000 590000" OPEN_CLOSED=0 \
  MIX_ROUNDS=${MIX_ROUNDS:-12} MIX_WIDTHS="8 prod" \
  "$REPO/tools/zio-arm-ab.sh"
