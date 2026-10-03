#!/bin/sh
# Re-verify on nachos that PLB with the default rank (sum) keeps its win
# after a fix: WS, WS2 (A/A), PL, PLB; SSE 500 at 2, CPU per event (200
# streams on 1 and 10 connections), paced mix at 8 and 12 executors.
#   WORK=<dir from build.sh> captures/placement-followup/run-verify.sh <out-prefix>
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?}; OUT=${1:?}
T=x86_64-linux-musl
for spec in WS:work_stealing WS2:work_stealing PL:pinned PLB:pinned; do
  a=${spec%%:*}; rm -rf "$WORK/vb/out-$a"; mkdir -p "$WORK/vb/out-$a/bin"
  cp "$WORK/$T-${spec#*:}/bin/starh2-bench-server" "$WORK/vb/out-$a/bin/"
done
tail='"probe":0,"probe_handler_placement":"none","probe_conn_placement":"none"'
export EXPECT_WS='"zio_scheduling":"work_stealing","spawn_placement":"auto",'"$tail"',"conn_balance":0,"balance_rank":"none","placement_log":0}'
export EXPECT_WS2="$EXPECT_WS"
export EXPECT_PL='"zio_scheduling":"pinned","spawn_placement":"local",'"$tail"',"conn_balance":0,"balance_rank":"none","placement_log":0}'
export EXPECT_PLB='"zio_scheduling":"pinned","spawn_placement":"local",'"$tail"',"conn_balance":1,"balance_rank":"sum","placement_log":0}'
export ARGS_PL='--spawn-placement local' ARGS_PLB='--spawn-placement local --conn-balance'
run() {
  ARMS="WS WS2 PL PLB" SAME_ZIO_OK=1 BIN_ROOT="$WORK/vb" HOST=ryan@nachos.trex-elevator.ts.net \
    H2LOAD=/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load REMOTE_DIR=/tmp/starh2-verify CLIENT_BIN="$WORK/client-linux" \
    IDLE_MAX_CORES=0.5 PROC_MAX_CORES=0.3 HOST_WAIT_MAX=14400 ROUND_CHECK=1 HOST_BLOCK_RE='^(WowB?[.]exe|cc1plus|cc1|mod-tests|zig)$' \
    EXECUTORS=2 WIDE_EXECUTORS=8 PERF_SSE_STREAMS="500" ONESHOT_PLAIN=0 ONESHOT_LAT=0 CPU_STREAMS="200 200x10" \
    MIX_WIDTHS="8 prod" "$@" "$REPO/tools/zio-arm-ab.sh"
}
run env PHASES="1 3" PERF_ROUNDS=16 CPU_ROUNDS=8 > "$OUT-sse-cpu.txt"
run env PHASES=6 MIX_ROUNDS=8 MIX_CHURN_PAUSE_MS=20 MIX_LABEL=mix > "$OUT-mix-paced.txt"
