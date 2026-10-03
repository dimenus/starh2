#!/bin/sh
# The raw rows and logs this script reads or names are no longer in the repo.
# They are in the off-repo archive ~/Dropbox/starh2-captures/pr14-placement-followup-captures-3a6fad3.tar.gz
# (every file this branch added under captures/, as of 3a6fad3). Extract it at
# the repo root to restore them before rerunning.
# nachos: the committed balancer rank (connections, then handlers) against
# the variant rank-sum-variant.diff (connections + handlers as one score).
#
#   WORK=<dir> captures/placement-followup/run-rank-variant.sh <out-prefix>
#
# WORK holds x86_64-linux-musl-{work_stealing,pinned} from build.sh and
# x86_64-linux-musl-pinned-sum built with the diff applied. Arms: WS, WS2
# (A/A), PLB (committed rank), PLBS (variant). The variant's ready line
# carries "balance_rank":"sum", so each start proves which binary ran.
# Three runs: SSE 500 at 2 executors; the unpaced churn mix (the shape that
# regressed); the paced mix.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}
OUT=${1:?output prefix}
T=x86_64-linux-musl
for spec in WS:work_stealing WS2:work_stealing PLB:pinned PLBS:pinned-sum; do
  a=${spec%%:*}
  rm -rf "$WORK/vbins/out-$a"; mkdir -p "$WORK/vbins/out-$a/bin"
  cp "$WORK/$T-${spec#*:}/bin/starh2-bench-server" "$WORK/vbins/out-$a/bin/"
done
tail='"probe":0,"probe_handler_placement":"none","probe_conn_placement":"none"'
export EXPECT_WS='"zio_scheduling":"work_stealing","spawn_placement":"auto",'"$tail"',"conn_balance":0}'
export EXPECT_WS2="$EXPECT_WS"
export EXPECT_PLB='"zio_scheduling":"pinned","spawn_placement":"local",'"$tail"',"conn_balance":1}'
export EXPECT_PLBS='"zio_scheduling":"pinned","spawn_placement":"local",'"$tail"',"conn_balance":1,"balance_rank":"sum"}'
export ARGS_PLB='--spawn-placement local --conn-balance' ARGS_PLBS='--spawn-placement local --conn-balance'
run() {
  ARMS="WS WS2 PLB PLBS" SAME_ZIO_OK=1 BIN_ROOT="$WORK/vbins" \
    HOST=${HOST:-ryan@nachos.trex-elevator.ts.net} \
    H2LOAD=/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load \
    REMOTE_DIR=/tmp/starh2-placement-rebench CLIENT_BIN="$WORK/client-linux" \
    IDLE_MAX_CORES=0.5 PROC_MAX_CORES=0.3 HOST_WAIT_MAX=14400 ROUND_CHECK=1 \
    HOST_BLOCK_RE='^(WowB?[.]exe|cc1plus|cc1|mod-tests|zig)$' \
    EXECUTORS=2 WIDE_EXECUTORS=8 PERF_SSE_STREAMS="500" ONESHOT_PLAIN=0 ONESHOT_LAT=0 \
    MIX_WIDTHS="8 prod" "$@" "$REPO/tools/zio-arm-ab.sh"
}
run env PHASES=1 PERF_ROUNDS=16 > "$OUT-sse500.txt"
run env PHASES=6 MIX_ROUNDS=16 MIX_CHURN_PAUSE_MS=0 MIX_LABEL=mix > "$OUT-mix-unpaced.txt"
run env PHASES=6 MIX_ROUNDS=16 MIX_CHURN_PAUSE_MS=20 MIX_LABEL=mix > "$OUT-mix-paced.txt"
