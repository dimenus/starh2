#!/bin/sh
# Balancer rank A/B on nachos: one tree, one zio package, one session.
#
#   WORK=<dir> OLD_WS=<0932727 WS binary> captures/placement-followup/run-rank-ab.sh <out-prefix>
#
# WORK holds x86_64-linux-musl-{work_stealing,pinned} from build.sh at the
# commit under test. Arms (every balanced arm runs --placement-log, so the
# log's cost is equal across ranks and absent from WS):
#   WS, WS2  work_stealing (WS2 = same binary and args: the A/A pair)
#   PLC      pinned + .local + --conn-balance --balance-rank connections_first
#   PLH      ... --balance-rank handlers_first
#   PLS      ... --balance-rank sum
# The ready line names the rank on every start.
#
# Runs, each a rotating harness call over all five arms (rounds are
# multiples of 2n = 10):
#   sse500           SSE 500 streams at 2 executors, 20 rounds
#   mix-unpaced      heavy = executors/2, churn unpaced, 8 and 12 executors, 10 rounds
#   mixm-unpaced     heavy = executors/4, unpaced
#   mix-paced        heavy = executors/2, churn paced 20 ms
#   mixm-paced       heavy = executors/4, paced
# Then:
#   clockfix         OLD_WS (0932727, before the waitUntilCb clock fix) vs
#                    WS, unpaced mix at 8 executors, 8 rounds: does the fix
#                    remove the stopped streams?
#   inline pile-up   run separately by defects-live-ranks.sh.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}
OLD_WS=${OLD_WS:?OLD_WS is required}
OUT=${1:?output prefix}
T=x86_64-linux-musl
for spec in WS:work_stealing WS2:work_stealing PLC:pinned PLH:pinned PLS:pinned; do
  a=${spec%%:*}
  rm -rf "$WORK/abbins/out-$a"; mkdir -p "$WORK/abbins/out-$a/bin"
  cp "$WORK/$T-${spec#*:}/bin/starh2-bench-server" "$WORK/abbins/out-$a/bin/"
done
rm -rf "$WORK/abbins/out-WSO"; mkdir -p "$WORK/abbins/out-WSO/bin"
cp "$OLD_WS" "$WORK/abbins/out-WSO/bin/starh2-bench-server"
tail='"probe":0,"probe_handler_placement":"none","probe_conn_placement":"none"'
ws='"zio_scheduling":"work_stealing","spawn_placement":"auto",'"$tail"
pl='"zio_scheduling":"pinned","spawn_placement":"local",'"$tail"',"conn_balance":1'
export EXPECT_WS="$ws"',"conn_balance":0,"balance_rank":"none","placement_log":0}' EXPECT_WS2="$ws"',"conn_balance":0,"balance_rank":"none","placement_log":0}'
export EXPECT_WSO="$ws"',"conn_balance":0}'
export EXPECT_PLC="$pl"',"balance_rank":"connections_first","placement_log":1}'
export EXPECT_PLH="$pl"',"balance_rank":"handlers_first","placement_log":1}'
export EXPECT_PLS="$pl"',"balance_rank":"sum","placement_log":1}'
B='--spawn-placement local --conn-balance --placement-log --balance-rank'
export ARGS_PLC="$B connections_first" ARGS_PLH="$B handlers_first" ARGS_PLS="$B sum"
run() {
  SAME_ZIO_OK=1 BIN_ROOT="$WORK/abbins" \
    HOST=${HOST:-ryan@nachos.trex-elevator.ts.net} \
    H2LOAD=/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load \
    REMOTE_DIR=/tmp/starh2-rank-ab CLIENT_BIN="$WORK/client-linux" \
    IDLE_MAX_CORES=0.5 PROC_MAX_CORES=0.3 HOST_WAIT_MAX=14400 ROUND_CHECK=1 \
    HOST_BLOCK_RE='^(WowB?[.]exe|cc1plus|cc1|mod-tests|zig)$' \
    EXECUTORS=2 WIDE_EXECUTORS=8 PERF_SSE_STREAMS="500" ONESHOT_PLAIN=0 ONESHOT_LAT=0 \
    MIX_WIDTHS="8 prod" PLACE_LOG=1 "$@" "$REPO/tools/zio-arm-ab.sh"
}
A="WS WS2 PLC PLH PLS"
run env ARMS="$A" PHASES=1 PERF_ROUNDS=${SSE_ROUNDS:-20} > "$OUT-sse500.txt"
run env ARMS="$A" PHASES=6 MIX_ROUNDS=${MIX_ROUNDS:-10} MIX_CHURN_PAUSE_MS=0 MIX_HEAVY_DIV=2 MIX_LABEL=mix > "$OUT-mix-unpaced.txt"
run env ARMS="$A" PHASES=6 MIX_ROUNDS=${MIX_ROUNDS:-10} MIX_CHURN_PAUSE_MS=0 MIX_HEAVY_DIV=4 MIX_LABEL=mixm > "$OUT-mixm-unpaced.txt"
run env ARMS="$A" PHASES=6 MIX_ROUNDS=${MIX_ROUNDS:-10} MIX_CHURN_PAUSE_MS=20 MIX_HEAVY_DIV=2 MIX_LABEL=mix > "$OUT-mix-paced.txt"
run env ARMS="$A" PHASES=6 MIX_ROUNDS=${MIX_ROUNDS:-10} MIX_CHURN_PAUSE_MS=20 MIX_HEAVY_DIV=4 MIX_LABEL=mixm > "$OUT-mixm-paced.txt"
run env ARMS="WSO WS" PHASES=6 MIX_ROUNDS=${FIX_ROUNDS:-8} MIX_WIDTHS=8 MIX_CHURN_PAUSE_MS=0 MIX_HEAVY_DIV=2 MIX_LABEL=mix PLACE_LOG=0 > "$OUT-clockfix.txt"
