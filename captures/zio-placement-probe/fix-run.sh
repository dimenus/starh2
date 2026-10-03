#!/bin/sh
# The raw rows and logs this script reads or names are no longer in the repo.
# They are in the off-repo archive ~/Dropbox/starh2-captures/pr14-placement-followup-captures-3a6fad3.tar.gz
# (every file this branch added under captures/, as of 3a6fad3). Extract it at
# the repo root to restore them before rerunning.
# Bench the load-aware connection placement experiment (--conn-balance)
# against pinned + .local without it and against work_stealing, all built
# from this branch.
#
#   WORK=<dir with out-PIN, out-WS> captures/zio-placement-probe/fix-run.sh > fix-rows.txt
#
# Binaries (this tree, -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl):
#   out-WS   default (work_stealing)
#   out-PIN  -Dzio-scheduling=pinned
# Arms:
#   WSL  out-WS
#   PL   out-PIN, --spawn-placement local
#   PLB  out-PIN, --spawn-placement local --conn-balance
# Rows (3 arms, balanced rotation: every 6 rounds is one full cycle):
#   phase 1, 30 rounds: SSE 500 streams (the knee shape; knee = all 500
#     delivered and p50 > 200 us), one-shot rps at 2 and 8 executors
#   phase 2, 60 rounds: burst fail-close (200 streams at once)
#   phase 3, 12 rounds: CPU per event at 200 streams on 1 connection and
#     over 10 connections
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}
for a in WSL PL PLB; do
  src=out-PIN; [ $a = WSL ] && src=out-WS
  rm -rf "$WORK/bins/out-$a"; mkdir -p "$WORK/bins/out-$a/bin"
  cp "$WORK/$src/bin/starh2-bench-server" "$WORK/bins/out-$a/bin/"
done
export ARGS_PL='--spawn-placement local' ARGS_PLB='--spawn-placement local --conn-balance'
export EXPECT_WSL='"zio_scheduling":"work_stealing","spawn_placement":"auto","probe":0,"probe_handler_placement":"none","probe_conn_placement":"none","conn_balance":0'
export EXPECT_PL='"zio_scheduling":"pinned","spawn_placement":"local","probe":0,"probe_handler_placement":"none","probe_conn_placement":"none","conn_balance":0'
export EXPECT_PLB='"zio_scheduling":"pinned","spawn_placement":"local","probe":0,"probe_handler_placement":"none","probe_conn_placement":"none","conn_balance":1'
ARMS="WSL PL PLB" BIN_ROOT="$WORK/bins" \
  HOST=${HOST:-ryan@100.113.184.27} \
  H2LOAD=${H2LOAD:-/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load} \
  REMOTE_DIR=${REMOTE_DIR:-/tmp/zio-placement-fix} CLIENT_BIN="$WORK/zioab-client" \
  IDLE_MAX_CORES=0.5 PROC_MAX_CORES=0.3 HOST_WAIT_MAX=${HOST_WAIT_MAX:-14400} ROUND_CHECK=1 \
  HOST_BLOCK_RE='^(WowB?[.]exe|cc1plus|cc1|mod-tests|zig)$' \
  PHASES="1 2 3" EXECUTORS=2 WIDE_EXECUTORS=8 \
  PERF_ROUNDS=30 PERF_SSE_STREAMS="500" ONESHOT_WIDTHS="2 8" ONESHOT_PLAIN=1 ONESHOT_LAT=0 \
  ONESHOT_N=2000000 ONESHOT_WIDE_N=2000000 \
  BURST_ROUNDS=60 BURST_ARMS="WSL PL PLB" \
  CPU_ROUNDS=12 CPU_STREAMS="200 200x10" \
  "$REPO/tools/zio-arm-ab.sh"
