#!/bin/sh
# Sessions of the placement / #718-rate A/B on nachos. Arms are built by
# prep.sh into $WORK.
#
#   WORK=<dir> captures/zio-placement-ab-0299e57/run.sh main      # A WS PL PA WSN
#   WORK=<dir> captures/zio-placement-ab-0299e57/run.sh mech747   # WSI vs WSIR, io_uring poll counters
#   WORK=<dir> captures/zio-placement-ab-0299e57/run.sh mech718   # WS WSN WSR, per-thread CPU
#
# Summaries: `REF=WS tools/zio-arm-ab-summary.sh main-rows.txt` (and REF=A).
#
# # main
#
# Every arm, rotated each round: 10 one-shot rounds (rps at 2 and 8
# executors, then a logged run at 2 executors for p50/p99, with per-thread
# CPU), SSE200 latency in the same rounds, 8 CPU rounds at 200 and 500
# streams, and 60 burst rounds for WS, PL and PA only.
#
# PL and PA are one binary. The harness accepts that because their args
# differ, and every server start must print the expected scheduling and
# placement in its ready line, or the run stops.
#
# # Host
#
# The busy-core limit is 0.5 (the brief), one process may use 0.3, and a process
# matching HOST_BLOCK_RE makes the host busy while it exists. A busy host is
# waited out, up to four hours per check,
# re-checked every 30 s, and every wait is printed; the check also runs
# before every round of phases 1 and 3 and every 10th burst round.
set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}

export BIN_ROOT=$WORK/bins
export HOST=${HOST:-ryan@100.113.184.27}
export H2LOAD=${H2LOAD:-/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load}
export REMOTE_DIR=${REMOTE_DIR:-/tmp/zio-placement-ab}
export CLIENT_BIN=$WORK/zioab-client
export IDLE_MAX_CORES=0.5 PROC_MAX_CORES=0.3 HOST_WAIT_MAX=${HOST_WAIT_MAX:-14400} ROUND_CHECK=1
# Processes that make nachos busy while they exist: the game that ran through
# this session, and the compile and test jobs seen during the bisect.
export HOST_BLOCK_RE='^(WowB?\.exe|cc1plus|cc1|mod-tests|zig)$'
export ONESHOT_N=2000000 ONESHOT_WIDE_N=2000000

WS_READY='"zio_scheduling":"work_stealing","spawn_placement":"auto"'
export ARGS_PL='--spawn-placement local' ARGS_PA='--spawn-placement auto'
export EXPECT_PL='"zio_scheduling":"pinned","spawn_placement":"local"'
export EXPECT_PA='"zio_scheduling":"pinned","spawn_placement":"auto"'
export EXPECT_WS="$WS_READY" EXPECT_WSN="$WS_READY" EXPECT_WSR="$WS_READY"
export EXPECT_WSI="$WS_READY" EXPECT_WSIR="$WS_READY"
for arm in A WS PL PA WSN WSR WSI WSIR; do export "TREE_$arm=$WORK/tree-$arm"; done

case "${1:-}" in
  main)
    ARMS="A WS PL PA WSN" PHASES="1 2 3" \
      PERF_ROUNDS=10 PERF_SSE_STREAMS=200 ONESHOT_WIDTHS="2 8" \
      ONESHOT_LAT=1 ONESHOT_LAT_WIDTHS=2 THREAD_CPU=1 \
      BURST_ROUNDS=60 BURST_ARMS="WS PL PA" \
      CPU_ROUNDS=8 CPU_STREAMS="200 500" \
      "$REPO/tools/zio-arm-ab.sh"
    ;;
  mech747)
    ARMS="WSI WSIR" PHASES=3 CPU_ROUNDS=6 CPU_STREAMS=200 LOG_GREP=iouring-stats \
      "$REPO/tools/zio-arm-ab.sh"
    ;;
  mech718)
    ARMS="WS WSN WSR" PHASES=1 PERF_ROUNDS=6 PERF_SSE_STREAMS="" ONESHOT_PLAIN=0 \
      ONESHOT_LAT=1 ONESHOT_LAT_WIDTHS=2 THREAD_CPU=1 \
      "$REPO/tools/zio-arm-ab.sh"
    ;;
  oneconn)
    # Bimodality: one-shot over ONE TLS connection, the shape 27ff454 saw
    # pinned placement split into two bands in. Every row is printed.
    ARMS="A WS PL PA" PHASES=4 ONECONN_ROUNDS=12 ONECONN_WIDTHS="2 8" \
      "$REPO/tools/zio-arm-ab.sh"
    ;;
  trees)
    # Does the starh2 tree move one-shot p99 at 2 executors? The same zio on
    # 619242f (B, U) and on 1080022 (WS, WSR). B and U print no scheduling in
    # their ready line, so their check is that the line ENDS after
    # batch_wake_sleepers, which only the 619242f bench server does.
    EXPECT_B='"batch_wake_sleepers":1}' EXPECT_U='"batch_wake_sleepers":1}' \
    ARMS="B WS U WSR" PHASES=1 PERF_ROUNDS=10 PERF_SSE_STREAMS="" ONESHOT_PLAIN=0 \
      ONESHOT_LAT=1 ONESHOT_LAT_WIDTHS=2 THREAD_CPU=1 TREE_B="$WORK/tree-B" TREE_U="$WORK/tree-U" \
      "$REPO/tools/zio-arm-ab.sh"
    ;;
  readycheck)
    # Arms the ready-line check on purpose: PL runs with --spawn-placement
    # local but is told to expect auto, so the first start must stop the run
    # with READY-MISMATCH and exit 4. It measures nothing, so the host limits
    # are lifted for it.
    ARMS=PL EXPECT_PL='"zio_scheduling":"pinned","spawn_placement":"auto"' \
      PHASES=3 CPU_ROUNDS=1 CPU_STREAMS=200 IDLE_MAX_CORES=99 PROC_MAX_CORES=99 HOST_WAIT_MAX=0 \
      "$REPO/tools/zio-arm-ab.sh"
    ;;
  *)
    echo "usage: WORK=<dir> $0 main|mech747|mech718|oneconn|trees|readycheck" >&2
    exit 2
    ;;
esac
