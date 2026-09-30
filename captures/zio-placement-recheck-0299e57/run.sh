#!/bin/sh
# Recheck of the placement session (captures/zio-placement-ab-0299e57) with a
# balanced arm order. That session rotated its 5 arms cyclically, so in each
# pair one arm ran first in 8-9 of 10 rounds (t-2500). tools/zio-arm-ab.sh
# now runs blocks of n forward, then n reversed rotations; with 5 arms, 10
# rounds is exactly one balanced cycle (captures/zio-openloop-0299e57/
# rotate-check.sh).
#
#   PIN_WORK=<bisect WORK> PLACE_WORK=<placement WORK> WORK=<dir> \
#     captures/zio-placement-recheck-0299e57/run.sh prep
#   WORK=<dir> captures/zio-placement-recheck-0299e57/run.sh main
#
# # Arms (every 0299e57 arm on the 1080022 tree, so the tree is not a variable)
#
#   A     3786083 reference (908992b + d8e46d6)
#   WSL   0299e57 work_stealing                  (the control)
#   WSL2  the WSL binary again                   (A/A band)
#   PL    0299e57 pinned, --spawn-placement local
#   PA    the PL binary, --spawn-placement auto
#
# All binaries were built in earlier sessions; prep copies them (A from the
# bisect WORK, the rest from the placement WORK: out-WS, out-P).
#
# # Rows
#
# Phase 1, 10 rounds: SSE 200 and 500 streams (p50/p99), h2load one-shot rps
#   at 2 and 8 executors (closed loop; rps only).
# Phase 2, 60 rounds, WSL, PL and PA only: burst fail-close.
# Phase 3, 10 rounds: CPU per SSE event at 200 and 500 streams on one TLS
#   connection, and 200 streams over 10 connections (cpu200c10), so PL's CPU
#   result is also seen when the streams are not all on one connection.
# Phase 5, 10 rounds: open-loop latency at 590k offered req/s, 2 executors
#   (h2load -c 50 -m 10 -t 12 --rps=11800 -D 6 --warm-up-time 1).
set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}

case "${1:-}" in
  prep)
    PIN_WORK=${PIN_WORK:?PIN_WORK is required}
    PLACE_WORK=${PLACE_WORK:?PLACE_WORK is required}
    for spec in A:$PIN_WORK/bins/out-A:$PIN_WORK/treeA \
                WSL:$PLACE_WORK/bins/out-WS:$PLACE_WORK/tree-WS \
                WSL2:$PLACE_WORK/bins/out-WS:$PLACE_WORK/tree-WS \
                PL:$PLACE_WORK/bins/out-P:$PLACE_WORK/tree-P \
                PA:$PLACE_WORK/bins/out-P:$PLACE_WORK/tree-P; do
      arm=${spec%%:*}; rest=${spec#*:}; bin=${rest%%:*}; tree=${rest#*:}
      rm -rf "$WORK/bins/out-$arm"; mkdir -p "$WORK/bins/out-$arm/bin"
      cp "$bin/bin/starh2-bench-server" "$WORK/bins/out-$arm/bin/"
      ln -sfn "$tree" "$WORK/tree-$arm"
    done
    shasum -a 256 "$WORK"/bins/out-*/bin/starh2-bench-server
    ;;
  main)
    WS_READY='"zio_scheduling":"work_stealing","spawn_placement":"auto"'
    export ARGS_PL='--spawn-placement local' ARGS_PA='--spawn-placement auto'
    export EXPECT_WSL="$WS_READY" EXPECT_WSL2="$WS_READY"
    export EXPECT_PL='"zio_scheduling":"pinned","spawn_placement":"local"'
    export EXPECT_PA='"zio_scheduling":"pinned","spawn_placement":"auto"'
    for arm in A WSL WSL2 PL PA; do export "TREE_$arm=$WORK/tree-$arm"; done
    ARMS="A WSL WSL2 PL PA" SAME_ZIO_OK=1 BIN_ROOT="$WORK/bins" \
      HOST=${HOST:-ryan@100.113.184.27} \
      H2LOAD=${H2LOAD:-/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load} \
      REMOTE_DIR=${REMOTE_DIR:-/tmp/zio-placement-recheck} CLIENT_BIN="$WORK/zioab-client" \
      IDLE_MAX_CORES=0.5 PROC_MAX_CORES=0.3 HOST_WAIT_MAX=${HOST_WAIT_MAX:-14400} ROUND_CHECK=1 \
      HOST_BLOCK_RE='^(WowB?[.]exe|cc1plus|cc1|mod-tests|zig)$' \
      PHASES="1 2 3 5" EXECUTORS=2 WIDE_EXECUTORS=8 \
      PERF_ROUNDS=10 PERF_SSE_STREAMS="200 500" ONESHOT_WIDTHS="2 8" ONESHOT_PLAIN=1 ONESHOT_LAT=0 \
      ONESHOT_N=2000000 ONESHOT_WIDE_N=2000000 \
      BURST_ROUNDS=60 BURST_ARMS="WSL PL PA" \
      CPU_ROUNDS=10 CPU_STREAMS="200 500 200x10" \
      OPEN_ROUNDS=10 OPEN_RATES=590000 OPEN_SECONDS=6 OPEN_WARMUP=1 OPEN_THREADS=12 OPEN_CLOSED=0 \
      "$REPO/tools/zio-arm-ab.sh"
    ;;
  *)
    echo "usage: WORK=<dir> $0 prep|main" >&2
    exit 2
    ;;
esac
