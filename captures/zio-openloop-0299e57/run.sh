#!/bin/sh
# The raw rows and logs this script reads or names are no longer in the repo.
# They are in the off-repo archive ~/Dropbox/starh2-captures/pr14-placement-followup-captures-3a6fad3.tar.gz
# (every file this branch added under captures/, as of 3a6fad3). Extract it at
# the repo root to restore them before rerunning.
# Open-loop latency on zio 0299e57: is the one-shot latency cost (M2, and
# pinned placement's +30% p50) real below saturation, or a property of
# measuring latency with a closed loop at saturation?
#
#   PIN_WORK=<bisect WORK> PLACE_WORK=<placement WORK> WORK=<dir> \
#     captures/zio-openloop-0299e57/run.sh prep
#   WORK=<dir> captures/zio-openloop-0299e57/run.sh main
#
# # Arms, and which starh2 tree each uses
#
#   A    3786083 reference (908992b + d8e46d6)
#   WS   0299e57 stock, 619242f tree                  (the control)
#   WS2  the WS binary again                          (A/A band at each load)
#   WSR  0299e57 without the #718 peek, 619242f tree
#   WSL  0299e57 stock, work_stealing, 1080022 tree   (same-tree control for PL/PA)
#   PL   0299e57 pinned, 1080022, --spawn-placement local
#   PA   the PL binary, --spawn-placement auto
#
# WS and WSR use the 619242f tree so that #718 is the only difference between
# them: on 1080022 the tree changes how much removing #718 helps (t-2498).
# PL and PA need 1080022 (it has --spawn-placement), so their same-tree
# control is WSL. Every binary here was built earlier; prep only copies them:
# A, WS, WSR from captures/zio-pin-bisect-0299e57 (bins out-A, out-B,
# out-unpeek) and WSL, PL, PA from captures/zio-placement-ab-0299e57 (out-WS,
# out-P).
#
# # The instrument
#
# Open loop: h2load -c 50 -m 10 -t 12 --rps=<total/50> -D 6 --warm-up-time 1
# at 320k and 590k offered req/s, about 40% and 75% of WS's saturated rate at
# 2 executors. -t 12, not the closed loop's -t 4: client-probe.txt shows the
# client's own thread count moves the latency at a fixed offered load, most
# at 590k, so the client gets the most threads the -c 50 split allows without
# going below 4 connections per thread. In the same round every arm also runs
# the saturated closed-loop row (h2load -n 2000000 -c 50 -m 10 -t 4).
#
# h2load measures from when it sends a request. If the server falls behind,
# requests wait inside the client and that wait is not in the latency; the
# achieved-rate check (98% of offered) is what catches an arm that fell
# behind.
set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}

case "${1:-}" in
  prep)
    PIN_WORK=${PIN_WORK:?PIN_WORK is required}
    PLACE_WORK=${PLACE_WORK:?PLACE_WORK is required}
    for spec in A:$PIN_WORK/bins/out-A:$PIN_WORK/treeA \
                WS:$PIN_WORK/bins/out-B:$PIN_WORK/treeB \
                WS2:$PIN_WORK/bins/out-B:$PIN_WORK/treeB \
                WSR:$PIN_WORK/bins/out-unpeek:$PIN_WORK/tree-unpeek \
                WSL:$PLACE_WORK/bins/out-WS:$PLACE_WORK/tree-WS \
                PL:$PLACE_WORK/bins/out-P:$PLACE_WORK/tree-P \
                PA:$PLACE_WORK/bins/out-P:$PLACE_WORK/tree-P; do
      arm=${spec%%:*}; rest=${spec#*:}; bin=${rest%%:*}; tree=${rest#*:}
      rm -rf "$WORK/bins/out-$arm"; mkdir -p "$WORK/bins/out-$arm/bin"
      cp "$bin/bin/starh2-bench-server" "$WORK/bins/out-$arm/bin/"
      ln -sfn "$tree" "$WORK/tree-$arm"
    done
    shasum -a 256 "$WORK"/bins/out-*/bin/starh2-bench-server
    ;;
  main|balanced|fixed)
    # main: the first session, 7 arms, 10 rounds, cyclic rotation (each arm
    # kept its neighbours, so a pair ran in one order 8-9 rounds of 10).
    # balanced: the rerun after rotate() began reversing even rounds; 6 arms
    # (A dropped), 12 rounds. That reversal is balanced only for an odd arm
    # count, so with 6 arms WS still ran before WS2, WSL and PA in 8 of 12.
    # fixed: the same 6 arms and 12 rounds after rotate() switched to blocks
    # of n forward then n reversed rotations; rotate-check.sh proves every
    # ordered pair runs 6 times each way and every arm sits in every slot
    # twice. This is the session the report reads first.
    if [ "$1" = balanced ] || [ "$1" = fixed ]; then
      SESSION_ARMS="WS WS2 WSR WSL PL PA"; SESSION_ROUNDS=12
    else
      SESSION_ARMS="A WS WS2 WSR WSL PL PA"; SESSION_ROUNDS=10
    fi
    OLD_READY='"batch_wake_sleepers":1}'
    NEW_WS='"zio_scheduling":"work_stealing","spawn_placement":"auto"'
    export ARGS_PL='--spawn-placement local' ARGS_PA='--spawn-placement auto'
    export EXPECT_WS="$OLD_READY" EXPECT_WS2="$OLD_READY" EXPECT_WSR="$OLD_READY"
    export EXPECT_WSL="$NEW_WS"
    export EXPECT_PL='"zio_scheduling":"pinned","spawn_placement":"local"'
    export EXPECT_PA='"zio_scheduling":"pinned","spawn_placement":"auto"'
    for arm in A WS WS2 WSR WSL PL PA; do export "TREE_$arm=$WORK/tree-$arm"; done
    ARMS="$SESSION_ARMS" SAME_ZIO_OK=1 BIN_ROOT="$WORK/bins" \
      HOST=${HOST:-ryan@100.113.184.27} \
      H2LOAD=${H2LOAD:-/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load} \
      REMOTE_DIR=${REMOTE_DIR:-/tmp/zio-openloop} CLIENT_BIN="$WORK/zioab-client" \
      IDLE_MAX_CORES=0.5 PROC_MAX_CORES=0.3 HOST_WAIT_MAX=${HOST_WAIT_MAX:-14400} ROUND_CHECK=1 \
      HOST_BLOCK_RE='^(WowB?\.exe|cc1plus|cc1|mod-tests|zig)$' \
      PHASES=5 OPEN_ROUNDS=$SESSION_ROUNDS OPEN_RATES="320000 590000" OPEN_SECONDS=6 OPEN_WARMUP=1 \
      OPEN_THREADS=12 OPEN_CLOSED=1 ONESHOT_N=2000000 EXECUTORS=2 \
      "$REPO/tools/zio-arm-ab.sh"
    ;;
  *)
    echo "usage: WORK=<dir> $0 prep|main|balanced|fixed" >&2
    exit 2
    ;;
esac
