#!/bin/sh
# Runner for the zio 3786083 -> 0299e57 A/B on nachos (io_uring), both arms
# with zio work_stealing scheduling (task migration on).
#
#   captures/zio-pin-ab-0299e57/run.sh prep   # export + build both arms into $WORK
#   captures/zio-pin-ab-0299e57/run.sh aa     # A/A noise floor: arm B against itself
#   captures/zio-pin-ab-0299e57/run.sh ab     # A/B: arm A against arm B
#
# # The arms
#
# - A (control): 908992b (zio 3786083) with d8e46d6 applied. 908992b alone
#   cannot cross-build from a Mac: its build.zig stops at configure time with
#   `macOS SDK workaround: libc file generated but 0 Compile steps received
#   it`. d8e46d6 only adds an early return to `applyMacosSdkLibc` when the
#   target is not macOS, so no Compile step of a Linux build changes.
# - B (candidate): 619242f (zio 0299e57, default -Dzio-scheduling=work_stealing).
#
# Between A and B the starh2 source under src/ differs only by the renames
# zio.ResetEvent -> zio.Event and resetHasWaiters -> eventHasWaiters. `check`
# below proves it mechanically. examples/bench_server.zig loses its
# task-migration flag, whose default was ON, so arm A runs with
# enable_task_migration=true and arm B with work_stealing.
#
# Both trees come from `git archive`, not from a checkout, so neither arm can
# pick up an uncommitted file. Both build with the same zig, the same flags and
# the same BoringSSL checkout.
#
# # Host
#
# nachos has had no h2load since its 2026-09-07 reinstall. This run uses a
# user-space build: nghttp2 v1.70.0 from the GitHub release tarball, configured
# with --enable-app against the system libnghttp2 1.70.0 and libev from
# archive.archlinux.org (libev-4.33-5), installed at H2LOAD below.
set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required: a scratch dir that holds the arm trees and binaries}
BSSL=${BSSL:-$HOME/Source/oss/http2-zig-hendrik/boringssl}
CERTS=${CERTS:-$REPO/testdata}
export HOST=${HOST:-ryan@100.113.184.27}
export H2LOAD=${H2LOAD:-/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load}
export REMOTE_DIR=${REMOTE_DIR:-/tmp/zio-pin-ab}
export CLIENT_BIN=${CLIENT_BIN:-$WORK/zioab-client}
# The brief: 2M requests at BOTH widths; 100k measures start-up only.
export ONESHOT_N=${ONESHOT_N:-2000000}
export ONESHOT_WIDE_N=${ONESHOT_WIDE_N:-2000000}
export ONESHOT_LAT=${ONESHOT_LAT:-1}
# 10 rounds: the p99 rows need 10 pairs, and rps and CPU need at least 6.
export PERF_ROUNDS=${PERF_ROUNDS:-10}
export CPU_ROUNDS=${CPU_ROUNDS:-10}
# 60 per arm: at a true fail rate of 1/10 the chance of 0/60 is 0.9^60 = 0.2%,
# and 6/60 against 0/60 is a one-sided Fisher p of about 0.014. At 20 per arm
# (0.9^20 = 12%) a 1-in-10 arm reads as clean one time in eight.
export BURST_ROUNDS=${BURST_ROUNDS:-60}

A_REF=908992b
A_FIX=d8e46d6
B_REF=619242f

case "${1:-}" in
  prep)
    rm -rf "$WORK/treeA" "$WORK/treeB"
    mkdir -p "$WORK/treeA" "$WORK/treeB"
    git -C "$REPO" archive "$A_REF" | tar -x -C "$WORK/treeA"
    git -C "$REPO" archive "$B_REF" | tar -x -C "$WORK/treeB"
    git -C "$REPO" format-patch -1 --stdout "$A_FIX" | (cd "$WORK/treeA" && patch -p1)
    for t in A B; do
      mkdir -p "$WORK/tree$t/testdata"
      cp "$CERTS/cert.pem" "$CERTS/key.pem" "$WORK/tree$t/testdata/"
      (cd "$WORK/tree$t" && ./zb build starh2-bench-server -Doptimize=ReleaseFast \
        -Dtarget=x86_64-linux-musl -Dboringssl-source-path="$BSSL" --prefix "$WORK/bins/out-$t")
    done
    # A/A ships the B binary under two names.
    for t in B1 B2; do
      mkdir -p "$WORK/bins/out-$t/bin"
      cp "$WORK/bins/out-B/bin/starh2-bench-server" "$WORK/bins/out-$t/bin/"
    done
    shasum -a 256 "$WORK"/bins/out-*/bin/starh2-bench-server
    ;;
  check)
    # src/ differs only by the two renames: undo them on the removed lines and
    # the result must equal the added lines.
    git -C "$REPO" diff "$A_REF" "$B_REF" -- src > "$WORK/src.diff"
    grep -E '^-[^-]' "$WORK/src.diff" | sed -E 's/^-//; s/ResetEvent/Event/g; s/resetHasWaiters/eventHasWaiters/g' > "$WORK/minus.txt"
    grep -E '^\+[^+]' "$WORK/src.diff" | sed -E 's/^\+//' > "$WORK/plus.txt"
    [ -s "$WORK/minus.txt" ] || { echo "empty src diff: the check did not run" >&2; exit 1; }
    diff "$WORK/minus.txt" "$WORK/plus.txt"
    echo "src diff $A_REF..$B_REF: $(wc -l < "$WORK/plus.txt") changed lines, all renames"
    ;;
  aa)
    ARMS="B1 B2" SAME_ZIO_OK=1 TREE_B1="$WORK/treeB" TREE_B2="$WORK/treeB" \
      BIN_ROOT="$WORK/bins" "$REPO/tools/zio-arm-ab.sh"
    ;;
  ab)
    ARMS="A B" TREE_A="$WORK/treeA" TREE_B="$WORK/treeB" \
      BIN_ROOT="$WORK/bins" "$REPO/tools/zio-arm-ab.sh"
    ;;
  *)
    echo "usage: WORK=<dir> $0 prep|check|aa|ab" >&2
    exit 2
    ;;
esac
