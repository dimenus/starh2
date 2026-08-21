#!/bin/sh
# Where the CPU goes, per arm, on the conns50 oneshot shape (Linux + perf).
#
# The harness rows say starh2 spends about 1.8x the CPU of hyper for the
# same rps on the h2load shape (tools/sse_bench/mixed.sh CONNS=50). This
# script attaches `perf record -g` to each server for exactly the client
# window and reports three things the rps row cannot: the user/sys CPU
# split from /proc/<pid>/stat, the context-switch count summed over every
# thread of the process, and the top symbols by self time. Kernel samples
# need perf_event_paranoid <= 1 or root; the script uses sudo for perf
# only, and refuses to run when sudo needs a password.
#
# ARMS="starh2 hyper" (default) picks the servers. OPPONENT_WIDTH applies
# as in run.sh/mixed.sh: the opponent is pinned to starh2's executor
# count. Output under $OUT: <arm>.perf.data, <arm>.report.txt (symbols),
# <arm>.dso.txt (user vs kernel vs libc split), and a summary on stdout.
set -u

REPO=$(cd "$(dirname "$0")/../.." && pwd -P)
. "$REPO/tools/bench_lock.sh"
. "$REPO/tools/sse_bench/hyper_build.sh"
. "$REPO/tools/sse_bench/arm_width.sh"
CONNS=${CONNS:-50}
ONESHOT_WORKERS=${ONESHOT_WORKERS:-500}
SECONDS_RUN=${SECONDS_RUN:-5}
WARMUP=${WARMUP:-1}
STARH2_EXECUTORS=${STARH2_EXECUTORS:-auto}
ARMS=${ARMS:-"starh2 hyper"}
PERF_HZ=${PERF_HZ:-999}
OUT=${OUT:-/tmp/starh2-perf-arms}

[ "$(uname -s)" = Linux ] || { echo "perf-arms.sh is Linux-only (perf)" >&2; exit 1; }
command -v perf >/dev/null 2>&1 || { echo "perf is required; it is not installed" >&2; exit 1; }
sudo -n true 2>/dev/null || { echo "sudo without a password is required for perf kernel samples" >&2; exit 1; }
command -v go >/dev/null 2>&1 || { echo "go is required for the client" >&2; exit 1; }

mkdir -p "$OUT"
cd "$REPO/tools/sse_bench"
go build -o "$OUT/sse-client" ./client.go || exit 1
cd "$REPO"
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/starh2" || exit 1
STARH2="$OUT/starh2/bin/starh2-bench-server"
build_hyper || exit 1
HYPER="$OUT/hyper/sse-hyper"

# starh2 starts first in every case so its width is known for the pin.
EXECUTOR_ARGS=
if [ "$STARH2_EXECUTORS" != auto ]; then
  EXECUTOR_ARGS="--executors $STARH2_EXECUTORS"
fi
# Same knob as mixed.sh: STARH2_TASK_MIGRATION=0 runs the arm with zio task
# migration off, to separate migration from cross-executor handoff in the
# context-switch count.
MIGRATION_ARGS=
if [ "${STARH2_TASK_MIGRATION:-1}" = 0 ]; then
  MIGRATION_ARGS=--no-task-migration
fi

proc_cpu() {
  # utime stime in clock ticks (fields 14 15), summed over the process.
  awk '{ printf "%d %d\n", $14, $15 }' "/proc/$1/stat"
}
proc_ctxt() {
  cat /proc/"$1"/task/*/status 2>/dev/null | awk '
    /^voluntary_ctxt_switches/ { v += $2 }
    /^nonvoluntary_ctxt_switches/ { n += $2 }
    END { printf "%d %d\n", v, n }'
}
hz=$(getconf CLK_TCK)

measure() {
  arm=$1; pid=$2; port=$3
  if ! "$OUT/sse-client" -streams 0 -oneshot-url "https://127.0.0.1:$port/" -oneshot-workers 1 -seconds 1 -warmup 0 -label probe >/dev/null 2>&1; then
    echo "$arm on :$port did not answer GET / — refusing to report" >&2
    return 1
  fi
  cpu0=$(proc_cpu "$pid"); ctx0=$(proc_ctxt "$pid")
  sudo perf record -F "$PERF_HZ" -g -p "$pid" -o "$OUT/$arm.perf.data" >"$OUT/$arm.perf.log" 2>&1 &
  perf_pid=$!
  sleep 0.5
  "$OUT/sse-client" -streams 0 -conns "$CONNS" -oneshot-url "https://127.0.0.1:$port/" \
    -oneshot-workers "$ONESHOT_WORKERS" -seconds "$SECONDS_RUN" -warmup "$WARMUP" -label "$arm" | sed 's/^/  /'
  sudo kill -INT "$perf_pid" 2>/dev/null
  wait "$perf_pid" 2>/dev/null
  cpu1=$(proc_cpu "$pid"); ctx1=$(proc_ctxt "$pid")
  # Fields: utime0 stime0 utime1 stime1 vol0 nonvol0 vol1 nonvol1 (ticks / counts).
  echo "$cpu0 $cpu1 $ctx0 $ctx1" | tee "$OUT/$arm.raw.txt" | awk -v hz="$hz" -v arm="$arm" '{
    u = ($3 - $1) / hz; s = ($4 - $2) / hz
    printf "  %-8s CPU user=%.2fs sys=%.2fs total=%.2fs  ctxt voluntary=%d nonvoluntary=%d\n", arm, u, s, u + s, $7 - $5, $8 - $6 }'
  # perf report as root (kernel symbols need it); -f because the file was
  # written by a different perf session than the one that reads it.
  sudo perf report -f -i "$OUT/$arm.perf.data" --stdio --no-children --sort dso -g none 2>/dev/null \
    | grep -v '^#' | grep -v '^$' | head -8 > "$OUT/$arm.dso.txt"
  sudo perf report -f -i "$OUT/$arm.perf.data" --stdio --no-children --sort dso,symbol -g none 2>/dev/null \
    | grep -v '^#' | grep -v '^$' > "$OUT/$arm.report.txt"
  sudo perf report -f -i "$OUT/$arm.perf.data" --stdio --no-children --sort symbol -g none --dsos '[kernel.kallsyms]' 2>/dev/null \
    | grep -v '^#' | grep -v '^$' > "$OUT/$arm.kernel.txt"
  sudo chown "$(id -u)" "$OUT/$arm".*
  echo "  $arm by dso:"; sed 's/^/    /' "$OUT/$arm.dso.txt"
  echo "  $arm top symbols (self):"; head -30 "$OUT/$arm.report.txt" | cut -c1-160 | sed 's/^/    /'
  echo "  $arm top kernel symbols:"; head -15 "$OUT/$arm.kernel.txt" | cut -c1-120 | sed 's/^/    /'
}

bench_lock
PIDS=
cleanup() { kill $PIDS 2>/dev/null; wait 2>/dev/null; bench_unlock; }
trap cleanup EXIT INT TERM

"$STARH2" --mode tls --port 19460 --sse-interval-ms 10 $EXECUTOR_ARGS $MIGRATION_ARGS > "$OUT/starh2.log" 2>&1 &
S_PID=$!; PIDS="$S_PID"
STARH2_WIDTH=$(starh2_width "$OUT/starh2.log") || exit 1
WIDTH=$(opponent_width "$STARH2_WIDTH")
PIN=$(width_env "$WIDTH")
echo "== perf-arms: $CONNS conns, $ONESHOT_WORKERS workers, ${SECONDS_RUN}s + ${WARMUP}s warmup, perf ${PERF_HZ}Hz -g"
echo "== starh2 executors=$STARH2_EXECUTORS (=$STARH2_WIDTH) migration=${STARH2_TASK_MIGRATION:-1}  opponents width=$WIDTH  arms: $ARMS"

for arm in $ARMS; do
  echo "--- $arm ---"
  case $arm in
    starh2) measure starh2 "$S_PID" 19460 || exit 1 ;;
    hyper)
      $PIN "$HYPER" -port 19461 -sse-interval-ms 10 -cert testdata/cert.pem -key testdata/key.pem > "$OUT/hyper.log" 2>&1 &
      H_PID=$!; PIDS="$PIDS $H_PID"
      check_widths_one=$(json_int "$(wait_ready "$OUT/hyper.log")" width)
      if [ "$WIDTH" != default ] && [ "$check_widths_one" != "$WIDTH" ]; then
        echo "hyper width=$check_widths_one did not pin to $WIDTH — refusing to report" >&2; exit 1
      fi
      measure hyper "$H_PID" 19461 || exit 1 ;;
    *) echo "unknown arm $arm" >&2; exit 1 ;;
  esac
done
cleanup
trap - EXIT INT TERM
