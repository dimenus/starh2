#!/bin/sh
# Where the CPU goes, per arm, on a oneshot or an SSE shape (Linux + perf).
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
# SHAPE=oneshot (default) is the conns50 oneshot shape above. SHAPE=sse
# opens STREAMS (default 200) SSE streams over CONNS (default 1) and the
# servers send one event per INTERVAL ms (default 10) on each; the per-unit
# lines then divide by delivered events instead of requests. The perf
# window also covers stream opening and warmup, and the client counts only
# the measurement window, so per-unit numbers read a little high; the bias
# is the same for every arm.
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
SHAPE=${SHAPE:-oneshot}
case $SHAPE in
  oneshot) CONNS=${CONNS:-50} ;;
  sse) CONNS=${CONNS:-1} ;;
  *) echo "SHAPE must be oneshot or sse, not $SHAPE" >&2; exit 2 ;;
esac
STREAMS=${STREAMS:-200}
INTERVAL=${INTERVAL:-10}
ONESHOT_WORKERS=${ONESHOT_WORKERS:-500}
SECONDS_RUN=${SECONDS_RUN:-5}
WARMUP=${WARMUP:-1}
STARH2_EXECUTORS=${STARH2_EXECUTORS:-auto}
ARMS=${ARMS:-"starh2 hyper"}
PERF_HZ=${PERF_HZ:-999}
# Tracepoints counted over the client window (see the syscalls-per-request
# lines). Parks end in io_uring_enter (zio), epoll_wait (tokio) or futex.
SYSCALL_EVENTS=${SYSCALL_EVENTS:-syscalls:sys_enter_io_uring_enter,syscalls:sys_enter_epoll_wait,syscalls:sys_enter_epoll_pwait,syscalls:sys_enter_futex,syscalls:sys_enter_sendmsg,syscalls:sys_enter_sendto,syscalls:sys_enter_write,syscalls:sys_enter_writev,syscalls:sys_enter_recvfrom,syscalls:sys_enter_recvmsg,syscalls:sys_enter_read,syscalls:sys_enter_readv}
OUT=${OUT:-/tmp/starh2-perf-arms}

[ "$(uname -s)" = Linux ] || { echo "perf-arms.sh is Linux-only (perf)" >&2; exit 1; }
command -v perf >/dev/null 2>&1 || { echo "perf is required; it is not installed" >&2; exit 1; }
sudo -n true 2>/dev/null || { echo "sudo without a password is required for perf kernel samples" >&2; exit 1; }
command -v go >/dev/null 2>&1 || { echo "go is required for the client" >&2; exit 1; }

# A fresh clone has no testdata/*.pem (gitignored, generated). Without it
# every TLS arm dies before its ready line and the harness would only say
# "no ready line", so check first and print the command that creates it.
for f in testdata/cert.pem testdata/key.pem; do
  [ -f "$REPO/$f" ] || {
    echo "$f is missing; create it with:" >&2
    echo "  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout testdata/key.pem -out testdata/cert.pem -days 365 -nodes -subj '/CN=localhost'" >&2
    exit 1
  }
done
mkdir -p "$OUT"
cd "$REPO/tools/sse_bench"
go build -o "$OUT/sse-client" ./client.go || exit 1
# zio scheduling is a build option, not a server flag, so an A/B builds two
# binaries. STARH2_ZIO_SCHEDULING=pinned builds the migration-off arm.
if [ -n "${STARH2_TASK_MIGRATION:-}" ]; then
  echo "FAIL: STARH2_TASK_MIGRATION is gone. zio scheduling is the build option -Dzio-scheduling: set STARH2_ZIO_SCHEDULING=pinned for migration off (default work_stealing)." >&2
  exit 2
fi
STARH2_ZIO_SCHEDULING=${STARH2_ZIO_SCHEDULING:-work_stealing}
cd "$REPO"
./zb build starh2-bench-server -Doptimize=fast -Dzio-scheduling="$STARH2_ZIO_SCHEDULING" --prefix "$OUT/starh2" || exit 1
STARH2="$OUT/starh2/bin/starh2-bench-server"
build_hyper || exit 1
HYPER="$OUT/hyper/sse-hyper"

# starh2 starts first in every case so its width is known for the pin.
EXECUTOR_ARGS=
if [ "$STARH2_EXECUTORS" != auto ]; then
  EXECUTOR_ARGS="--executors $STARH2_EXECUTORS"
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
  # Syscall counts over the same window: how a voluntary context switch
  # ends (io_uring_enter vs epoll vs futex) and how many socket syscalls a
  # request costs. Tracepoints need root, hence sudo.
  sudo perf stat -x, -e "$SYSCALL_EVENTS" -p "$pid" -o "$OUT/$arm.stat.csv" >/dev/null 2>&1 &
  stat_pid=$!
  sleep 0.5
  case $SHAPE in
    oneshot)
      "$OUT/sse-client" -streams 0 -conns "$CONNS" -oneshot-url "https://127.0.0.1:$port/" \
        -oneshot-workers "$ONESHOT_WORKERS" -seconds "$SECONDS_RUN" -warmup "$WARMUP" -label "$arm" | tee "$OUT/$arm.client.txt" | sed 's/^/  /' ;;
    sse)
      "$OUT/sse-client" -url "https://127.0.0.1:$port/sse" -streams "$STREAMS" -conns "$CONNS" \
        -seconds "$SECONDS_RUN" -warmup "$WARMUP" -label "$arm" | tee "$OUT/$arm.client.txt" | sed 's/^/  /' ;;
  esac
  sudo kill -INT "$perf_pid" 2>/dev/null
  sudo kill -INT "$stat_pid" 2>/dev/null
  wait "$perf_pid" 2>/dev/null
  wait "$stat_pid" 2>/dev/null
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
  # perf stat -x, rows: count,unit,event,...; units = the client's ok count
  # (oneshot) or delivered events (sse).
  case $SHAPE in
    oneshot) unit=req; units=$(grep -m1 'oneshot ok=' "$OUT/$arm.client.txt" 2>/dev/null | sed 's/.*ok=\([0-9]*\).*/\1/') ;;
    sse) unit=event; units=$(grep -m1 ' streams=.* events=' "$OUT/$arm.client.txt" 2>/dev/null | sed 's/.*events=\([0-9]*\).*/\1/') ;;
  esac
  if [ -z "$units" ] || [ "$units" -eq 0 ]; then
    echo "$arm: the client reported no $unit count — refusing to report per-$unit numbers" >&2
    return 1
  fi
  awk -v hz="$hz" -v n="$units" -v unit="$unit" '{
    printf "    per %s: CPU=%.2fus voluntary ctxt=%.3f\n", unit, (($3 - $1) + ($4 - $2)) / hz * 1e6 / n, ($7 - $5) / n }' "$OUT/$arm.raw.txt"
  echo "  $arm syscalls per $unit (window, $unit count=$units):"
  sudo chown "$(id -u)" "$OUT/$arm.stat.csv" 2>/dev/null
  awk -F, -v n="$units" -v unit="$unit" '$1 ~ /^[0-9]+$/ {
    name = $3; sub(/^syscalls:sys_enter_/, "", name)
    printf "    %-16s %10d  %6.3f/%s\n", name, $1, $1 / n, unit }' "$OUT/$arm.stat.csv"
}

bench_lock
PIDS=
cleanup() { kill $PIDS 2>/dev/null; wait 2>/dev/null; bench_unlock; }
trap cleanup EXIT INT TERM

# STARH2_EXTRA_ARGS passes any further bench-server flags (A/B knobs).
"$STARH2" --mode tls --port 19460 --sse-interval-ms "$INTERVAL" $EXECUTOR_ARGS ${STARH2_EXTRA_ARGS:-} > "$OUT/starh2.log" 2>&1 &
S_PID=$!; PIDS="$S_PID"
STARH2_WIDTH=$(starh2_width "$OUT/starh2.log") || exit 1
WIDTH=$(opponent_width "$STARH2_WIDTH")
PIN=$(width_env "$WIDTH")
case $SHAPE in
  oneshot) echo "== perf-arms: oneshot, $CONNS conns, $ONESHOT_WORKERS workers, ${SECONDS_RUN}s + ${WARMUP}s warmup, perf ${PERF_HZ}Hz -g" ;;
  sse) echo "== perf-arms: sse, $STREAMS streams over $CONNS conns, one event per ${INTERVAL}ms per stream, ${SECONDS_RUN}s + ${WARMUP}s warmup, perf ${PERF_HZ}Hz -g" ;;
esac
echo "== starh2 executors=$STARH2_EXECUTORS (=$STARH2_WIDTH) zio-scheduling=$STARH2_ZIO_SCHEDULING extra=[${STARH2_EXTRA_ARGS:-}]  opponents width=$WIDTH  arms: $ARMS"
echo "== starh2 ready: $(grep -m1 ready "$OUT/starh2.log")"

for arm in $ARMS; do
  echo "--- $arm ---"
  case $arm in
    starh2) measure starh2 "$S_PID" 19460 || exit 1 ;;
    hyper)
      $PIN "$HYPER" -port 19461 -sse-interval-ms "$INTERVAL" -cert testdata/cert.pem -key testdata/key.pem > "$OUT/hyper.log" 2>&1 &
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
