#!/bin/sh
# Catch a TLS stall and sample BOTH processes' threads with sample(1).
#
# PROTOCOL
#
#   tools/stall-thread-sample.sh [--mode tls|h2c] [--rounds R] [--after SEC]
#     [-n N] [-c C] [-m M] [-t T] [--bin PATH] [--out DIR] [--no-task-migration]
#
# It runs h2load rounds until one round passes --after seconds, then samples
# the server's threads, the client's threads and the sockets, and stops.
#
# WHY THIS EXISTS, AND WHAT IT ANSWERS THAT NOTHING ELSE DOES
#
#   netstat says what the KERNEL holds. It cannot see a thread. A profile of a
#   healthy run cannot see blocked time at all. sample(1) reads every thread's
#   stack in a live process without changing one line of production code, so it
#   separates three failures that look identical from the outside:
#
#     all threads in kevent          -> nothing is runnable. A lost wakeup, or
#                                       work stranded inside the process.
#     a thread in a socket read,
#       with a non-zero server Recv-Q -> readiness was lost below starh2.
#     threads in __psynch/__ulock    -> lock contention or a deadlock.
#     a thread burning CPU in starh2 -> a spin, not a park.
#
#   The whole point is that these need different fixes and the h2load output
#   cannot tell them apart.
#
# WHAT IT CANNOT DO. A suspended coroutine sits on no thread stack, so this
# names the RUNTIME state, never the await point of a parked task. Use the
# in-process diagnostics for that.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

mode=tls
rounds=80
after=6
n=100000
c=50
m=10
t=4
migrate=1
bin=${STARH2_BENCH_BIN:-/tmp/starh2-stall-delta/bin/starh2-bench-server}
out=/tmp/starh2-thread-sample

while [ $# -gt 0 ]; do
  case $1 in
    --mode) mode=$2; shift 2 ;;
    --rounds) rounds=$2; shift 2 ;;
    --after) after=$2; shift 2 ;;
    -n) n=$2; shift 2 ;;
    -c) c=$2; shift 2 ;;
    -m) m=$2; shift 2 ;;
    -t) t=$2; shift 2 ;;
    --bin) bin=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    --no-task-migration) migrate=0; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[ -x "$bin" ] || { echo "FAIL: no bench server at $bin" >&2; exit 2; }
command -v h2load >/dev/null 2>&1 || { echo "FAIL: h2load is not on PATH" >&2; exit 2; }
[ -x /usr/bin/sample ] || { echo "FAIL: /usr/bin/sample is missing" >&2; exit 2; }

rm -rf "$out"; mkdir -p "$out"

server_args="--mode $mode --port 0 --cert $ROOT/testdata/cert.pem --key $ROOT/testdata/key.pem"
[ "$migrate" -eq 1 ] && server_args="$server_args --task-migration"

"$bin" $server_args >"$out/server.log" 2>&1 &
spid=$!
trap 'kill $spid 2>/dev/null || true' EXIT INT TERM

i=0
port=""
while [ "$i" -lt 100 ]; do
  port=$(sed -n 's/.*"port":\([0-9][0-9]*\).*/\1/p' "$out/server.log" 2>/dev/null | head -1)
  [ -n "$port" ] && [ "$port" -gt 0 ] && break
  kill -0 "$spid" 2>/dev/null || { echo "FAIL: server died during bind" >&2; cat "$out/server.log" >&2; exit 1; }
  i=$((i + 1)); sleep 0.1
done
[ -n "$port" ] || { echo "FAIL: server never bound" >&2; exit 1; }

if [ "$mode" = tls ]; then url="https://127.0.0.1:$port/"; else url="http://127.0.0.1:$port/"; fi
echo "server pid=$spid port=$port out=$out"

r=1
caught=0
while [ "$r" -le "$rounds" ]; do
  start=$(date +%s)
  h2load -n "$n" -c "$c" -m "$m" -t "$t" "$url" >"$out/round$r.h2load" 2>&1 &
  hpid=$!
  sampled=0
  while kill -0 "$hpid" 2>/dev/null; do
    waited=$(($(date +%s) - start))
    if [ "$waited" -ge "$after" ] && [ "$sampled" -eq 0 ]; then
      sampled=1
      caught=1
      echo "  round $r passed ${waited}s; sampling"
      /usr/bin/sample "$spid" 3 1 -file "$out/round$r.server.sample" >/dev/null 2>&1 || true
      /usr/bin/sample "$hpid" 2 1 -file "$out/round$r.client.sample" >/dev/null 2>&1 || true
      netstat -anv -p tcp 2>/dev/null | awk -v sp="$spid" -v pat="[.]$port\$" '
        $6 == "ESTABLISHED" && ($4 ~ pat || $5 ~ pat) {
          printf "%-6s recvq=%-8s sendq=%-8s local=%-22s foreign=%-22s pid=%s\n",
            ($11 == sp ? "server" : "client"), $2, $3, $4, $5, $11
          rows++ }
        END { printf "# rows=%d\n", rows+0 }' >"$out/round$r.sockets"
    fi
    sleep 0.3
  done
  wait "$hpid" 2>/dev/null || true
  if [ "$sampled" -eq 1 ]; then
    echo "  round $r $(awk '/^requests:/{print;exit}' "$out/round$r.h2load")"
    echo "  $(awk '/^finished in/{print;exit}' "$out/round$r.h2load")"
    break
  fi
  r=$((r + 1))
done

if [ "$caught" -eq 0 ]; then
  echo "NO ROUND passed ${after}s in $rounds rounds. Nothing was sampled."
  exit 2
fi

# Report what was covered. A sample with no stacks must be loud, not silent.
sfile="$out/round$r.server.sample"
threads=$(grep -c "^ *[0-9][0-9]* Thread_" "$sfile" 2>/dev/null || echo 0)
if [ "$threads" -eq 0 ]; then
  echo "FAIL: the server sample captured 0 threads. See $sfile" >&2
  exit 1
fi
echo "CAUGHT round $r; server threads sampled=$threads"
echo "--- server, sorted by top of stack ---"
awk '/^Sort by top of stack/{f=1;next} f&&/^$/{exit} f' "$sfile"
echo "--- sockets ---"
cat "$out/round$r.sockets"
echo "full stacks: $sfile"
