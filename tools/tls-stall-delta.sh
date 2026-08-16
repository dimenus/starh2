#!/bin/sh
# Repeated-h2load lifecycle probe for the intermittent TLS stall.
# It does not change production code. It only starts servers and runs h2load.
#
# PROTOCOL
#
#   tools/tls-stall-delta.sh NAME [options]
#
#   --mode tls|h2c        transport under test (default tls)
#   --task-migration      pass --task-migration to the server
#   --executors N         pass --executors N to the server
#   --rounds R            rounds PER ARM (default 8)
#   -n -c -m -t           h2load request/connection/stream/thread counts
#   -N SEC / -T SEC       h2load inactivity / active timeouts
#   --bin PATH            arm A binary (default $STARH2_BENCH_BIN)
#   --bin-b PATH          arm B binary; turns on the A/B compare mode
#   --label-a NAME        arm A label (default base)
#   --label-b NAME        arm B label (default fix)
#   --sample-after SEC    first socket sample (default 4)
#   --sample-every SEC    later socket samples (default 8)
#   --allow-stale         permit a binary older than the sources
#
# WHY EACH GUARD EXISTS. Every one of these replaced an instruction that a
# human had to remember, and each has produced a wrong answer here before.
#
#   Port 0 always.      The server prints its bound port. A fixed port lets a
#                       leftover server answer for the build under test.
#   Binary identity.    The RESULT line carries the sha256 of every arm. Two
#                       arms that resolve to one file is a hard failure, so a
#                       "control" cannot silently be the build under test.
#   Freshness.          A binary older than a source file fails the run. A
#                       forgotten install otherwise measures the old code.
#   Compare mode.       Arms alternate round by round in one session, so a
#                       fixed order cannot manufacture the result, and the
#                       control proves the stall still reproduces TODAY.
#   Inconclusive.       A compare run whose control never stalled exits 2. A
#                       clean arm B is not evidence when the instrument did
#                       not fire.
#   Signature split.    Rounds are classified by h2load's REQUEST ACCOUNTING,
#                       not by duration. `stall` needs every request started
#                       and done; `not-started` means the client never
#                       submitted the rest. They are different defects. See
#                       the classification comment in run_round for the
#                       captured evidence.
#   Socket sides.       netstat field 11 is the owning pid, so each row is
#                       labelled server or client. An empty sample is loud.
#   No python3 poll.    The old loop started 10 python processes a second on
#                       the box under test, next to a timing-sensitive bug.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

if [ $# -lt 1 ]; then
  sed -n '2,30p' "$0" >&2
  exit 2
fi

name=$1
shift
mode=tls
migrate=0
executors=""
rounds=8
n=100000
c=50
m=10
t=4
inact=""
active=""
bin_a=${STARH2_BENCH_BIN:-/tmp/starh2-stall-delta/bin/starh2-bench-server}
bin_b=""
label_a=base
label_b=fix
sample_after=4
sample_every=8
allow_stale=0

while [ $# -gt 0 ]; do
  case $1 in
    --mode) mode=$2; shift 2 ;;
    --task-migration) migrate=1; shift ;;
    --executors) executors=$2; shift 2 ;;
    --rounds) rounds=$2; shift 2 ;;
    -n) n=$2; shift 2 ;;
    -c) c=$2; shift 2 ;;
    -m) m=$2; shift 2 ;;
    -t) t=$2; shift 2 ;;
    -N) inact=$2; shift 2 ;;
    -T) active=$2; shift 2 ;;
    --bin) bin_a=$2; shift 2 ;;
    --bin-b) bin_b=$2; shift 2 ;;
    --label-a) label_a=$2; shift 2 ;;
    --label-b) label_b=$2; shift 2 ;;
    --sample-after) sample_after=$2; shift 2 ;;
    --sample-every) sample_every=$2; shift 2 ;;
    --allow-stale) allow_stale=1; shift ;;
    --port) echo "FAIL $name: --port is gone. The harness always binds port 0." >&2; exit 2 ;;
    *) echo "FAIL $name: unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [ "$rounds" -lt 1 ]; then
  echo "FAIL $name: --rounds must be at least 1" >&2
  exit 2
fi

command -v h2load >/dev/null 2>&1 || { echo "FAIL $name: h2load is not on PATH" >&2; exit 2; }
[ -f testdata/cert.pem ] || { echo "FAIL $name: testdata/cert.pem is missing" >&2; exit 2; }
[ -f testdata/key.pem ] || { echo "FAIL $name: testdata/key.pem is missing" >&2; exit 2; }

outdir=/tmp/starh2-stall-$name
rm -rf "$outdir"
mkdir -p "$outdir"

# ---------------------------------------------------------------- binaries

sha_of() {
  shasum -a 256 "$1" | awk '{print substr($1,1,16)}'
}

bin_sha=""
describe_bin() {
  # $1 path, $2 label. Sets bin_sha. Fails when the file is missing or stale.
  _p=$1
  _l=$2
  if [ ! -x "$_p" ]; then
    echo "FAIL $name: arm $_l binary is missing or not executable: $_p" >&2
    echo "  build it with: ./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix <dir>" >&2
    exit 2
  fi
  bin_sha=$(sha_of "$_p")
  _size=$(LC_ALL=C stat -f '%z' "$_p")
  _mtime=$(LC_ALL=C stat -f '%Sm' -t '%Y-%m-%dT%H:%M:%S' "$_p")
  _newer=$(find src examples build.zig build.zig.zon -type f -newer "$_p" -print 2>/dev/null | head -3 || true)
  if [ -n "$_newer" ]; then
    echo "BIN $_l sha=$bin_sha size=$_size mtime=$_mtime STALE"
    printf '  newer source: %s\n' $_newer
    if [ "$allow_stale" -ne 1 ]; then
      echo "FAIL $name: arm $_l binary predates a source file. Reinstall it, or pass --allow-stale." >&2
      exit 2
    fi
  else
    echo "BIN $_l sha=$bin_sha size=$_size mtime=$_mtime fresh"
  fi
}

describe_bin "$bin_a" "$label_a"
sha_a=$bin_sha
compare=0
sha_b=""
if [ -n "$bin_b" ]; then
  compare=1
  describe_bin "$bin_b" "$label_b"
  sha_b=$bin_sha
  if [ "$sha_a" = "$sha_b" ]; then
    echo "FAIL $name: both arms resolve to the same binary (sha=$sha_a). A control must differ." >&2
    exit 2
  fi
fi

# ------------------------------------------------------------------ server

server_args="--mode $mode --port 0 --cert $ROOT/testdata/cert.pem --key $ROOT/testdata/key.pem"
if [ -n "$executors" ]; then
  server_args="$server_args --executors $executors"
fi
if [ "$migrate" -eq 1 ]; then
  server_args="$server_args --task-migration"
fi
if [ "${STARH2_DIAG:-0}" = 1 ]; then
  server_args="$server_args --diag"
fi

h2_args="-n $n -c $c -m $m -t $t"
if [ -n "$inact" ]; then
  h2_args="$h2_args -N $inact"
fi
if [ -n "$active" ]; then
  h2_args="$h2_args -T $active"
fi

srv_pid=""
srv_port=""
start_server() {
  # $1 binary, $2 label. Sets srv_pid and srv_port. Runs in this shell so
  # that $! stays valid for the trap.
  _bin=$1
  _l=$2
  _log="$outdir/$_l.server.log"
  : >"$_log"
  "$_bin" $server_args >"$_log" 2>&1 &
  srv_pid=$!
  srv_port=""
  _i=0
  while [ "$_i" -lt 100 ]; do
    if grep -q '"ready":true' "$_log" 2>/dev/null; then
      srv_port=$(sed -n 's/.*"port":\([0-9][0-9]*\).*/\1/p' "$_log" | head -1)
      if [ -n "$srv_port" ] && [ "$srv_port" -gt 0 ]; then
        return 0
      fi
    fi
    if ! kill -0 "$srv_pid" 2>/dev/null; then
      echo "FAIL $name: server $_l died during bind" >&2
      cat "$_log" >&2
      exit 1
    fi
    _i=$((_i + 1))
    sleep 0.1
  done
  echo "FAIL $name: server $_l never printed a ready line with a port" >&2
  cat "$_log" >&2
  exit 1
}

pid_a=""
pid_b=""
cleanup() {
  for _p in $pid_a $pid_b; do
    kill "$_p" 2>/dev/null || true
  done
  for _p in $pid_a $pid_b; do
    wait "$_p" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

start_server "$bin_a" "$label_a"
pid_a=$srv_pid
port_a=$srv_port
if [ "$compare" -eq 1 ]; then
  start_server "$bin_b" "$label_b"
  pid_b=$srv_pid
  port_b=$srv_port
fi

# ------------------------------------------------------------------ probes

url_for() {
  if [ "$mode" = tls ]; then
    echo "https://127.0.0.1:$1/"
  else
    echo "http://127.0.0.1:$1/"
  fi
}

sample_sockets() {
  # $1 server pid, $2 port, $3 seconds waited, $4 arm label, $5 round
  _spid=$1
  _sport=$2
  _waited=$3
  _l=$4
  _r=$5
  _dest="$outdir/$_l.round$_r.sockets"
  {
    echo "# t=${_waited}s arm=$_l round=$_r port=$_sport server_pid=$_spid"
    netstat -anv -p tcp 2>/dev/null | awk -v sp="$_spid" -v pat="[.]$_sport\$" '
      $6 == "ESTABLISHED" && ($4 ~ pat || $5 ~ pat) {
        side = ($11 == sp) ? "server" : "client"
        printf "%-6s recvq=%-8s sendq=%-8s local=%-24s foreign=%-24s pid=%s\n", side, $2, $3, $4, $5, $11
        rows++
        if (side == "server") { srows++; srq += $2; ssq += $3 } else { crows++; crq += $2; csq += $3 }
      }
      END {
        printf "# rows=%d server rows=%d recvq=%d sendq=%d | client rows=%d recvq=%d sendq=%d\n",
          rows+0, srows+0, srq+0, ssq+0, crows+0, crq+0, csq+0
      }'
  } >>"$_dest"
  _summary=$(tail -1 "$_dest")
  _rows=$(printf '%s' "$_summary" | sed -n 's/^# rows=\([0-9][0-9]*\) .*/\1/p')
  if [ "${_rows:-0}" -eq 0 ]; then
    echo "    t=${_waited}s SAMPLE-EMPTY no ESTABLISHED socket on port $_sport; the sample proves nothing"
  else
    echo "    t=${_waited}s ${_summary#\# }"
  fi
}

bump() {
  _f="$outdir/$1"
  _v=$(cat "$_f" 2>/dev/null || echo 0)
  echo $((_v + 1)) >"$_f"
}

read_counter() {
  cat "$outdir/$1" 2>/dev/null || echo 0
}

run_round() {
  # $1 arm label, $2 port, $3 server pid, $4 round number
  _l=$1
  _p=$2
  _spid=$3
  _r=$4
  _out="$outdir/$_l.round$_r.h2load"
  _url=$(url_for "$_p")

  _start=$(date +%s)
  h2load $h2_args "$_url" >"$_out" 2>&1 &
  _hpid=$!
  _next=$((_start + sample_after))
  while kill -0 "$_hpid" 2>/dev/null; do
    _now=$(date +%s)
    if [ "$_now" -ge "$_next" ]; then
      sample_sockets "$_spid" "$_p" "$((_now - _start))" "$_l" "$_r"
      _next=$((_now + sample_every))
    fi
    sleep 0.2
  done
  wait "$_hpid" || true
  _wall=$(($(date +%s) - _start))

  # h2load's own timing is the authority. It prints ms, s or m.
  _fin=$(awk '/^finished in/{print; exit}' "$_out")
  _ms=$(printf '%s' "$_fin" | awk '{
    v = $3; sub(",", "", v)
    if (v ~ /ms$/)     { sub("ms$", "", v); printf "%d", v + 0 }
    else if (v ~ /m$/) { sub("m$",  "", v); printf "%d", (v + 0) * 60000 }
    else if (v ~ /s$/) { sub("s$",  "", v); printf "%d", (v + 0) * 1000 }
    else               { printf "-1" }
  }')
  if [ -z "$_ms" ] || [ "$_ms" -lt 0 ]; then
    _ms=$((_wall * 1000))
    _fin="finished in ${_wall}s (wall clock; h2load printed no summary)"
  fi

  _reqs=$(awk '/^requests:/{print; exit}' "$_out")
  _tot=$(printf '%s\n' "$_reqs" | sed -n 's/.* \([0-9][0-9]*\) total.*/\1/p')
  _start_n=$(printf '%s\n' "$_reqs" | sed -n 's/.* \([0-9][0-9]*\) started.*/\1/p')
  _done=$(printf '%s\n' "$_reqs" | sed -n 's/.* \([0-9][0-9]*\) done.*/\1/p')
  _succ=$(printf '%s\n' "$_reqs" | sed -n 's/.* \([0-9][0-9]*\) succeeded.*/\1/p')
  _fail=$(printf '%s\n' "$_reqs" | sed -n 's/.* \([0-9][0-9]*\) failed.*/\1/p')
  _err=$(printf '%s\n' "$_reqs" | sed -n 's/.* \([0-9][0-9]*\) errored.*/\1/p')
  _tout=$(printf '%s\n' "$_reqs" | sed -n 's/.* \([0-9][0-9]*\) timeout.*/\1/p')
  _tot=${_tot:-0}
  _start_n=${_start_n:-0}
  _done=${_done:-0}
  _succ=${_succ:-0}
  _fail=${_fail:-0}
  _err=${_err:-0}
  _tout=${_tout:-0}
  if [ "$_tot" -eq 0 ]; then
    echo "FAIL $name: h2load printed no requests line for arm $_l round $_r; see $_out" >&2
    exit 1
  fi

  # CLASSIFICATION, derived from the captured logs rather than from prose.
  #
  #   stall        total == started == done, failed == errored > 0, timeout 0.
  #                Every request reached the server. A few never completed on
  #                time. The accounting is the signature; the duration is NOT.
  #                Observed at 30.14s, 64.74s and 137.47s with an identical
  #                line, so a 30-second threshold would have missed two of
  #                three (/tmp/starh2-stall-tls-mig-100.round{2,85,86}).
  #   not-started  started < total. The client never submitted the rest. This
  #                is the -c 10 family and the large -n family. It is a
  #                DIFFERENT defect and must never be counted as a stall.
  #   other        any remaining shortfall.
  #
  # slow is orthogonal and applies to every class, so a round that both parks
  # and loses unstarted requests reports both facts instead of one.
  _slow=0
  if [ "$_ms" -ge 5000 ]; then
    _slow=1
    bump "$_l.slow"
  fi
  _mark=ok
  if [ "$_start_n" -lt "$_tot" ]; then
    _mark=NOT-STARTED
    bump "$_l.notstarted"
    if [ "$_slow" -eq 1 ]; then
      bump "$_l.mixed"
    fi
  elif [ "$_fail" -gt 0 ] && [ "$_fail" -eq "$_err" ] && [ "$_tout" -eq 0 ] && [ "$_done" -eq "$_tot" ]; then
    _mark=STALL
    bump "$_l.stalls"
  elif [ "$_succ" -ne "$_tot" ] || [ "$_fail" -ne 0 ] || [ "$_err" -ne 0 ] || [ "$_tout" -ne 0 ]; then
    _mark=OTHER-FAIL
    bump "$_l.other"
  fi
  if [ "$_slow" -eq 1 ]; then
    _mark="$_mark+SLOW"
  fi
  bump "$_l.rounds"

  echo "  round $_r [$_l] ${_ms}ms $_mark started=$_start_n/$_tot done=$_done succeeded=$_succ failed=$_fail errored=$_err timeout=$_tout"
}

# ------------------------------------------------------------------- drive

echo "CASE $name mode=$mode migrate=$migrate executors=${executors:-auto} rounds=$rounds/arm h2load $h2_args"
echo "ARM $label_a sha=$sha_a port=$port_a pid=$pid_a"
if [ "$compare" -eq 1 ]; then
  echo "ARM $label_b sha=$sha_b port=$port_b pid=$pid_b"
fi
echo "OUT $outdir"

r=1
while [ "$r" -le "$rounds" ]; do
  run_round "$label_a" "$port_a" "$pid_a" "$r"
  if [ "$compare" -eq 1 ]; then
    run_round "$label_b" "$port_b" "$pid_b" "$r"
  fi
  r=$((r + 1))
done

# ------------------------------------------------------------------ report

a_rounds=$(read_counter "$label_a.rounds")
a_stalls=$(read_counter "$label_a.stalls")
a_notstarted=$(read_counter "$label_a.notstarted")
a_other=$(read_counter "$label_a.other")
a_slow=$(read_counter "$label_a.slow")
a_mixed=$(read_counter "$label_a.mixed")

if [ "$a_rounds" -ne "$rounds" ]; then
  echo "FAIL $name: arm $label_a ran $a_rounds rounds, expected $rounds" >&2
  exit 1
fi

if [ "$compare" -eq 0 ]; then
  echo "RESULT $name arm=$label_a sha=$sha_a rounds=$a_rounds stalls=$a_stalls notstarted=$a_notstarted other=$a_other slow=$a_slow mixed=$a_mixed migrate=$migrate"
  if [ "$a_stalls" -gt 0 ] || [ "$a_other" -gt 0 ] || [ "$a_notstarted" -gt 0 ]; then
    exit 1
  fi
  exit 0
fi

b_rounds=$(read_counter "$label_b.rounds")
b_stalls=$(read_counter "$label_b.stalls")
b_notstarted=$(read_counter "$label_b.notstarted")
b_other=$(read_counter "$label_b.other")
b_slow=$(read_counter "$label_b.slow")
b_mixed=$(read_counter "$label_b.mixed")

echo "RESULT $name arm=$label_a sha=$sha_a rounds=$a_rounds stalls=$a_stalls notstarted=$a_notstarted other=$a_other slow=$a_slow mixed=$a_mixed"
echo "RESULT $name arm=$label_b sha=$sha_b rounds=$b_rounds stalls=$b_stalls notstarted=$b_notstarted other=$b_other slow=$b_slow mixed=$b_mixed"

# The gate is the stall class only. A difference in the other classes is a
# separate defect, so it is loud but it does not decide this run.
if [ "$b_notstarted" -gt "$a_notstarted" ] || [ "$b_other" -gt "$a_other" ]; then
  echo "WARN $name: arm $label_b lost more rounds outside the stall class than $label_a."
  echo "  notstarted $a_notstarted -> $b_notstarted, other $a_other -> $b_other. Investigate separately."
fi

if [ "$a_stalls" -eq 0 ]; then
  echo "INCONCLUSIVE $name: the control arm $label_a never stalled in $a_rounds rounds."
  echo "  The instrument did not fire, so arm $label_b proves nothing. Raise --rounds or the load."
  exit 2
fi
if [ "$b_stalls" -gt 0 ]; then
  echo "FAIL $name: arm $label_b stalled $b_stalls/$b_rounds while control $label_a stalled $a_stalls/$a_rounds"
  exit 1
fi
echo "PASS $name: control $label_a stalled $a_stalls/$a_rounds, arm $label_b stalled 0/$b_rounds"
exit 0
