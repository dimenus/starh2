#!/bin/sh
# N-way A/B of zio PINS under one starh2 tree, run on nachos (linux, io_uring).
#
# # Why this exists
#
# starh2 carries a zio fork. Upstream is replacing those patches with its own
# (PR #709 supersedes our #702), and #709's own microbenchmarks report a 7-23%
# regression on select paths. starh2's actor parks in ONE `zio.select` per turn,
# so that cost lands on every SSE event. A microbenchmark cannot say what it
# costs here; this does.
#
# # What varies, and what is held constant
#
# ONE thing varies: the `.zio` pin in build.zig.zon. Each arm is a worktree of
# the SAME starh2 commit, built by the same compiler for the same target, so a
# difference between arms is a difference in zio.
#
# A pin move that also needs starh2 source changes (an API rename, a removed
# option) cannot use one starh2 commit. Then each arm is its own starh2 tree:
# name it with TREE_<arm>=<path>, and the identity check below also requires
# the arm's embedded zio package to be the one that tree's build.zig.zon pins.
# Whoever runs it must show that the starh2 source difference between the
# trees is mechanical, or the result is not a zio result.
#
# Every arm also carries the fork-only `runtime: poll timeouts on a dedicated
# executor` patch. Nothing upstream replaces it, so an arm without it would
# measure a missing timer fix instead of the select protocol.
#
# # Why nachos and not the laptop
#
# nachos runs the io_uring backend; the laptop runs kqueue. The CompletionQueue
# path #709 rewrites is backend-specific. nachos cannot build (toolchain drift,
# t-880), so arms are cross-built here as static x86_64-linux-musl binaries and
# shipped, as tools/cq-nachos-ab.sh does.
#
# # The two phases, and why the second needs its own denominator
#
# PERF rows are throughput and latency. BURST rows are a rate: opening 200 SSE
# streams at once on one TLS connection fails closed sometimes, and on nachos
# the current pin failed 1 round in 10 once and 0 in 20 twice. A rate that low
# cannot be compared from three rounds, so the burst phase runs BURST_ROUNDS
# per arm and reports fails/rounds per arm. Two arms are only different when
# their rates are, against that denominator.
#
# # The instrument checks, before any number is read
#
# 1. Each binary must embed EXACTLY ONE `zio-<version>-<hash>` package path,
#    and it must be that arm's (when TREE_<arm> is set, the one its
#    build.zig.zon pins). A stale cache silently linking another arm's zio
#    would produce four numbers for one build.
# 2. All arm hashes must differ; two arms resolving to one package measure the
#    same thing twice and read as "no difference". The one exception is an A/A
#    run, which measures the noise floor on purpose: SAME_ZIO_OK=1 allows a
#    shared package only when every arm binary is byte-identical (sha256).
# 3. Zero rows is a FAILURE, not a pass.
# 4. The h2load thread count was swept (t=2,4,8,12) at both widths and the rate
#    is flat, so the one-shot rows are server-bound rather than client-bound.
#    That sweep was of the h2load nachos had then; a different h2load build
#    must be swept again before the claim carries over.
# 5. The load client must exist on the host. A missing h2load exits 127, which
#    the one-shot row would otherwise record as a server WEDGE-OR-FAIL.
# 6. Each shipped binary's sha256 is printed here and re-checked on the host,
#    so the capture names exactly which binary produced each arm's rows.
# 7. Before each phase the host prints its load, its top processes, and the
#    cores busy over 2 s. Above IDLE_MAX_CORES the run stops: another job
#    sharing the machine is not noise this design can cancel.
#
# # One-shot latency
#
# h2load prints no percentiles. ONESHOT_LAT=1 adds a SECOND one-shot run per
# width with `--log-file` and reports p50/p99 from its per-request rows. It is
# a separate run so the rps rows keep the client they were swept with; writing
# one log line per request is extra client work.
#
# # Order
#
# The arm that runs second in a round wins on every ordering (see the header of
# tools/sse_bench/run.sh). The arm order ROTATES every round, so each arm takes
# every slot and the ordering cancels.
#
#   ARMS="current main 702 709" BIN_ROOT=/tmp/scratch PERF_ROUNDS=5 \
#     BURST_ROUNDS=50 tools/zio-arm-ab.sh > rows.txt
#
# BIN_ROOT holds out-<arm>/bin/starh2-bench-server for every arm in ARMS.
# Arm names must be shell identifiers, because TREE_<arm> is looked up by name.
# PHASES picks the phases to run (default "1 2 3").
set -eu

REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
ARMS=${ARMS:?ARMS is required, e.g. ARMS="current main 702 709"}
BIN_ROOT=${BIN_ROOT:?BIN_ROOT is required}
PERF_ROUNDS=${PERF_ROUNDS:-5}
BURST_ROUNDS=${BURST_ROUNDS:-50}
CPU_ROUNDS=${CPU_ROUNDS:-6}
HOST=${HOST:-nachos}
SECONDS_RUN=${SECONDS_RUN:-10}
INTERVAL=${INTERVAL:-1}
EXECUTORS=${EXECUTORS:-2}
WIDE_EXECUTORS=${WIDE_EXECUTORS:-8}
# Sized from the sweeps: 820k req/s at 2 executors and 2.3M at 8, so these give
# runs of roughly 2.4 s and 1.7 s. A 100k run finished in 28 ms and measured
# start-up, not steady state.
ONESHOT_N=${ONESHOT_N:-2000000}
ONESHOT_WIDE_N=${ONESHOT_WIDE_N:-4000000}
# 200 streams delivers 100% at p50 ~11us; 500 sits at the knee (p50 ~750us,
# still 100% delivered) and is where a per-event cost shows.
SSE_LOW=${SSE_LOW:-200}
SSE_HIGH=${SSE_HIGH:-500}
REMOTE_DIR=${REMOTE_DIR:-/tmp/zioab}
PHASES=${PHASES:-1 2 3}
H2LOAD=${H2LOAD:-h2load}
ONESHOT_LAT=${ONESHOT_LAT:-0}
SAME_ZIO_OK=${SAME_ZIO_OK:-0}
IDLE_MAX_CORES=${IDLE_MAX_CORES:-1.5}
CLIENT_BIN=${CLIENT_BIN:-/tmp/zioab-client}
# Row selectors, so a bisect runs only the rows it classifies on. The defaults
# run every row. An empty PERF_SSE_STREAMS skips the phase-1 SSE rows.
PERF_SSE_STREAMS=${PERF_SSE_STREAMS-$SSE_LOW $SSE_HIGH}
ONESHOT_PLAIN=${ONESHOT_PLAIN:-1}
ONESHOT_WIDTHS=${ONESHOT_WIDTHS:-$EXECUTORS $WIDE_EXECUTORS}
CPU_STREAMS=${CPU_STREAMS:-$SSE_LOW $SSE_HIGH}
ONESHOT_LAT_WIDTHS=${ONESHOT_LAT_WIDTHS:-$ONESHOT_WIDTHS}
# Burst is a 60-round phase; BURST_ARMS runs it on a subset of ARMS.
BURST_ARMS=${BURST_ARMS:-$ARMS}
# THREAD_CPU=1 appends each server thread's utime+stime (ticks, largest first)
# to every one-shot latency row, read just before the kill.
THREAD_CPU=${THREAD_CPU:-0}
# LOG_GREP copies matching lines of the server's own log into the rows after
# each phase-3 run, for counters a diagnostic build prints as it runs: the
# last matching line per second field (a loop or thread key), or
# LOG-GREP-NO-MATCH, so a counter that never printed cannot pass as absent.
LOG_GREP=${LOG_GREP:-}
# A busy host is waited out, up to HOST_WAIT_MAX seconds, re-checking every
# 30 s; every wait is printed. 0 stops at once, as before. ROUND_CHECK=1 also
# checks before every round of phases 1 and 3 and every 10th burst round,
# because a phase lasts long enough for another job to start inside it.
HOST_WAIT_MAX=${HOST_WAIT_MAX:-0}
ROUND_CHECK=${ROUND_CHECK:-0}
# The check window, and the most any ONE process may use in it. A game ran on
# nachos at 1-2 cores and one 2 s total sample still read 0.46, under a 0.5
# limit; the per-process limit is what caught it.
CHECK_SECONDS=${CHECK_SECONDS:-5}
PROC_MAX_CORES=${PROC_MAX_CORES:-0.3}
# A process whose name matches HOST_BLOCK_RE makes the host busy while it
# exists at all, whatever it used in the window: that game idled at 0.23 cores
# for one window and used 2 in the next.
HOST_BLOCK_RE=${HOST_BLOCK_RE:-}
# Phase 4: one-shot requests over ONE TLS connection (the mixed recipe's
# oneshot-only shape: tools/sse_bench/client.go -streams 0 -conns 1). With one
# connection, where its tasks land decides the whole run, so this is the
# shape a placement difference (27ff454's bimodal pinned bands) shows in;
# h2load's 50 connections average it away.
ONECONN_ROUNDS=${ONECONN_ROUNDS:-10}
ONECONN_ARMS=${ONECONN_ARMS:-$ARMS}
ONECONN_WIDTHS=${ONECONN_WIDTHS:-$EXECUTORS}
ONECONN_WORKERS=${ONECONN_WORKERS:-8}
ONECONN_SECONDS=${ONECONN_SECONDS:-5}

# Per-arm server arguments and ready-line expectations: ARGS_<arm> is appended
# to that arm's server command line, and EXPECT_<arm>, when set, must appear in
# the server's ready line on EVERY start, or the run stops. That is how two arms
# that differ only in build options or run options prove which one ran: the
# ready line prints the scheduling and spawn placement the binary really uses.
# No EXIT trap to delete it: under bash-as-sh a trap turns a set -u failure
# into exit status 0, and a failed run must not read as a pass.
ARMENV=$(mktemp)
for arm in $ARMS; do
  eval "a=\${ARGS_$arm:-}; e=\${EXPECT_$arm:-}"
  case "$a$e" in *"'"*) echo "ARGS_$arm / EXPECT_$arm may not contain a single quote" >&2; exit 1;; esac
  printf "ARGS_%s='%s'\nEXPECT_%s='%s'\n" "$arm" "$a" "$arm" "$e" >> "$ARMENV"
done

SOCK=$(ls /private/tmp/com.apple.launchd.*/Listeners 2>/dev/null | head -1) || true
export SSH_AUTH_SOCK=${SSH_AUTH_SOCK:-$SOCK}

echo "== arm identity (embedded zio package hash, binary sha256) =="
hashes=""
ids=""
shas=""
for arm in $ARMS; do
  bin="$BIN_ROOT/out-$arm/bin/starh2-bench-server"
  [ -f "$bin" ] || { echo "missing binary for arm $arm: $bin" >&2; exit 1; }
  h=$(strings -a "$bin" | grep -oE 'zio-[0-9]+\.[0-9]+\.[0-9]+-[A-Za-z0-9_-]{20,}' | sort -u)
  n=$(printf '%s\n' "$h" | grep -c .)
  [ "$n" = 1 ] || { echo "arm $arm embeds $n zio package paths, expected 1" >&2; exit 1; }
  eval "tree=\${TREE_$arm:-}"
  if [ -n "$tree" ]; then
    pinned=$(grep -A2 '\.zio = ' "$tree/build.zig.zon" | sed -n 's/.*\.hash = "\(zio-[^"]*\)".*/\1/p')
    [ -n "$pinned" ] || { echo "arm $arm: no .zio hash in $tree/build.zig.zon" >&2; exit 1; }
    [ "$pinned" = "$h" ] || { echo "arm $arm embeds $h but $tree pins $pinned" >&2; exit 1; }
  fi
  sha=$(shasum -a 256 "$bin" | cut -d' ' -f1)
  eval "a=\${ARGS_$arm:-}; e=\${EXPECT_$arm:-}"
  echo "  $arm  $h  sha256=$sha${tree:+  tree=$tree}  args='$a'${e:+  expect-ready='$e'}"
  hashes="$hashes$h
"
  shas="$shas$arm=$sha "
  ids="$ids$arm	$h	$sha	$a	$e
"
done
# Two arms that share a zio package are one of three things, and only these:
# - the same binary with different ARGS_: one build, two run configurations;
# - the same binary with the same args: an A/A run, only with SAME_ZIO_OK=1;
# - different binaries (one package, different build options): allowed only
#   when both declare an EXPECT_ ready-line check and the two differ, so the
#   run itself proves each binary is the configuration it is named for.
# Anything else is the stale-cache failure the package check exists for.
dupes=$(printf '%s' "$hashes" | sort | uniq -d)
for d in $dupes; do
  printf '%s' "$ids" | awk -F'\t' -v d="$d" '$2 == d' > "$ARMENV.grp"
  bad=$(awk -F'\t' -v same="$SAME_ZIO_OK" '
    { arm[NR] = $1; sha[NR] = $3; args[NR] = $4; xp[NR] = $5 }
    END {
      for (i = 1; i <= NR; i++) for (j = i + 1; j <= NR; j++) {
        if (sha[i] == sha[j] && args[i] != args[j]) continue
        if (sha[i] == sha[j] && args[i] == args[j]) { if (same == 1) continue
          print arm[i] " and " arm[j] " are the same binary with the same args (A/A needs SAME_ZIO_OK=1)"; continue }
        if (xp[i] != "" && xp[j] != "" && xp[i] != xp[j]) continue
        print arm[i] " and " arm[j] " share one zio package in different binaries without distinct EXPECT_ ready checks"
      }
    }' "$ARMENV.grp")
  rm -f "$ARMENV.grp"
  [ -z "$bad" ] || { echo "$bad" >&2; exit 1; }
  echo "  shared zio package $d: allowed (same binary with different args, A/A, or ready-line-checked build options)"
done

for f in testdata/cert.pem testdata/key.pem; do
  [ -f "$REPO/$f" ] || { echo "$f is missing; generate it (see tools/README.md)" >&2; exit 1; }
done
(cd "$REPO/tools/sse_bench" && GOOS=linux GOARCH=amd64 go build -o "$CLIENT_BIN" ./client.go)
ssh "$HOST" "mkdir -p $REMOTE_DIR"
for arm in $ARMS; do
  scp -q "$BIN_ROOT/out-$arm/bin/starh2-bench-server" "$HOST:$REMOTE_DIR/$arm-server"
done
scp -q "$CLIENT_BIN" "$HOST:$REMOTE_DIR/client"
scp -q "$REPO/testdata/cert.pem" "$REPO/testdata/key.pem" "$HOST:$REMOTE_DIR/"
scp -q "$ARMENV" "$HOST:$REMOTE_DIR/armenv.sh"
rm -f "$ARMENV"

ssh "$HOST" "ARMS='$ARMS' BURST_ARMS='$BURST_ARMS' ONESHOT_LAT_WIDTHS='$ONESHOT_LAT_WIDTHS' \
  THREAD_CPU=$THREAD_CPU LOG_GREP='$LOG_GREP' HOST_WAIT_MAX=$HOST_WAIT_MAX ROUND_CHECK=$ROUND_CHECK \
  CHECK_SECONDS=$CHECK_SECONDS PROC_MAX_CORES=$PROC_MAX_CORES HOST_BLOCK_RE='$HOST_BLOCK_RE' \
  ONECONN_ROUNDS=$ONECONN_ROUNDS ONECONN_ARMS='$ONECONN_ARMS' ONECONN_WIDTHS='$ONECONN_WIDTHS' \
  ONECONN_WORKERS=$ONECONN_WORKERS ONECONN_SECONDS=$ONECONN_SECONDS \
  PERF_ROUNDS=$PERF_ROUNDS BURST_ROUNDS=$BURST_ROUNDS CPU_ROUNDS=$CPU_ROUNDS \
  SECONDS_RUN=$SECONDS_RUN INTERVAL=$INTERVAL EXECUTORS=$EXECUTORS \
  WIDE_EXECUTORS=$WIDE_EXECUTORS ONESHOT_N=$ONESHOT_N ONESHOT_WIDE_N=$ONESHOT_WIDE_N \
  SSE_LOW=$SSE_LOW SSE_HIGH=$SSE_HIGH D=$REMOTE_DIR PHASES='$PHASES' H2LOAD='$H2LOAD' \
  ONESHOT_LAT=$ONESHOT_LAT IDLE_MAX_CORES=$IDLE_MAX_CORES SHAS='$shas' \
  PERF_SSE_STREAMS='$PERF_SSE_STREAMS' ONESHOT_PLAIN=$ONESHOT_PLAIN ONESHOT_WIDTHS='$ONESHOT_WIDTHS' \
  CPU_STREAMS='$CPU_STREAMS' sh -s" <<'REMOTE'
set -u
chmod +x $D/*-server $D/client
. $D/armenv.sh
echo "== host =="
uname -r; echo "cores=$(nproc)"; uptime
command -v "$H2LOAD" > /dev/null || { echo "load client '$H2LOAD' is missing on $(hostname); a missing client reads as a server wedge" >&2; exit 1; }
echo "h2load: $(command -v "$H2LOAD") $("$H2LOAD" --version 2>&1 | head -1)"
for pair in $SHAS; do
  arm=${pair%%=*}; want=${pair#*=}
  got=$(sha256sum $D/$arm-server | cut -d' ' -f1)
  [ "$got" = "$want" ] || { echo "arm $arm: shipped binary sha256 $got, expected $want" >&2; exit 1; }
  echo "  ran $D/$arm-server sha256=$got"
done

# The cores busy over 2 s, from /proc/stat. iowait counts as idle: it is a
# task waiting on a disk, not a task using a core.
# pid|comm|utime+stime for every process. comm can hold spaces, so the fields
# are counted after the ") <state> " that ends it.
proc_ticks() {
  for f in /proc/[0-9]*/stat; do cat "$f" 2>/dev/null; echo; done | awk '
    NF == 0 { next }
    { i = match($0, /\) [A-Za-z] /); if (!i) next
      head = substr($0, 1, i - 1); rest = substr($0, i + 2)
      pid = substr(head, 1, index(head, " ") - 1); comm = substr(head, index(head, "(") + 1)
      split(rest, r, " "); print pid "|" comm "|" r[12] + r[13] }'
}
host_check() {
  waited=0
  while :; do
    if [ "${2:-full}" = full ] || [ $waited -gt 0 ]; then
      echo "-- host check before $1 --"
      uptime
      top -bn1 -o %CPU -w 160 < /dev/null | sed -n '7,13p' | cut -c1-160
    fi
    s1=$(awk '/^cpu /{print $2+$3+$4+$7+$8+$9, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat)
    p1=$(proc_ticks)
    sleep $CHECK_SECONDS
    s2=$(awk '/^cpu /{print $2+$3+$4+$7+$8+$9, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat)
    p2=$(proc_ticks)
    busy=$(echo "$s1 $s2" | awk -v n=$(nproc) '{printf "%.2f", ($3-$1)/($4-$2)*n}')
    # The busiest single process over the same window. A game or a compile
    # can dip under the total limit for one sample while it is still running,
    # so any one process above PROC_MAX_CORES also counts as busy.
    top1=$(printf '%s\n--\n%s\n' "$p1" "$p2" | awk -v s=$CHECK_SECONDS -v tck=$(getconf CLK_TCK) '
      $0 == "--" { second = 1; next }
      { split($0, f, "|"); if (!second) a[f[1]] = f[3]; else if (f[1] in a) { d = (f[3] - a[f[1]]) / tck / s; if (d > best) { best = d; who = f[1] " " f[2] } } }
      END { printf "%.2f %s", best, who }')
    pbusy=${top1%% *}
    blockers=""
    [ -z "$HOST_BLOCK_RE" ] || blockers=$(printf '%s\n' "$p2" | awk -F'|' -v re="$HOST_BLOCK_RE" '$2 ~ re { printf "%s%s(%s)", sep, $2, $1; sep = " " }')
    echo "busy_cores=$busy limit=$IDLE_MAX_CORES top_process_cores=$pbusy (${top1#* }) limit=$PROC_MAX_CORES${blockers:+ blocking=$blockers} before $1"
    # Idle means: no blocking process present, and neither limit exceeded.
    if [ -z "$blockers" ] && ! awk -v b=$busy -v m=$IDLE_MAX_CORES -v p=$pbusy -v pm=$PROC_MAX_CORES 'BEGIN{exit !(b>m || p>pm)}'; then
      return 0
    fi
    if [ $waited -ge $HOST_WAIT_MAX ]; then
      echo "HOST-BUSY before $1: $busy cores busy, limit $IDLE_MAX_CORES, waited ${waited}s; stopping, not measuring" >&2
      exit 3
    fi
    echo "HOST-BUSY-WAIT before $1: $busy cores busy; waiting 30 s (waited ${waited}s so far)"
    sleep 30
    waited=$((waited+30))
  done
}
round_check() { [ "$ROUND_CHECK" = 1 ] && host_check "$1" brief; return 0; }
has_phase() { case " $PHASES " in *" $1 "*) return 0;; esac; return 1; }

start_srv() {
  ARM=$1; EXEC=$2
  eval "extra=\${ARGS_$ARM:-}; expect=\${EXPECT_$ARM:-}"
  rm -f $D/$ARM.log
  $D/$ARM-server --mode tls --port 0 --executors $EXEC --sse-interval-ms $INTERVAL \
    --cert $D/cert.pem --key $D/key.pem $extra > $D/$ARM.log 2>&1 &
  SRV_PID=$!
  i=0; SRV_PORT=
  while [ $i -lt 200 ]; do
    SRV_PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' $D/$ARM.log 2>/dev/null | head -1)
    [ -n "$SRV_PORT" ] && break
    i=$((i+1)); sleep 0.05
  done
  [ -n "$SRV_PORT" ] || echo "NO-READY-LINE arm=$ARM"
  if [ -n "$SRV_PORT" ]; then
    ready=$(grep '"ready"' $D/$ARM.log | head -1)
    eval "shown=\${SHOWN_$ARM:-}"
    if [ -z "$shown" ]; then
      echo "  ready $ARM args='$extra' $ready"
      eval "SHOWN_$ARM=1"
    fi
    if [ -n "$expect" ]; then
      case "$ready" in
        *"$expect"*) ;;
        *) echo "READY-MISMATCH arm=$ARM expected '$expect' in: $ready" >&2
           stop_srv; exit 4 ;;
      esac
    fi
  fi
}
stop_srv() { kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null; }

# rotate an arm list (default ARMS) by (round-1) so each arm takes every slot
rotate() {
  R=$1; LIST=${2:-$ARMS}; set -- $LIST; n=$#; k=$(( (R - 1) % n )); i=0; ORDER=""
  while [ $i -lt $n ]; do
    idx=$(( (k + i) % n + 1 )); j=1
    for a in $LIST; do [ $j -eq $idx ] && ORDER="$ORDER $a"; j=$((j+1)); done
    i=$((i+1))
  done
}

# One h2load one-shot run. The `requests:` line is kept whole, so a row shows
# how many requests succeeded, not only a rate.
oneshot() {
  ARM=$1; EXEC=$2; N=$3
  start_srv $ARM $EXEC
  if [ -n "$SRV_PORT" ]; then
    out=$(timeout 180 "$H2LOAD" -n $N -c 50 -m 10 -t 4 https://127.0.0.1:$SRV_PORT/ 2>&1)
    rc=$?
    if [ $rc -ne 0 ]; then echo "r$r $ARM oneshot-e$EXEC WEDGE-OR-FAIL rc=$rc"
    else echo "$out" | grep -E 'finished in|requests:' | tr '\n' ' ' | sed "s/^/r$r $ARM oneshot-e$EXEC /"; echo; fi
    rows=$((rows+1))
  fi
  stop_srv
}

# The same run with a per-request log, for latency. Column 2 is the status
# (-1 for a failed stream), column 3 the microseconds to end of response.
oneshot_lat() {
  ARM=$1; EXEC=$2; N=$3
  start_srv $ARM $EXEC
  if [ -n "$SRV_PORT" ]; then
    rm -f $D/lat.tsv
    out=$(timeout 180 "$H2LOAD" -n $N -c 50 -m 10 -t 4 --log-file=$D/lat.tsv https://127.0.0.1:$SRV_PORT/ 2>&1)
    rc=$?
    if [ $rc -ne 0 ]; then echo "r$r $ARM oneshot-lat-e$EXEC WEDGE-OR-FAIL rc=$rc"
    else
      non200=$(awk -F'\t' '$2 != 200' $D/lat.tsv | wc -l)
      pct=$(awk -F'\t' '$2 == 200 {print $3}' $D/lat.tsv | sort -n | awk '
        { a[NR] = $1 }
        END {
          if (NR == 0) { print "n=0"; exit }
          i50 = int(NR * 0.50); if (i50 < 1) i50 = 1
          i99 = int(NR * 0.99); if (i99 < 1) i99 = 1
          printf "n=%d p50us=%d p99us=%d maxus=%d", NR, a[i50], a[i99], a[NR]
        }')
      thr=""
      if [ "$THREAD_CPU" = 1 ]; then
        # comm may hold spaces, so fields are counted after the ")" that ends it:
        # utime and stime are then fields 12 and 13.
        thr=" threads=$(for t in /proc/$SRV_PID/task/*; do sed 's/^.*) //' $t/stat 2>/dev/null | awk '{print $12+$13}'; done | sort -rn | tr '\n' ',' | sed 's/,$//')"
      fi
      echo "r$r $ARM oneshot-lat-e$EXEC $pct non200=$non200$thr $(echo "$out" | grep -E 'finished in|requests:' | tr '\n' ' ')"
    fi
    rm -f $D/lat.tsv
    rows=$((rows+1))
  fi
  stop_srv
}

rows=0
if has_phase 1; then
host_check "phase 1"
echo "== phase 1: perf =="
r=1
while [ $r -le $PERF_ROUNDS ]; do
  round_check "perf round $r"
  rotate $r
  for arm in $ORDER; do
    for S in $PERF_SSE_STREAMS; do
      start_srv $arm $EXECUTORS
      if [ -n "$SRV_PORT" ]; then
        out=$(timeout 180 $D/client -url https://127.0.0.1:$SRV_PORT/sse -streams $S \
          -seconds $SECONDS_RUN -warmup 1 -label $arm 2>&1)
        echo "$out" | grep -E 'streams=|sse latency|NO EVENTS' | sed "s/^/r$r $arm sse$S /"
        rows=$((rows+1))
      fi
      stop_srv
    done

    for W in $ONESHOT_WIDTHS; do
      if [ "$W" = "$EXECUTORS" ]; then N=$ONESHOT_N; else N=$ONESHOT_WIDE_N; fi
      [ "$ONESHOT_PLAIN" = 1 ] && oneshot $arm $W $N
    done
    for W in $ONESHOT_LAT_WIDTHS; do
      if [ "$W" = "$EXECUTORS" ]; then N=$ONESHOT_N; else N=$ONESHOT_WIDE_N; fi
      [ "$ONESHOT_LAT" = 1 ] && oneshot_lat $arm $W $N
    done
  done
  r=$((r+1))
done
fi

if has_phase 2; then
host_check "phase 2"
echo "== phase 2: burst fail-close rate ($BURST_ROUNDS rounds per arm, arms: $BURST_ARMS) =="
b=1
while [ $b -le $BURST_ROUNDS ]; do
  [ $((b % 10)) = 1 ] && [ $b -gt 1 ] && round_check "burst round $b"
  rotate $b "$BURST_ARMS"
  for arm in $ORDER; do
    start_srv $arm $EXECUTORS
    if [ -n "$SRV_PORT" ]; then
      line=$(timeout 60 $D/client -url https://127.0.0.1:$SRV_PORT/sse -streams $SSE_LOW \
        -seconds 2 -warmup 1 -label burst 2>&1 | grep 'streams=')
      tr=$(curl -sk --http2 https://127.0.0.1:$SRV_PORT/trace 2>/dev/null)
      ov=$(echo "$tr" | sed -n 's/.*"tls_write_overflow":\([0-9]*\).*/\1/p')
      st=$(echo "$tr" | sed -n 's/.*"tls_stage_failed":\([0-9]*\).*/\1/p')
      case "$line" in
        *"opened=$SSE_LOW "*"delivering=$SSE_LOW "*"failed=0 "*)
          echo "b$b $arm burst OK overflow=${ov:-absent} stage_failed=${st:-absent}" ;;
        *)
          echo "b$b $arm burst FAIL $line overflow=${ov:-absent} stage_failed=${st:-absent}"
          echo "b$b $arm burst-log $(grep -v '"ready"' $D/$arm.log | head -3)" ;;
      esac
      rows=$((rows+1))
    fi
    stop_srv
  done
  b=$((b+1))
done
fi

if has_phase 3; then
host_check "phase 3"
echo "== phase 3: server CPU per event at a FIXED offered load =="
# Every arm delivers 100% of a 200-stream 1ms offering, so a latency compare
# puts two servers side by side that are both keeping up. CPU per event does
# not: the client fixes the work, so the only thing that varies is what the
# server spends to do it. utime+stime come from /proc/<pid>/stat fields 14 and
# 15, read BEFORE the kill, in clock ticks.
TCK=$(getconf CLK_TCK)
c=1
while [ $c -le $CPU_ROUNDS ]; do
  round_check "cpu round $c"
  rotate $c
  for arm in $ORDER; do
    for S in $CPU_STREAMS; do
      start_srv $arm $EXECUTORS
      if [ -n "$SRV_PORT" ]; then
        line=$(timeout 180 $D/client -url https://127.0.0.1:$SRV_PORT/sse -streams $S \
          -seconds $SECONDS_RUN -warmup 1 -label $arm 2>&1 | grep 'events=')
        cpu=$(awk '{print $14+$15}' /proc/$SRV_PID/stat 2>/dev/null)
        echo "c$c $arm cpu$S ticks=${cpu:-absent} tck=$TCK $line"
        rows=$((rows+1))
      fi
      stop_srv
      if [ -n "$LOG_GREP" ] && [ -n "$SRV_PORT" ]; then
        # A diagnostic build may print a running total; keep the last line
        # per key (the second field), which is the total at the kill.
        grep -E "$LOG_GREP" $D/$arm.log | awk '{ last[$2] = $0; if (!($2 in seen)) { seen[$2] = 1; key[++n] = $2 } }
          END { for (i = 1; i <= n; i++) print last[key[i]]; if (n == 0) print "LOG-GREP-NO-MATCH" }' \
          | sed "s/^/c$c $arm cpu$S log /"
      fi
    done
  done
  c=$((c+1))
done
fi

if has_phase 4; then
host_check "phase 4"
echo "== phase 4: one-shot over one TLS connection ($ONECONN_WORKERS workers, arms: $ONECONN_ARMS) =="
q=1
while [ $q -le $ONECONN_ROUNDS ]; do
  round_check "oneconn round $q"
  rotate $q "$ONECONN_ARMS"
  for arm in $ORDER; do
    for W in $ONECONN_WIDTHS; do
      start_srv $arm $W
      if [ -n "$SRV_PORT" ]; then
        line=$(timeout 60 $D/client -streams 0 -conns 1 -oneshot-url https://127.0.0.1:$SRV_PORT/ \
          -oneshot-workers $ONECONN_WORKERS -seconds $ONECONN_SECONDS -warmup 1 -label $arm 2>&1 \
          | grep -E 'oneshot ok=|NO ONESHOTS' | tr '\n' ' ')
        echo "q$q $arm oneconn-e$W ${line:-NO-CLIENT-LINE}"
        rows=$((rows+1))
      fi
      stop_srv
    done
  done
  q=$((q+1))
done
fi

echo "== rows=$rows =="
[ $rows -gt 0 ] || { echo "zio-arm-ab: produced no rows; that is a failure, not a pass" >&2; exit 1; }
REMOTE
