#!/usr/bin/env bash
# Does RSS level off, and does it come back down? (t-3469)
#
# The question is RSS growth and instability, not the smallest number. A
# server whose RSS keeps rising across identical load cycles, or that lands
# somewhere different on each run of the same workload, has memory whose
# lifetime nobody chose. This tool runs the same cycle several times and
# reports what RSS does across the cycles.
#
# Shape, chosen to match what starh2 is built for:
#   - for the whole run: RSS_SSE SSE streams, one per TLS connection, each
#     sending an event every RSS_SSE_INTERVAL_MS (mostly idle connections that
#     live for minutes, as a Datastar page holds them)
#   - per cycle:
#       load   a fixed count of mixed requests (RSS_COMPLETE_N, RSS_TASK_N,
#              RSS_POST_N): complete and task
#              fragments from 1 KiB to 1 MiB rendered into the request arena,
#              plus 64 KiB POST bodies. Complete and task routes run on
#              separate connections: mixed on one connection, complete
#              handlers fail beside 1 MiB task responses (t-3509). Put them
#              back together when that is fixed.
#       idle   RSS_IDLE_S seconds of the SSE streams only
#       churn  RSS_CHURN_S seconds of waves of fresh TLS connections, one
#              request each
# RSS is sampled every second (samples.tsv) and read at the end of each phase
# (rows.tsv).
#
# Report, cycle 1 counted as warm-up:
#   idle_end   RSS at the end of each idle phase: what the server keeps once
#              the load has stopped. A positive slope across cycles is growth.
#   load_end   RSS at the end of each load phase.
#   peak       the highest sample of the run.
# One run gives one curve. Run-to-run spread needs several runs of the same
# arm (n>=3), which is the caller's loop, not this script's.
#
# Arms: RSS_GPA=init|smp picks the server's allocator (bench server --gpa;
# `init` is glibc malloc on Linux). RSS_SERVER_ENV is prepended to the server
# command, e.g. RSS_SERVER_ENV="GLIBC_TUNABLES=glibc.malloc.trim_threshold=268435456".
#
# Exit codes: 0 ran and every phase did work; 2 setup failed; 3 a phase did
# no work or reported failures (the numbers are then not a result). A clean
# zero from a run that never loaded the server is the failure this guards.
#
# Needs: h2load, go, openssl (only when testdata/*.pem is absent). Linux reads
# /proc/<pid>/status; Darwin falls back to ps, whose RSS means something
# different, so compare arms on one OS only.
set -euo pipefail

REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
. "$REPO/tools/bench_lock.sh"
cd "$REPO"

OUT=${RSS_OUT:-/tmp/starh2-rss-stability}
BIN=${RSS_BIN:-$REPO/zig-out/bin/starh2-bench-server}
GPA=${RSS_GPA:-init}
CYCLES=${RSS_CYCLES:-6}
# Request counts per load phase, per h2load process.
COMPLETE_N=${RSS_COMPLETE_N:-400000}
TASK_N=${RSS_TASK_N:-40000}
POST_N=${RSS_POST_N:-40000}
IDLE_S=${RSS_IDLE_S:-10}
CHURN_S=${RSS_CHURN_S:-10}
SSE=${RSS_SSE:-512}
SSE_INTERVAL_MS=${RSS_SSE_INTERVAL_MS:-1000}
LOAD_C=${RSS_LOAD_C:-128}
LOAD_M=${RSS_LOAD_M:-16}
POST_C=${RSS_POST_C:-32}
POST_M=${RSS_POST_M:-8}
CHURN_C=${RSS_CHURN_C:-128}
T=${RSS_THREADS:-4}
SERVER_ENV=${RSS_SERVER_ENV:-}
# taskset lists (Linux). Empty runs unpinned.
SERVER_CPUS=${RSS_SERVER_CPUS:-}
CLIENT_CPUS=${RSS_CLIENT_CPUS:-}
EXECUTORS=${RSS_EXECUTORS:-}

for tool in h2load go; do
  command -v "$tool" >/dev/null || { echo "rss-stability: $tool not found" >&2; exit 2; }
done
[ -x "$BIN" ] || { echo "rss-stability: no server at $BIN (./zb build starh2-bench-server -Doptimize=ReleaseFast)" >&2; exit 2; }
case "$GPA" in init|smp) ;; *) echo "rss-stability: RSS_GPA must be init or smp, got $GPA" >&2; exit 2 ;; esac

rm -rf "$OUT"
mkdir -p "$OUT"
CERT=testdata/cert.pem
KEY=testdata/key.pem
if [ ! -f "$CERT" ] || [ ! -f "$KEY" ]; then
  CERT=$OUT/cert.pem
  KEY=$OUT/key.pem
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -keyout "$KEY" -out "$CERT" \
    -days 2 -nodes -subj '/CN=localhost' >/dev/null 2>&1
fi
(cd tools/sse_bench && go build -o "$OUT/sse-client" ./client.go) || exit 2
head -c 65536 /dev/zero > "$OUT/body64k"

# taskset prefixes. A command started with `prefix... cmd &` is a direct
# command, so `$!` is its own pid (taskset and env exec). Do not wrap a
# background command in a shell function: the function runs in a subshell,
# `$!` is then the subshell's pid, and a signal or RSS read reaches the shell
# instead of the process.
SERVER_PIN=()
CLIENT_PIN=()
if command -v taskset >/dev/null; then
  [ -n "$SERVER_CPUS" ] && SERVER_PIN=(taskset -c "$SERVER_CPUS")
  [ -n "$CLIENT_CPUS" ] && CLIENT_PIN=(taskset -c "$CLIENT_CPUS")
fi

rss_kb() {
  if [ -r "/proc/$SRV/status" ]; then
    awk '/^VmRSS:/ {print $2}' "/proc/$SRV/status"
  else
    ps -o rss= -p "$SRV" | tr -d ' '
  fi
}

SRV=
SAMPLER=
SSE_PID=
cleanup() {
  for p in $SAMPLER $SSE_PID $SRV; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  bench_unlock
}
trap cleanup EXIT

bench_lock

srv_cmd=(${SERVER_PIN[@]+"${SERVER_PIN[@]}"})
# shellcheck disable=SC2206
srv_cmd+=(env $SERVER_ENV "$BIN" --port 0 --mode tls --cert "$CERT" --key "$KEY" --gpa "$GPA"
  --sse-interval-ms "$SSE_INTERVAL_MS" --max-connections $((SSE + LOAD_C + POST_C + CHURN_C + 64)))
[ -n "$EXECUTORS" ] && srv_cmd+=(--executors "$EXECUTORS")
"${srv_cmd[@]}" > "$OUT/server.out" 2> "$OUT/server.err" &
SRV=$!
for _ in $(seq 100); do grep -q '"ready":true' "$OUT/server.out" 2>/dev/null && break; sleep 0.1; done
grep -q '"ready":true' "$OUT/server.out" || { echo "rss-stability: server never printed its ready line" >&2; exit 2; }
# The ready line names the allocator the server really runs with.
grep -q "\"gpa\":\"$GPA\"" "$OUT/server.out" || { echo "rss-stability: server is not on gpa=$GPA: $(cat "$OUT/server.out")" >&2; exit 2; }
PORT=$(grep -o '"port":[0-9]*' "$OUT/server.out" | cut -d: -f2)
BASE=https://127.0.0.1:$PORT

PHASE_FILE=$OUT/phase
echo "0 start" > "$PHASE_FILE"
(
  while kill -0 "$SRV" 2>/dev/null; do
    printf '%s\t%s\t%s\n' "$(date +%s)" "$(cat "$PHASE_FILE")" "$(rss_kb)"
    sleep 1
  done
) > "$OUT/samples.tsv" &
SAMPLER=$!

RSS_BOOT=$(rss_kb)
# The SSE client needs a fixed window, and a load phase's length is not
# known up front. RSS_SSE_WINDOW_S must cover the whole run; a client that
# ends early fails the run below (its streams end before the last cycle).
TOTAL_S=${RSS_SSE_WINDOW_S:-1800}
${CLIENT_PIN[@]+"${CLIENT_PIN[@]}"} "$OUT/sse-client" -url "$BASE/sse" -streams "$SSE" -conns "$SSE" \
  -seconds "$TOTAL_S" -warmup 1 -interval-ms "$SSE_INTERVAL_MS" -interrupt-ends-window -label sse > "$OUT/sse.out" 2>&1 &
SSE_PID=$!
sleep 5
RSS_SSE=$(rss_kb)

bad=0
check_h2load() { # check_h2load <log> <phase label>
  local log=$1 label=$2 line ok fail
  line=$(grep '^requests:' "$log" || true)
  if [ -z "$line" ]; then echo "INVALID $label: h2load printed no requests line" >&2; bad=1; return; fi
  ok=$(echo "$line" | awk '{print $8}')
  # "F failed, E errored, T timeout": errored and timeout are part of failed.
  fail=$(echo "$line" | awk '{print $10}')
  echo "$label ok=$ok failed=$fail" >> "$OUT/phases.log"
  if [ "$ok" -eq 0 ] || [ "$fail" -ne 0 ]; then echo "INVALID $label: ok=$ok failed=$fail" >&2; bad=1; fi
}

COMPLETE_URIS=("$BASE/fragment?kb=1" "$BASE/fragment?kb=4" "$BASE/fragment?kb=16" "$BASE/fragment?kb=60")
TASK_URIS=("$BASE/fragment-task?kb=4" "$BASE/fragment-task?kb=64" "$BASE/fragment-task?kb=256" "$BASE/fragment-task?kb=1024")

printf 'cycle\tload_end_kb\tidle_end_kb\tchurn_end_kb\n' > "$OUT/rows.tsv"
for c in $(seq "$CYCLES"); do
  echo "$c load" > "$PHASE_FILE"
  # -n, not -D: at a -D deadline h2load counts the requests still in
  # flight as errored, which large responses make common and which says
  # nothing about the server. Each phase sends a fixed count instead; the
  # phase is however long the counts really take.
  ${CLIENT_PIN[@]+"${CLIENT_PIN[@]}"} h2load -c $((LOAD_C / 2)) -m "$LOAD_M" -t "$T" -n "$COMPLETE_N" "${COMPLETE_URIS[@]}" > "$OUT/load-complete-$c.log" 2>&1 &
  g1=$!
  ${CLIENT_PIN[@]+"${CLIENT_PIN[@]}"} h2load -c $((LOAD_C / 2)) -m "$LOAD_M" -t "$T" -n "$TASK_N" "${TASK_URIS[@]}" > "$OUT/load-task-$c.log" 2>&1 &
  g2=$!
  ${CLIENT_PIN[@]+"${CLIENT_PIN[@]}"} h2load -c "$POST_C" -m "$POST_M" -t 1 -n "$POST_N" -d "$OUT/body64k" "$BASE/echo-len" > "$OUT/post-$c.log" 2>&1 &
  p=$!
  wait "$g1" "$g2" "$p" || true
  load_end=$(rss_kb)
  check_h2load "$OUT/load-complete-$c.log" "cycle $c load-complete"
  check_h2load "$OUT/load-task-$c.log" "cycle $c load-task"
  check_h2load "$OUT/post-$c.log" "cycle $c load-post"

  echo "$c idle" > "$PHASE_FILE"
  sleep "$IDLE_S"
  idle_end=$(rss_kb)

  echo "$c churn" > "$PHASE_FILE"
  end=$(( $(date +%s) + CHURN_S ))
  w=0
  while [ "$(date +%s)" -lt "$end" ]; do
    w=$((w + 1))
    ${CLIENT_PIN[@]+"${CLIENT_PIN[@]}"} h2load -c "$CHURN_C" -n "$CHURN_C" -t "$T" "$BASE/fragment?kb=16" > "$OUT/churn-$c-$w.log" 2>&1 || true
    check_h2load "$OUT/churn-$c-$w.log" "cycle $c churn wave $w"
  done
  churn_end=$(rss_kb)
  printf '%s\t%s\t%s\t%s\n' "$c" "$load_end" "$idle_end" "$churn_end" >> "$OUT/rows.tsv"
  echo "cycle $c: load_end=${load_end} KB idle_end=${idle_end} KB churn_end=${churn_end} KB (churn waves=$w)"
done

echo "end idle" > "$PHASE_FILE"
if ! kill -0 "$SSE_PID" 2>/dev/null; then
  echo "INVALID sse: the client ended before the last cycle did (raise RSS_SSE_WINDOW_S)" >&2
  bad=1
fi
# Stop the client now rather than wait out its window: with
# -interrupt-ends-window, SIGINT closes the window and it prints its report.
kill -INT "$SSE_PID" 2>/dev/null || true
wait "$SSE_PID" || true
SSE_PID=
sse_line=$(grep ' streams=' "$OUT/sse.out" | head -1 || true)
echo "sse: $sse_line" >> "$OUT/phases.log"
delivering=$(echo "$sse_line" | grep -o 'delivering=[0-9]*' | cut -d= -f2)
ended=$(echo "$sse_line" | grep -o 'ended_early=[0-9]*' | cut -d= -f2)
if [ "${delivering:-0}" -ne "$SSE" ] || [ "${ended:-1}" -ne 0 ]; then
  echo "INVALID sse: wanted $SSE delivering streams and none ended early: $sse_line" >&2
  bad=1
fi

peak=$(awk -F'\t' 'BEGIN {m = 0} $3 > m {m = $3} END {print m}' "$OUT/samples.tsv")
# A sampler pointed at the wrong process reads a flat line. Load that moved
# megabytes through request arenas must raise RSS above the SSE-only level.
if [ "$peak" -le "$RSS_SSE" ]; then
  echo "INVALID: peak RSS $peak KB is not above the SSE-only $RSS_SSE KB; the sampler is not reading the server" >&2
  bad=1
fi
nsamples=$(wc -l < "$OUT/samples.tsv" | tr -d ' ')
[ "$nsamples" -gt 0 ] || { echo "INVALID: no RSS samples" >&2; bad=1; }

# Least-squares slope of idle_end over cycles 2..N, in KB per cycle.
slope=$(awk -F'\t' 'NR > 2 {n++; x = $1; y = $3; sx += x; sy += y; sxx += x * x; sxy += x * y}
  END {d = n * sxx - sx * sx; if (n < 2 || d == 0) print "n/a"; else printf "%.0f", (n * sxy - sx * sy) / d}' "$OUT/rows.tsv")
idle_first=$(awk -F'\t' 'NR == 3 {print $3}' "$OUT/rows.tsv")
idle_last=$(awk -F'\t' 'END {print $3}' "$OUT/rows.tsv")

{
  echo "rss-stability gpa=$GPA server_env=[${SERVER_ENV}] cycles=$CYCLES load_n=$COMPLETE_N/$TASK_N/$POST_N idle=${IDLE_S}s churn=${CHURN_S}s sse=$SSE"
  echo "host=$(uname -n) bin_sha256=$( (sha256sum "$BIN" 2>/dev/null || shasum -a 256 "$BIN") | awk '{print $1}')"
  echo "boot_kb=$RSS_BOOT with_sse_kb=$RSS_SSE peak_kb=$peak samples=$nsamples"
  echo "idle_end cycle2=${idle_first:-n/a} last=${idle_last:-n/a} slope_kb_per_cycle=$slope"
} | tee "$OUT/summary.txt"

if [ "$bad" -ne 0 ]; then
  echo "rss-stability: a phase did no work or failed; see $OUT/phases.log" >&2
  exit 3
fi
