#!/bin/sh
# Measure how often one connection with many task streams fails (t-3538).
#
# The shape: h2load -c 1 -m M -n N over /fragment-task?kb=KB. With M x KB
# above the connection's outbound budget, some runs fail thousands of
# requests. This script runs a matrix of MODES x MS x KBS, RUNS times per
# cell, one fresh server per run, and prints one line per run and one
# summary line per cell.
#
# Every server runs with --close-reason, so each run line also says what
# the SERVER saw: how many connections it ended (`ended`, `by=server`
# lines only: a failed write is how a peer close shows), the first such
# site and error (`reason`), and how many handler writes it refused for
# lack of outbound budget (`budget_refusals`, each one a failed stream).
# A server that is dead before the script stops it, or that printed a
# panic, is a CRASH run (`panic` quotes the message). On h2c,
# `ack_high_water` is the most write completions a connection owed room in
# its ack channel, over the channel's capacity (`ACK_LEDGER` lines). A
# crashed server prints no ledger line; its panic message carries the count.
#
# A run counts only when it did work: h2load must print a "requests:" line
# with started > 0. A run that did no work is INVALID and the script exits
# 2, because a probe that measured nothing must not read as clean.
#
# Usage (absolute paths; POSIX sh, so macOS /bin/bash 3.2 is not involved):
#   RUNS=10 MODES="h2c tls" MS="16 50 64 100 200" KBS="16 64 128" tools/t3538-rate.sh
#   BIN=/abs/starh2-bench-server  skip the build and use this server; it
#                                 must accept --close-reason
#   OUTBOUND=8388608              --outbound-bytes-per-connection for the server
#   SERVER_ARGS='...'             extra bench-server flags (splitting intended)
#
# Exit: 0 no run failed a request, 1 some run failed, 2 harness error.
set -eu

REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
. "$REPO/tools/bench_lock.sh"

MODES=${MODES:-"h2c tls"}
MS=${MS:-100}
KBS=${KBS:-64}
RUNS=${RUNS:-10}
REQUESTS=${REQUESTS:-5000}
EXECUTORS=${EXECUTORS:-8}
WATCHDOG=${WATCHDOG:-60}
OUT=${OUT:-/tmp/starh2-t3538}

command -v h2load >/dev/null 2>&1 || { echo "t3538-rate: h2load is required" >&2; exit 2; }
mkdir -p "$OUT"

if [ -n "${BIN:-}" ]; then
  STARH2=$BIN
else
  (cd "$REPO" && ./zb build starh2-bench-server -Doptimize=fast --prefix "$OUT/starh2") || exit 2
  STARH2="$OUT/starh2/bin/starh2-bench-server"
fi
[ -x "$STARH2" ] || { echo "t3538-rate: no server at $STARH2" >&2; exit 2; }

EXTRA=
[ -n "${OUTBOUND:-}" ] && EXTRA="--outbound-bytes-per-connection $OUTBOUND"

S_PID=
H_PID=
cleanup() {
  if [ -n "$H_PID" ]; then kill "$H_PID" 2>/dev/null || true; wait "$H_PID" 2>/dev/null || true; H_PID=; fi
  if [ -n "$S_PID" ]; then kill "$S_PID" 2>/dev/null || true; wait "$S_PID" 2>/dev/null || true; S_PID=; fi
  bench_unlock
}
trap cleanup EXIT INT TERM

echo "t3538-rate: sha=$(git -C "$REPO" rev-parse --short HEAD) dirty=$(git -C "$REPO" status --porcelain --untracked-files=no | wc -l | tr -d ' ') modes=\"$MODES\" ms=\"$MS\" kbs=\"$KBS\" runs=$RUNS n=$REQUESTS executors=$EXECUTORS outbound=${OUTBOUND:-default} server=$STARH2"

any_failed=0
for MODE in $MODES; do
  case "$MODE" in
    tls) SCHEME=https ;;
    h2c) SCHEME=http ;;
    *) echo "t3538-rate: a mode must be tls or h2c, got $MODE" >&2; exit 2 ;;
  esac
  for M in $MS; do
    for KB in $KBS; do
      failed_runs=0
      ended_runs=0
      crash_runs=0
      run=1
      while [ "$run" -le "$RUNS" ]; do
        tag="$MODE-m$M-kb$KB-$run"
        SLOG="$OUT/server-$tag.log"
        HLOG="$OUT/h2load-$tag.txt"
        : >"$SLOG"
        bench_lock
        if [ "$MODE" = tls ]; then
          "$STARH2" --mode tls --port 0 --executors "$EXECUTORS" --close-reason $EXTRA \
            --cert "$REPO/testdata/cert.pem" --key "$REPO/testdata/key.pem" \
            ${SERVER_ARGS:-} >"$SLOG" 2>&1 &
        else
          "$STARH2" --mode h2c --port 0 --executors "$EXECUTORS" --close-reason $EXTRA \
            ${SERVER_ARGS:-} >"$SLOG" 2>&1 &
        fi
        S_PID=$!

        PORT=
        i=0
        while [ "$i" -lt 100 ]; do
          if grep -q '"ready":true' "$SLOG" 2>/dev/null; then
            PORT=$(sed -n 's/.*"port":\([0-9][0-9]*\).*/\1/p' "$SLOG" | head -1)
            [ -n "$PORT" ] && break
          fi
          kill -0 "$S_PID" 2>/dev/null || { echo "t3538-rate: server died at bind ($tag)" >&2; cat "$SLOG" >&2; exit 2; }
          i=$((i + 1))
          sleep 0.1
        done
        [ -n "$PORT" ] || { echo "t3538-rate: no ready line ($tag)" >&2; exit 2; }

        h2load -t 1 -c 1 -m "$M" -n "$REQUESTS" "$SCHEME://127.0.0.1:$PORT/fragment-task?kb=$KB" >"$HLOG" 2>&1 &
        H_PID=$!
        waited=0
        while kill -0 "$H_PID" 2>/dev/null; do
          [ "$waited" -ge "$WATCHDOG" ] && break
          sleep 1
          waited=$((waited + 1))
        done
        verdict=
        if kill -0 "$H_PID" 2>/dev/null; then
          verdict=WEDGE
          kill "$H_PID" 2>/dev/null || true
        fi
        wait "$H_PID" 2>/dev/null || true
        H_PID=
        # The teardown lines print as the connection ends, which h2load's
        # exit does not order; give the server a moment before stopping it.
        sleep 0.2
        # A server that died on its own crashed: a panic ends the process,
        # and it must not read as a connection the peer closed.
        alive=1
        kill -0 "$S_PID" 2>/dev/null || alive=0
        kill "$S_PID" 2>/dev/null || true
        wait "$S_PID" 2>/dev/null || true
        S_PID=
        bench_unlock

        # requests: 5000 total, 5000 started, 5000 done, 5000 succeeded, 0 failed, 0 errored, 0 timeout
        REQ=$(grep '^requests:' "$HLOG" | head -1 || true)
        started=$(echo "$REQ" | sed -n 's/.* \([0-9][0-9]*\) started.*/\1/p')
        succeeded=$(echo "$REQ" | sed -n 's/.* \([0-9][0-9]*\) succeeded.*/\1/p')
        failed=$(echo "$REQ" | sed -n 's/.* \([0-9][0-9]*\) failed.*/\1/p')
        errored=$(echo "$REQ" | sed -n 's/.* \([0-9][0-9]*\) errored.*/\1/p')
        if [ -z "$started" ] || [ "$started" -eq 0 ]; then
          echo "run $tag INVALID: h2load did no work (see $HLOG)"
          cat "$HLOG" >&2
          exit 2
        fi
        ended=$(grep -c '^\[[0-9]*\] CLOSE_REASON .* by=server ' "$SLOG" || true)
        reason=$(sed -n 's/^\[[0-9]*\] CLOSE_REASON .* by=server site=\([^ ]*\) err=\([^ ]*\) goaway=\([^ ]*\).*/\1:\2:goaway=\3/p' "$SLOG" | head -1)
        refused=$(sed -n 's/^\[[0-9]*\] STREAM_BUDGET .* stream_budget_refusals=\([0-9]*\).*/\1/p' "$SLOG" | awk '{ s += $1 } END { print s + 0 }')
        # h2c only: the largest count of completions owed room in the write
        # ack channel, over the run's connections, and the channel's size.
        ack_hw=$(sed -n 's/^\[[0-9]*\] ACK_LEDGER .* high_water=\([0-9]*\).*/\1/p' "$SLOG" | sort -n | tail -1)
        ack_cap=$(sed -n 's/^\[[0-9]*\] ACK_LEDGER .* capacity=\([0-9]*\).*/\1/p' "$SLOG" | head -1)
        panic=$(sed -n 's/^.*panic: \(.*\)$/\1/p' "$SLOG" | head -1)
        if [ "$alive" -eq 0 ] || [ -n "$panic" ]; then
          verdict=CRASH
          crash_runs=$((crash_runs + 1))
        elif [ -z "$verdict" ]; then
          if [ "$failed" -gt 0 ]; then verdict=FAIL; else verdict=PASS; fi
        fi
        [ "$verdict" = PASS ] || failed_runs=$((failed_runs + 1))
        [ "$ended" -eq 0 ] || ended_runs=$((ended_runs + 1))
        echo "run $tag $verdict started=$started succeeded=$succeeded failed=$failed errored=$errored ended=$ended reason=${reason:--} budget_refusals=$refused ack_high_water=${ack_hw:--}/${ack_cap:--} panic=\"${panic:--}\" secs=$waited"
        run=$((run + 1))
      done
      echo "cell mode=$MODE m=$M kb=$KB in_flight_kib=$((M * KB)) failing_runs=$failed_runs/$RUNS server_ended_runs=$ended_runs/$RUNS crashed_runs=$crash_runs/$RUNS"
      [ "$failed_runs" -eq 0 ] || any_failed=1
    done
  done
done
[ "$any_failed" -eq 0 ] || exit 1
