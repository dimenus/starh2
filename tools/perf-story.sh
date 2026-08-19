#!/bin/sh
# Characterization matrix for t-845. Adds a runner; does not change harnesses.
#
# Logs live under /tmp/perf-story/ (override PERF_STORY_OUT). Every log starts
# with uname -n, then the uptime load line, binary sha256, and the exact
# command. A scan that produces zero rows exits non-zero instead of printing
# an empty table.
#
# Darwin: 1-min load > 2.0 marks the row BLOCKED-LOADED and skips the run.
# Linux: pgrep must show no leftover bench/sse servers before a row; pkill
# uses a bracket pattern so ssh's own argv is not matched.
set -eu

REPO=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd -P)
OUT=${PERF_STORY_OUT:-/tmp/perf-story}
NEW_ROOT=${PERF_STORY_NEW:-$REPO}
BASE_ROOT=${PERF_STORY_BASE:-}
HENDRIK_ROOT=${HENDRIK_ROOT:-}
N=${PERF_STORY_N:-100000}
PIPELINE_N=${PERF_STORY_PIPELINE_N:-1000000}
SELF_DRIVE_N=${PERF_STORY_SELF_DRIVE_N:-100000}
H2LOAD_C=${PERF_STORY_C:-50}
H2LOAD_M=${PERF_STORY_M:-10}
H2LOAD_T=${PERF_STORY_T:-4}
ROUNDS=${PERF_STORY_ROUNDS:-3}

OS=$(uname -s)
HOST_UNAME=$(uname -n)

if [ -z "$HENDRIK_ROOT" ]; then
  if [ -f "$NEW_ROOT/../../oss/http2-zig-hendrik/build.zig" ]; then
    HENDRIK_ROOT=$(CDPATH= cd -- "$NEW_ROOT/../../oss/http2-zig-hendrik" && pwd -P)
  elif [ -f "$HOME/src/oss/http2-zig-hendrik/build.zig" ]; then
    HENDRIK_ROOT=$(CDPATH= cd -- "$HOME/src/oss/http2-zig-hendrik" && pwd -P)
  fi
fi

mkdir -p "$OUT/logs" "$OUT/bin"
ROWS="$OUT/rows.tsv"
: > "$ROWS"

sha256_file() {
  f=$1
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$f" | awk '{print $1}'
  else
    shasum -a 256 "$f" | awk '{print $1}'
  fi
}

load1() {
  uptime | awk '{
    for (i = 1; i <= NF; i++) {
      if ($i ~ /average/) { v = $(i + 1); gsub(/,/, "", v); print v; exit }
    }
  }'
}

darwin_loaded() {
  [ "$OS" = Darwin ] || return 1
  awk -v l="$(load1)" 'BEGIN { exit (l + 0 > 2.0) ? 0 : 1 }'
}

write_log_header() {
  log=$1
  sha=$2
  cmd=$3
  {
    uname -n
    uptime
    echo "binary_sha256: $sha"
    echo "cmd: $cmd"
  } > "$log"
}

emit_row() {
  name=$1
  arm=$2
  status=$3
  summary=$4
  log=$5
  sha=$6
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$arm" "$status" "$summary" "$log" "$sha" >> "$ROWS"
}

cleanup_servers() {
  pkill -f "starh2-bench-serve[r]" 2>/dev/null || true
  pkill -f "sse-serve[r]" 2>/dev/null || true
  sleep 1
}

servers_clear() {
  leftover=$(pgrep -fl "starh2-bench-serve[r]|sse-serve[r]" 2>/dev/null || true)
  if [ -n "$leftover" ]; then
    echo "leftover servers:" >&2
    echo "$leftover" >&2
    return 1
  fi
  return 0
}

before_row() {
  cleanup_servers
  servers_clear
}

arm_sha() {
  git -C "$1" rev-parse --short HEAD
}

build_bench() {
  root=$1
  prefix=$2
  (CDPATH= cd -- "$root" && ./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$prefix")
}

build_pipeline() {
  root=$1
  prefix=$2
  (CDPATH= cd -- "$root" && ./zb build -Doptimize=ReleaseFast --prefix "$prefix")
}

skip_or_run() {
  name=$1
  arm=$2
  sha=$3
  cmd=$4
  log=$5
  if darwin_loaded; then
    write_log_header "$log" "$sha" "$cmd"
    echo "BLOCKED-LOADED 1-min=$(load1)" >> "$log"
    emit_row "$name" "$arm" "BLOCKED-LOADED" "1-min=$(load1)" "$log" "$sha"
    return 1
  fi
  if [ "$OS" != Darwin ]; then
    if ! before_row; then
      write_log_header "$log" "$sha" "$cmd"
      echo "BLOCKED-BUSY leftover servers" >> "$log"
      emit_row "$name" "$arm" "BLOCKED-BUSY" "leftover servers" "$log" "$sha"
      return 1
    fi
  fi
  write_log_header "$log" "$sha" "$cmd"
  return 0
}

run_logged() {
  log=$1
  shift
  set +e
  "$@" >> "$log" 2>&1
  st=$?
  set -e
  echo "exit: $st" >> "$log"
  return $st
}

NEW_SHA=$(arm_sha "$NEW_ROOT")
BASE_SHA=
if [ -n "$BASE_ROOT" ] && [ -d "$BASE_ROOT" ]; then
  BASE_SHA=$(arm_sha "$BASE_ROOT")
fi

echo "perf-story: host=$HOST_UNAME os=$OS out=$OUT"
echo "perf-story: new=$NEW_ROOT ($NEW_SHA)"
if [ -n "$BASE_SHA" ]; then
  echo "perf-story: base=$BASE_ROOT ($BASE_SHA)"
else
  echo "perf-story: base arm absent (PERF_STORY_BASE unset or missing)"
fi

echo "== build =="
build_bench "$NEW_ROOT" "$OUT/bin/new"
NEW_BIN="$OUT/bin/new/bin/starh2-bench-server"
NEW_SUM=$(sha256_file "$NEW_BIN")
build_pipeline "$NEW_ROOT" "$OUT/bin/new"
NEW_PIPE="$OUT/bin/new/bin/pipeline-bench"

BASE_BIN=
BASE_SUM=absent
BASE_PIPE=
if [ -n "$BASE_SHA" ]; then
  build_bench "$BASE_ROOT" "$OUT/bin/base"
  BASE_BIN="$OUT/bin/base/bin/starh2-bench-server"
  BASE_SUM=$(sha256_file "$BASE_BIN")
fi

if [ -d "$NEW_ROOT/tools/sse_bench" ]; then
  (CDPATH= cd -- "$NEW_ROOT/tools/sse_bench" && [ -f go.mod ] || printf 'module ssebench\n\ngo 1.26\n' > go.mod
   go build -o "$OUT/sse-client" ./client.go)
fi

packed() {
  arm=$1
  root=$2
  bin=$3
  sum=$4
  name="packed-tls-h2c"
  log="$OUT/logs/${name}-${arm}.log"
  cmd="cd $root && ./zb build bench -Doptimize=ReleaseFast -- -n $N -c $H2LOAD_C -m $H2LOAD_M -t $H2LOAD_T --rounds $ROUNDS"
  if skip_or_run "$name" "$arm" "$sum" "$cmd" "$log"; then
    if run_logged "$log" sh -c "CDPATH= cd -- \"$root\" && ./zb build bench -Doptimize=ReleaseFast -- -n $N -c $H2LOAD_C -m $H2LOAD_M -t $H2LOAD_T --rounds $ROUNDS"; then
      emit_row "$name" "$arm" "ok" "see log" "$log" "$sum"
    else
      emit_row "$name" "$arm" "FAIL" "bench exit non-zero" "$log" "$sum"
    fi
  fi
}

packed "$NEW_SHA" "$NEW_ROOT" "$NEW_BIN" "$NEW_SUM"
if [ -n "$BASE_SHA" ]; then
  packed "$BASE_SHA" "$BASE_ROOT" "$BASE_BIN" "$BASE_SUM"
else
  log="$OUT/logs/packed-tls-h2c-base-missing.log"
  write_log_header "$log" "absent" "PERF_STORY_BASE missing"
  echo "GAP: no 55835a4 tree on this machine" >> "$log"
  emit_row "packed-tls-h2c" "55835a4" "GAP" "no base tree" "$log" "absent"
fi

if [ -n "$HENDRIK_ROOT" ] && [ -f "$HENDRIK_ROOT/build.zig" ]; then
  name="packed-hendrik"
  log="$OUT/logs/${name}.log"
  cmd="HENDRIK_ROOT=$HENDRIK_ROOT $NEW_ROOT/tools/bench-hendrik.sh -n $N -c $H2LOAD_C -m $H2LOAD_M -t $H2LOAD_T --rounds $ROUNDS"
  if skip_or_run "$name" "http2.zig" "pending" "$cmd" "$log"; then
    if run_logged "$log" env HENDRIK_ROOT="$HENDRIK_ROOT" OUT="$OUT/hendrik" "$NEW_ROOT/tools/bench-hendrik.sh" -n "$N" -c "$H2LOAD_C" -m "$H2LOAD_M" -t "$H2LOAD_T" --rounds "$ROUNDS"; then
      hbin=$OUT/hendrik/http2-zig/bin/benchmark
      hsum=absent
      if [ -x "$hbin" ]; then hsum=$(sha256_file "$hbin"); fi
      emit_row "$name" "http2.zig" "ok" "see log" "$log" "$hsum"
    else
      emit_row "$name" "http2.zig" "FAIL" "bench-hendrik exit non-zero" "$log" "absent"
    fi
  fi
else
  log="$OUT/logs/packed-hendrik-missing.log"
  write_log_header "$log" "absent" "HENDRIK_ROOT missing"
  echo "GAP: http2.zig checkout not found" >> "$log"
  emit_row "packed-hendrik" "http2.zig" "GAP" "no http2.zig checkout" "$log" "absent"
fi

mixed_row() {
  arm=$1
  root=$2
  sum=$3
  name="mixed"
  log="$OUT/logs/${name}-${arm}.log"
  cmd="cd $root && STREAMS=32 INTERVAL=10 SECONDS_RUN=5 WARMUP=1 ONESHOT_WORKERS=8 STARH2_EXECUTORS=2 ROUNDS=$ROUNDS OUT=$OUT/mixed-$arm tools/sse_bench/mixed.sh"
  if skip_or_run "$name" "$arm" "$sum" "$cmd" "$log"; then
    if run_logged "$log" env STREAMS=32 INTERVAL=10 SECONDS_RUN=5 WARMUP=1 ONESHOT_WORKERS=8 STARH2_EXECUTORS=2 ROUNDS="$ROUNDS" OUT="$OUT/mixed-$arm" "$root/tools/sse_bench/mixed.sh"; then
      emit_row "$name" "$arm" "ok" "see log" "$log" "$sum"
    else
      emit_row "$name" "$arm" "FAIL" "mixed.sh exit non-zero" "$log" "$sum"
    fi
  fi
}

if [ -f "$NEW_ROOT/tools/sse_bench/mixed.sh" ]; then
  mixed_row "$NEW_SHA" "$NEW_ROOT" "$NEW_SUM"
fi
if [ -n "$BASE_SHA" ] && [ -f "$BASE_ROOT/tools/sse_bench/mixed.sh" ]; then
  mixed_row "$BASE_SHA" "$BASE_ROOT" "$BASE_SUM"
elif [ -z "$BASE_SHA" ]; then
  log="$OUT/logs/mixed-base-missing.log"
  write_log_header "$log" "absent" "PERF_STORY_BASE missing"
  echo "GAP: no 55835a4 tree" >> "$log"
  emit_row "mixed" "55835a4" "GAP" "no base tree" "$log" "absent"
fi

sse_row() {
  arm=$1
  root=$2
  sum=$3
  name="sse-200"
  log="$OUT/logs/${name}-${arm}.log"
  cmd="cd $root && STREAMS=200 SECONDS_RUN=10 INTERVAL=10 ROUNDS=4 WARMUP=1 STARH2_EXECUTORS=2 OUT=$OUT/sse-$arm tools/sse_bench/run.sh"
  if skip_or_run "$name" "$arm" "$sum" "$cmd" "$log"; then
    if run_logged "$log" env STREAMS=200 SECONDS_RUN=10 INTERVAL=10 ROUNDS=4 WARMUP=1 STARH2_EXECUTORS=2 OUT="$OUT/sse-$arm" "$root/tools/sse_bench/run.sh"; then
      emit_row "$name" "$arm" "ok" "see log" "$log" "$sum"
    else
      emit_row "$name" "$arm" "FAIL" "run.sh exit non-zero" "$log" "$sum"
    fi
  fi
}

sse_row "$NEW_SHA" "$NEW_ROOT" "$NEW_SUM"
if [ -n "$BASE_SHA" ]; then
  sse_row "$BASE_SHA" "$BASE_ROOT" "$BASE_SUM"
else
  log="$OUT/logs/sse-200-base-missing.log"
  write_log_header "$log" "absent" "PERF_STORY_BASE missing"
  echo "GAP: no 55835a4 tree" >> "$log"
  emit_row "sse-200" "55835a4" "GAP" "no base tree" "$log" "absent"
fi

pipeline_hf() {
  arm=$1
  root=$2
  pipe=$3
  sum=$4
  name="pipeline-hyperfine"
  log="$OUT/logs/${name}-${arm}.log"
  if [ -n "$pipe" ] && [ -x "$pipe" ]; then
    psum=$(sha256_file "$pipe")
    cmd="hyperfine --warmup 1 --runs 3 '$pipe -n $PIPELINE_N --rounds 5'"
    if skip_or_run "$name" "$arm" "$psum" "$cmd" "$log"; then
      if run_logged "$log" hyperfine --warmup 1 --runs 3 "$pipe -n $PIPELINE_N --rounds 5"; then
        emit_row "$name" "$arm" "ok" "hyperfine binary" "$log" "$psum"
      else
        emit_row "$name" "$arm" "FAIL" "hyperfine exit non-zero" "$log" "$psum"
      fi
    fi
  else
    cmd="cd $root && ./zb build bench-pipeline -Doptimize=ReleaseFast -- -n $PIPELINE_N --rounds 5"
    if skip_or_run "$name" "$arm" "$sum" "$cmd" "$log"; then
      if run_logged "$log" sh -c "CDPATH= cd -- \"$root\" && ./zb build bench-pipeline -Doptimize=ReleaseFast -- -n $PIPELINE_N --rounds 5"; then
        emit_row "$name" "$arm" "ok" "zb build bench-pipeline" "$log" "$sum"
      else
        emit_row "$name" "$arm" "FAIL" "bench-pipeline exit non-zero" "$log" "$sum"
      fi
    fi
  fi
}

pipeline_hf "$NEW_SHA" "$NEW_ROOT" "$NEW_PIPE" "$NEW_SUM"
if [ -n "$BASE_SHA" ]; then
  pipeline_hf "$BASE_SHA" "$BASE_ROOT" "" "$BASE_SUM"
else
  log="$OUT/logs/pipeline-hyperfine-base-missing.log"
  write_log_header "$log" "absent" "PERF_STORY_BASE missing"
  echo "GAP: no 55835a4 tree" >> "$log"
  emit_row "pipeline-hyperfine" "55835a4" "GAP" "no base tree" "$log" "absent"
fi

if [ "$OS" = Linux ] && command -v poop >/dev/null 2>&1; then
  name="pipeline-poop"
  log="$OUT/logs/${name}.log"
  if [ -n "$BASE_SHA" ] && [ -x "$NEW_PIPE" ]; then
    cmd="poop --duration 15000 'cd $BASE_ROOT && ./zb build bench-pipeline -Doptimize=ReleaseFast -- -n $PIPELINE_N --rounds 5' '$NEW_PIPE -n $PIPELINE_N --rounds 5'"
    if skip_or_run "$name" "$NEW_SHA+$BASE_SHA" "$NEW_SUM" "$cmd" "$log"; then
      if run_logged "$log" poop --duration 15000 "CDPATH= cd -- \"$BASE_ROOT\" && ./zb build bench-pipeline -Doptimize=ReleaseFast -- -n $PIPELINE_N --rounds 5" "$NEW_PIPE -n $PIPELINE_N --rounds 5"; then
        emit_row "$name" "$NEW_SHA+$BASE_SHA" "ok" "poop both arms" "$log" "$NEW_SUM"
      else
        emit_row "$name" "$NEW_SHA+$BASE_SHA" "FAIL" "poop exit non-zero" "$log" "$NEW_SUM"
      fi
    fi
  else
    cmd="poop --duration 15000 '$NEW_PIPE -n $PIPELINE_N --rounds 5'"
    if skip_or_run "$name" "$NEW_SHA" "$NEW_SUM" "$cmd" "$log"; then
      if run_logged "$log" poop --duration 15000 "$NEW_PIPE -n $PIPELINE_N --rounds 5"; then
        emit_row "$name" "$NEW_SHA" "ok" "poop new arm only" "$log" "$NEW_SUM"
      else
        emit_row "$name" "$NEW_SHA" "FAIL" "poop exit non-zero" "$log" "$NEW_SUM"
      fi
    fi
  fi
else
  log="$OUT/logs/pipeline-poop-skip.log"
  write_log_header "$log" "absent" "poop not on this OS"
  echo "GAP: poop is Linux-only (brief)" >> "$log"
  emit_row "pipeline-poop" "$OS" "GAP" "poop not available" "$log" "absent"
fi

peak_rss() {
  arm=$1
  bin=$2
  sum=$3
  name="peak-rss-200"
  log="$OUT/logs/${name}-${arm}.log"
  port=19460
  time_log="$OUT/logs/${name}-${arm}.time"
  if [ "$OS" = Darwin ]; then
    time_cmd="/usr/bin/time -l"
  else
    time_cmd="/usr/bin/time -v"
  fi
  cmd="$time_cmd $bin --mode tls --port $port --sse-interval-ms 10 --executors 2 ; sse-client 200 streams 10s"
  if skip_or_run "$name" "$arm" "$sum" "$cmd" "$log"; then
    set +e
    $time_cmd "$bin" --mode tls --port "$port" --sse-interval-ms 10 --executors 2 > "$OUT/logs/${name}-${arm}.server" 2>"$time_log" &
    spid=$!
    set -e
    i=0
    while [ "$i" -lt 50 ]; do
      if grep -q '"ready":true' "$OUT/logs/${name}-${arm}.server" 2>/dev/null; then
        break
      fi
      i=$((i + 1))
      sleep 0.1
    done
    set +e
    "$OUT/sse-client" -url "https://127.0.0.1:${port}/sse" -streams 200 -seconds 10 -warmup 1 -label "peak-$arm" >> "$log" 2>&1
    kill -TERM "$spid" 2>/dev/null
    wait "$spid" 2>/dev/null
    set -e
    {
      echo "time output:"
      cat "$time_log"
    } >> "$log"
    peak=$(awk '
      /maximum resident set size/ { print $1; exit }
      /Maximum resident set size/ { print $6; exit }
    ' "$time_log")
    if [ -n "$peak" ]; then
      emit_row "$name" "$arm" "ok" "peak=$peak" "$log" "$sum"
    else
      emit_row "$name" "$arm" "FAIL" "no peak line" "$log" "$sum"
    fi
  fi
}

if [ -x "$OUT/sse-client" ]; then
  peak_rss "$NEW_SHA" "$NEW_BIN" "$NEW_SUM"
  if [ -n "$BASE_BIN" ]; then
    peak_rss "$BASE_SHA" "$BASE_BIN" "$BASE_SUM"
  else
    log="$OUT/logs/peak-rss-200-base-missing.log"
    write_log_header "$log" "absent" "PERF_STORY_BASE missing"
    echo "GAP: no 55835a4 tree" >> "$log"
    emit_row "peak-rss-200" "55835a4" "GAP" "no base tree" "$log" "absent"
  fi
else
  log="$OUT/logs/peak-rss-200-no-client.log"
  write_log_header "$log" "absent" "sse-client missing"
  echo "GAP: go client did not build" >> "$log"
  emit_row "peak-rss-200" "all" "GAP" "no sse-client" "$log" "absent"
fi

h2load_hf() {
  arm=$1
  bin=$2
  sum=$3
  mode=$4
  name="whole-h2load-hyperfine-${mode}"
  log="$OUT/logs/${name}-${arm}.log"
  port=19470
  if [ "$mode" = tls ]; then
    url="https://127.0.0.1:${port}/"
    start_args="--mode tls --port $port"
  else
    url="http://127.0.0.1:${port}/"
    start_args="--mode h2c --port $port"
  fi
  cmd="hyperfine --warmup 1 h2load -n $N -c $H2LOAD_C -m $H2LOAD_M -t $H2LOAD_T $url  (server $bin $start_args)"
  if skip_or_run "$name" "$arm" "$sum" "$cmd" "$log"; then
    set +e
    "$bin" $start_args > "$OUT/logs/${name}-${arm}.server" 2>&1 &
    spid=$!
    set -e
    i=0
    while [ "$i" -lt 50 ]; do
      if grep -q '"ready":true' "$OUT/logs/${name}-${arm}.server" 2>/dev/null; then
        break
      fi
      i=$((i + 1))
      sleep 0.1
    done
    set +e
    hyperfine --warmup 1 "h2load -n $N -c $H2LOAD_C -m $H2LOAD_M -t $H2LOAD_T $url" >> "$log" 2>&1
    hf=$?
    kill -TERM "$spid" 2>/dev/null
    wait "$spid" 2>/dev/null
    set -e
    echo "exit: $hf" >> "$log"
    if [ "$hf" -eq 0 ]; then
      emit_row "$name" "$arm" "ok" "h2load hyperfine" "$log" "$sum"
    else
      emit_row "$name" "$arm" "FAIL" "hyperfine exit $hf" "$log" "$sum"
    fi
  fi
}

h2load_hf "$NEW_SHA" "$NEW_BIN" "$NEW_SUM" tls
h2load_hf "$NEW_SHA" "$NEW_BIN" "$NEW_SUM" h2c
if [ -n "$BASE_BIN" ]; then
  h2load_hf "$BASE_SHA" "$BASE_BIN" "$BASE_SUM" tls
  h2load_hf "$BASE_SHA" "$BASE_BIN" "$BASE_SUM" h2c
else
  log="$OUT/logs/whole-h2load-hyperfine-base-missing.log"
  write_log_header "$log" "absent" "PERF_STORY_BASE missing"
  echo "GAP: no 55835a4 tree" >> "$log"
  emit_row "whole-h2load-hyperfine" "55835a4" "GAP" "no base tree" "$log" "absent"
fi

selfdrive_hf() {
  name="whole-selfdrive-hyperfine"
  log="$OUT/logs/${name}-${NEW_SHA}.log"
  cmd="hyperfine --warmup 1 $NEW_BIN --mode tls --port 0 --self-drive-oneshots $SELF_DRIVE_N"
  if skip_or_run "$name" "$NEW_SHA" "$NEW_SUM" "$cmd" "$log"; then
    if run_logged "$log" hyperfine --warmup 1 "$NEW_BIN --mode tls --port 0 --self-drive-oneshots $SELF_DRIVE_N"; then
      emit_row "$name" "$NEW_SHA" "ok" "self-drive $SELF_DRIVE_N" "$log" "$NEW_SUM"
    else
      emit_row "$name" "$NEW_SHA" "FAIL" "hyperfine exit non-zero" "$log" "$NEW_SUM"
    fi
  fi
}

selfdrive_hf
if [ -z "$BASE_SHA" ]; then
  :
else
  log="$OUT/logs/whole-selfdrive-hyperfine-base-excluded.log"
  write_log_header "$log" "$BASE_SUM" "55835a4 excluded: flag not backported"
  echo "excluded by brief: 55835a4 stays byte-identical" >> "$log"
  emit_row "whole-selfdrive-hyperfine" "$BASE_SHA" "EXCLUDED" "flag not backported" "$log" "$BASE_SUM"
fi

if [ "$OS" = Linux ] && command -v poop >/dev/null 2>&1; then
  name="whole-selfdrive-poop"
  log="$OUT/logs/${name}.log"
  cmd="poop --duration 10000 '$NEW_BIN --mode tls --port 0 --self-drive-oneshots $SELF_DRIVE_N'"
  if skip_or_run "$name" "$NEW_SHA" "$NEW_SUM" "$cmd" "$log"; then
    if run_logged "$log" poop --duration 10000 "$NEW_BIN --mode tls --port 0 --self-drive-oneshots $SELF_DRIVE_N"; then
      emit_row "$name" "$NEW_SHA" "ok" "self-drive poop" "$log" "$NEW_SUM"
    else
      emit_row "$name" "$NEW_SHA" "FAIL" "poop exit non-zero" "$log" "$NEW_SUM"
    fi
  fi
else
  log="$OUT/logs/whole-selfdrive-poop-skip.log"
  write_log_header "$log" "absent" "poop not on this OS"
  echo "GAP: poop is Linux-only (brief)" >> "$log"
  emit_row "whole-selfdrive-poop" "$OS" "GAP" "poop not available" "$log" "absent"
fi

if [ "$OS" = Linux ]; then
  name="observe-bimodal"
  log="$OUT/logs/${name}.log"
  obs_prefix="$OUT/bin/observe"
  cmd="./zb build starh2-bench-server -Doptimize=ReleaseFast -Dobserve=true ; oneshot-only rounds with /trace"
  if skip_or_run "$name" "$NEW_SHA" "observe-pending" "$cmd" "$log"; then
    if run_logged "$log" sh -c "CDPATH= cd -- \"$NEW_ROOT\" && ./zb build starh2-bench-server -Doptimize=ReleaseFast -Dobserve=true --prefix \"$obs_prefix\""; then
      obs_bin="$obs_prefix/bin/starh2-bench-server"
      obs_sum=$(sha256_file "$obs_bin")
      port=19480
      set +e
      "$obs_bin" --mode tls --port "$port" --executors 2 --trace > "$OUT/logs/${name}.server" 2>&1 &
      spid=$!
      set -e
      i=0
      while [ "$i" -lt 50 ]; do
        if grep -q '"ready":true' "$OUT/logs/${name}.server" 2>/dev/null; then
          break
        fi
        i=$((i + 1))
        sleep 0.1
      done
      r=1
      while [ "$r" -le 6 ]; do
        echo "round $r" >> "$log"
        curl -sk --http2 "https://127.0.0.1:${port}/trace" > "$OUT/logs/${name}.r${r}.before.json"
        set +e
        "$OUT/sse-client" -streams 0 -oneshot-url "https://127.0.0.1:${port}/" \
          -oneshot-workers 8 -seconds 5 -warmup 1 -label "observe-r$r" >> "$log" 2>&1
        set -e
        curl -sk --http2 "https://127.0.0.1:${port}/trace" > "$OUT/logs/${name}.r${r}.after.json"
        r=$((r + 1))
      done
      kill -TERM "$spid" 2>/dev/null
      wait "$spid" 2>/dev/null || true
      emit_row "$name" "$NEW_SHA" "ok" "6 oneshot-only rounds + /trace" "$log" "$obs_sum"
    else
      emit_row "$name" "$NEW_SHA" "FAIL" "observe build failed" "$log" "absent"
    fi
  fi
else
  log="$OUT/logs/observe-bimodal-skip.log"
  write_log_header "$log" "absent" "Linux-only anomaly"
  echo "GAP: bimodal oneshot-only is the Linux anomaly (brief §4.2)" >> "$log"
  emit_row "observe-bimodal" "$OS" "GAP" "Linux-only" "$log" "absent"
fi

cleanup_servers

echo
echo "== matrix $HOST_UNAME =="
if [ ! -s "$ROWS" ]; then
  echo "FAIL: zero rows written to $ROWS" >&2
  exit 1
fi
rowcount=$(awk 'END { print NR }' "$ROWS")
if [ "$rowcount" -eq 0 ]; then
  echo "FAIL: zero rows written to $ROWS" >&2
  exit 1
fi

printf '%-28s %-12s %-16s %-40s %s\n' "ROW" "ARM" "STATUS" "SUMMARY" "LOG"
awk -F'\t' '{ printf "%-28s %-12s %-16s %-40s %s\n", $1, $2, $3, $4, $5 }' "$ROWS"
echo
echo "rows=$rowcount logs=$OUT/logs rows_file=$ROWS"
echo "new_bin_sha256=$NEW_SUM"
echo "base_bin_sha256=$BASE_SUM"
