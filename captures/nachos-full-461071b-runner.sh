#!/bin/sh
# Full nachos characterization of 461071b. Lives in /tmp on nachos; not a
# repo harness. Injects -Dtarget via the worktree zb wrapper (gcc-16 .sframe).
set -u
export PATH="$HOME/.local/bin:$PATH"
REPO=/tmp/starh2-full
OUT=/tmp/nachos-full-461071b
SHA_WANT=461071b

mkdir -p "$OUT/logs"
exec >"$OUT/run.log" 2>&1

fail=0
note() { echo; echo "======== $* ========"; date -Is; uptime; }

cd "$REPO" || exit 2
SHA=$(git rev-parse --short HEAD)
echo "host=$(uname -n)"
echo "sha=$SHA want=$SHA_WANT"
echo "zig=$($HOME/.zvm/bin/zig version)"
uname -a
if [ "$SHA" != "$SHA_WANT" ]; then
  echo "SHA mismatch" >&2
  exit 2
fi
if [ "$(uname -n)" != nachos ]; then
  echo "not nachos" >&2
  exit 2
fi

pkill -f "starh2-bench-serve[r]" 2>/dev/null || true
pkill -f "sse-serve[r]" 2>/dev/null || true
sleep 1

run_phase() {
  name=$1
  shift
  note "$name"
  log="$OUT/logs/$name.log"
  set +e
  "$@" >"$log" 2>&1
  rc=$?
  echo "$name rc=$rc" | tee -a "$OUT/summary.txt"
  echo "---- tail $name ----"
  tail -n 40 "$log"
  if [ "$rc" -ne 0 ]; then
    fail=1
  fi
  return 0
}

note "build ReleaseFast bench server (warmup compile)"
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/bin" \
  >"$OUT/logs/build-bench.log" 2>&1
echo "build-bench rc=$?" | tee -a "$OUT/summary.txt"
sha256sum "$OUT/bin/bin/starh2-bench-server" | tee -a "$OUT/summary.txt"

run_phase test ./zb build test
run_phase tls-smoke ./zb build tls-smoke

i=1
while [ "$i" -le 3 ]; do
  run_phase "wedge-w2-r$i" env WORKERS=2 OUT="$OUT/wedge-w2-r$i" "$REPO/tools/wedge-probe.sh"
  i=$((i + 1))
done
i=1
while [ "$i" -le 3 ]; do
  run_phase "wedge-w8-r$i" env WORKERS=8 OUT="$OUT/wedge-w8-r$i" "$REPO/tools/wedge-probe.sh"
  i=$((i + 1))
done

run_phase h2load-wedge-rate env ROUNDS=30 OUT="$OUT/h2load-wedge" \
  "$REPO/tools/h2load-wedge-rate.sh"

# Oneshot w2 vs Go (mixed.sh oneshot-only is w8). Compact 3-round alternate.
run_phase oneshot-w2-vs-go sh /tmp/nachos-oneshot-w2.sh

run_phase perf-story env PERF_STORY_OUT="$OUT/perf-story" HENDRIK_ROOT="$HOME/src/oss/http2-zig-hendrik" \
  "$REPO/tools/perf-story.sh"

note "done"
echo "NACHOS-FULL-DONE fail=$fail sha=$SHA" | tee -a "$OUT/summary.txt"
if [ -f "$OUT/perf-story/rows.tsv" ]; then
  echo "---- perf-story rows ----"
  cat "$OUT/perf-story/rows.tsv"
fi
exit 0
