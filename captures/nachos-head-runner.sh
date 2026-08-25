#!/bin/sh
# Full four-arm benchmark of starh2 at 978213b on nachos, INCLUDING the
# concurrent mixed phase that nachos-final-3bb5147 never ran.
#
# Arms: starh2, go-net/http, Kestrel, hyper. The harness under
# tools/sse_bench/ builds every arm and refuses to report when one does not
# answer, so this runner only supplies the conditions.
#
# Phases:
#   mixed-w2 / mixed-auto  SSE and oneshot CONCURRENT on one TLS connection,
#                          plus the stall probe. This is the phase that a
#                          STREAMS=0 run does not exercise.
#   sse200                 200 SSE streams, latency and RSS.
#   conns50-*              the official h2load shape (-c 50 -m 10), oneshot
#                          only, at three executor widths.
#   buildcfg-control       conns50-auto rebuilt with -Dtarget=x86_64-linux-gnu
#                          (baseline CPU features), to measure the build
#                          config against the native build used above.
#
# ROUNDS=4 everywhere, because mixed.sh shifts the arm order cyclically over
# four positions. Three rounds leave the order unbalanced.
set -u
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
REPO=$HOME/src/starh2-head
OUT=$HOME/src/starh2-head-out
SHA_WANT=978213b
HOST_WANT=nachos

mkdir -p "$OUT/logs"
exec >"$OUT/run.log" 2>&1

fail=0
note() { echo; echo "======== $* ========"; date -Is; uptime; }

cd "$REPO" || exit 2
SHA=$(git rev-parse --short HEAD)
echo "host=$(uname -n) sha=$SHA want=$SHA_WANT"
echo "dirty=$(git status --short | grep -vc '^??') tracked files modified"
echo "zig=$($HOME/.zvm/bin/zig version)"
echo "go=$(go version)"
echo "dotnet=$(dotnet --version)"
echo "cargo=$(cargo --version)"
echo "boringssl=$(readlink -f vendor/boringssl)"
uname -a
nproc
if [ "$SHA" != "$SHA_WANT" ] || [ "$(uname -n)" != "$HOST_WANT" ]; then
  echo "host/sha mismatch" >&2
  exit 2
fi
# ./zb must be the plain passthrough for the native phases. The control
# phase swaps it deliberately and restores it.
grep -q 'exec "$HOME/.zvm/bin/zig" "$@"' zb || { echo "zb is not the plain wrapper" >&2; exit 2; }
test -f testdata/cert.pem || { echo "testdata/cert.pem missing" >&2; exit 2; }

pkill -f "starh2-bench-serve[r]" 2>/dev/null || true
pkill -f "sse-serve[r]" 2>/dev/null || true
pkill -f "sse-kestre[l]" 2>/dev/null || true
pkill -f "sse-hype[r]" 2>/dev/null || true
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
  echo "---- $name ----"
  cat "$log"
  if [ "$rc" -ne 0 ]; then
    fail=1
  fi
  return 0
}

# --- the mixed phase: SSE and oneshot on the same connection ---
run_phase mixed-w2 \
  env STREAMS=32 INTERVAL=10 SECONDS_RUN=5 WARMUP=1 ONESHOT_WORKERS=8 \
      STARH2_EXECUTORS=2 ROUNDS=4 STALL=1 OUT="$OUT/mixed-w2" \
      "$REPO/tools/sse_bench/mixed.sh"

run_phase mixed-auto \
  env STREAMS=32 INTERVAL=10 SECONDS_RUN=5 WARMUP=1 ONESHOT_WORKERS=8 \
      STARH2_EXECUTORS=auto ROUNDS=4 STALL=1 OUT="$OUT/mixed-auto" \
      "$REPO/tools/sse_bench/mixed.sh"

# --- SSE-only latency and RSS ---
run_phase sse200 \
  env STREAMS=200 SECONDS_RUN=10 INTERVAL=10 ROUNDS=4 WARMUP=1 \
      STARH2_EXECUTORS=2 OUT="$OUT/sse" \
      "$REPO/tools/sse_bench/run.sh"

# --- oneshot-only, the h2load shape, three widths ---
for w in auto 2 24; do
  run_phase "conns50-exec$w" \
    env CONNS=50 ONESHOT_WORKERS=500 STREAMS=0 STALL=0 \
        SECONDS_RUN=5 WARMUP=1 ROUNDS=4 \
        STARH2_EXECUTORS="$w" OUT="$OUT/conns50-exec$w" \
        "$REPO/tools/sse_bench/mixed.sh"
done

# --- build-config control: baseline CPU features against native ---
note "buildcfg-control: swapping zb for the -Dtarget wrapper"
cp zb "$OUT/zb.native.bak"
cat > zb <<'ZBEOF'
#!/bin/sh
# Control only: force -Dtarget=x86_64-linux-gnu (baseline CPU features,
# bundled CRT). The native build is the measurement; this is the contrast.
ZIG="${HOME}/.zvm/bin/zig"
if [ "$1" = "build" ]; then
  shift
  exec "$ZIG" build -Dtarget=x86_64-linux-gnu "$@"
fi
exec "$ZIG" "$@"
ZBEOF
chmod +x zb
run_phase conns50-auto-baselinecpu \
  env CONNS=50 ONESHOT_WORKERS=500 STREAMS=0 STALL=0 \
      SECONDS_RUN=5 WARMUP=1 ROUNDS=4 \
      STARH2_EXECUTORS=auto OUT="$OUT/conns50-auto-baselinecpu" \
      "$REPO/tools/sse_bench/mixed.sh"
cp "$OUT/zb.native.bak" zb
chmod +x zb
echo "zb restored; dirty=$(git status --short | grep -vc '^??')"

# --- coverage: a run that measured nothing must fail loudly ---
note "coverage"
rows=$(grep -hcE 'oneshot ok=|sse latency' "$OUT"/logs/*.log 2>/dev/null | awk '{s+=$1} END {print s+0}')
echo "measured rows=$rows"
mixed_rows=$(grep -hcE 'streams=32 conns=' "$OUT/logs/mixed-w2.log" "$OUT/logs/mixed-auto.log" 2>/dev/null | awk '{s+=$1} END {print s+0}')
echo "concurrent mixed rows=$mixed_rows"
if [ "$rows" -eq 0 ] || [ "$mixed_rows" -eq 0 ]; then
  echo "ZERO ROWS: the run did not measure" >&2
  fail=1
fi

note "done"
echo "NACHOS-HEAD-DONE fail=$fail sha=$SHA rows=$rows mixed_rows=$mixed_rows" | tee -a "$OUT/summary.txt"
exit 0
