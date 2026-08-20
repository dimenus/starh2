#!/bin/sh
# Paired A/B of two starh2 bench servers (and optionally the Go reference)
# on nachos, driven from the Mac. Exists because nachos's toolchain drift
# (t-880) blocks building there: both arms are CROSS-BUILT here as static
# x86_64-linux-musl binaries, shipped with the Go client, and run paired
# with order alternated per round, so machine conditions cancel.
#
#   tools/cq-nachos-ab.sh <new-ref> <base-ref> [rounds]
#
# Emits per-round rows for: oneshot WORKERS=2, WORKERS=8, SSE-200, and
# (when h2load exists on nachos) the 400k/50-conn specimen gate with the
# same classification as h2load-wedge-rate.sh (60 s watchdog, full-success
# check, 10 s SLOW class). Zero rows produced is a failure, not a pass.
#
# Caveats to carry into any write-up: musl-static builds (not gnu-native),
# and every row should quote nachos's load line printed at the top.
set -eu

NEW_REF=${1:?new ref}
BASE_REF=${2:?base ref}
ROUNDS=${3:-3}
REPO=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
SOCK=$(ls /private/tmp/com.apple.launchd.*/Listeners 2>/dev/null | head -1)
export SSH_AUTH_SOCK=${SSH_AUTH_SOCK:-$SOCK}

build_arm() {
  REF=$1; OUT=$2
  WT=$(mktemp -d /tmp/starh2-ab-XXXXXX)
  git -C "$REPO" worktree add -f "$WT" "$REF" > /dev/null
  mkdir -p "$WT/vendor"
  ln -sfn "$REPO/vendor/boringssl" "$WT/vendor/boringssl" 2>/dev/null || true
  [ -e "$WT/vendor/boringssl/CMakeLists.txt" ] || \
    ln -sfn "$HOME/Source/oss/http2-zig-hendrik/boringssl" "$WT/vendor/boringssl"
  (cd "$WT" && ./zb build starh2-bench-server -Doptimize=ReleaseFast \
    -Dtarget=x86_64-linux-musl --prefix "$OUT")
  git -C "$REPO" worktree remove --force "$WT"
}

echo "building arms: new=$NEW_REF base=$BASE_REF"
build_arm "$NEW_REF" /tmp/ab-new
build_arm "$BASE_REF" /tmp/ab-base
(cd "$REPO/tools/sse_bench" && GOOS=linux GOARCH=amd64 go build -o /tmp/ab-client ./client.go)

scp -q /tmp/ab-new/bin/starh2-bench-server nachos:/tmp/ab-new-server
scp -q /tmp/ab-base/bin/starh2-bench-server nachos:/tmp/ab-base-server
scp -q /tmp/ab-client nachos:/tmp/ab-client
scp -q "$REPO/testdata/cert.pem" "$REPO/testdata/key.pem" nachos:/tmp/

ssh nachos 'chmod +x /tmp/ab-new-server /tmp/ab-base-server /tmp/ab-client
uptime
start_srv() {
  /tmp/ab-$1-server --mode tls --port 0 --executors 2 --sse-interval-ms 1 \
    --cert /tmp/cert.pem --key /tmp/key.pem > /tmp/ab-$1.log 2>&1 &
  SRV_PID=$!
  i=0; SRV_PORT=
  while [ $i -lt 100 ]; do
    SRV_PORT=$(sed -n "s/.*\"port\":\([0-9]*\).*/\1/p" /tmp/ab-$1.log | head -1)
    [ -n "$SRV_PORT" ] && break
    i=$((i+1)); sleep 0.1
  done
}
stop_srv() { kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null; }
row() {
  ARM=$1; R=$2; MODE=$3
  start_srv $ARM
  case $MODE in
    sse) /tmp/ab-client -url "https://127.0.0.1:$SRV_PORT/sse" -streams 200 \
           -seconds 10 -warmup 1 -label "r$R-$ARM" 2>&1 \
           | grep -E "sse latency|failed=[1-9]" | sed "s/^/r$R $ARM sse /";;
    h2)  T0=$(date +%s)
         timeout 60 h2load -n 400000 -c 50 -m 10 -t 4 \
           "https://127.0.0.1:$SRV_PORT/" > /tmp/ab-h2.txt 2>&1
         RC=$?; SECS=$(( $(date +%s) - T0 ))
         if [ $RC -ne 0 ]; then echo "r$R $ARM h2 WEDGE ${SECS}s"
         elif ! grep -q "400000 succeeded" /tmp/ab-h2.txt; then echo "r$R $ARM h2 PARTIAL"
         elif [ $SECS -gt 10 ]; then echo "r$R $ARM h2 SLOW ${SECS}s"
         else echo "r$R $ARM h2 PASS $(grep -o "finished in [0-9.]*[a-z]*, [0-9.]*" /tmp/ab-h2.txt | head -1)"
         fi;;
    *)   /tmp/ab-client -streams 0 -oneshot-url "https://127.0.0.1:$SRV_PORT/" \
           -oneshot-workers $MODE -seconds 5 -warmup 1 -label "r$R-$ARM" 2>&1 \
           | grep oneshot | sed "s/^/r$R $ARM w$MODE /";;
  esac
  stop_srv
}
MODES="2 8 sse"
command -v h2load > /dev/null && MODES="$MODES h2"
ROWS=0
for MODE in $MODES; do
  R=1
  while [ $R -le '"$ROUNDS"' ]; do
    if [ $((R % 2)) -eq 0 ]; then row base $R $MODE; row new $R $MODE
    else row new $R $MODE; row base $R $MODE; fi
    ROWS=$((ROWS+2)); R=$((R+1))
  done
done
echo "rows=$ROWS"
[ $ROWS -gt 0 ] || exit 2'
