#!/bin/sh
# Where does a flushed SSE event actually spend its time?
#
# Four boundaries per sampled event, three intervals plus the receipt:
#   block  = waiting to acquire session_mu     -> a convoy on the lock
#   hold   = inside the critical section       -> the serial work itself
#   ack    = unlock until AckDrainer completes -> transport handoff
#   resume = completion until the handler runs -> wake-to-run latency
#
# Read at 1 connection (saturated) and at 8 connections (not saturated). The
# interval that GROWS between them is the one that saturation is made of.
set -u
OUT=${OUT:-/tmp/starh2-sse-bench}
REPO=$(cd "$(dirname "$0")/../.." && pwd -P)
cd "$REPO"
# Install to a known prefix and use THAT path. Picking the newest binary in
# .zig-cache by mtime measures whatever the last build step happened to write:
# after `zig build ci`, that is an example at a different optimize level, and
# the run then reports a number for a binary nobody asked for. Observed: a
# 4.7x improvement read back as no change at all.
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/starh2" || exit 1
STARH2="$OUT/starh2/bin/starh2-bench-server"
[ -x "$OUT/sse-client" ] || { echo "run tools/sse_bench/run.sh first to build the client" >&2; exit 1; }
"$STARH2" --mode tls --port 19448 --sse-interval-ms 1 --trace --trace-every 256 > /tmp/tr-server.log 2>&1 &
S_PID=$!
sleep 2

read_trace() {
  curl -sk --http2-prior-knowledge https://127.0.0.1:19448/trace
}

run_phase() {
  label="$1"; conns="$2"
  before=$(read_trace)
  "$OUT/sse-client" -url https://127.0.0.1:19448/sse -streams 200 -seconds 10 -conns "$conns" -label "$label" | sed 's/^/  /'
  after=$(read_trace)
  echo "$before" "$after" | python3 -c '
import sys, json
a, b = sys.stdin.read().split("\n")[0].split("} {")
a = json.loads(a + "}"); b = json.loads("{" + b)
n = b["samples"] - a["samples"]
if n == 0:
    print("  NO SAMPLES — the trace never fired, so there is nothing to read")
    raise SystemExit
def us(k):
    return (b[k] - a[k]) / n / 1000.0
tot = us("block_ns") + us("hold_ns") + us("ack_ns") + us("resume_ns")
sk = b["skipped"] - a["skipped"]
print(f"  samples={n}  skipped={sk}  ({100*n/(n+sk):.0f}% of sampled events measured)  mean total={tot:8.1f}us")
for name, key in (("block ", "block_ns"), ("hold  ", "hold_ns"), ("ack   ", "ack_ns"), ("resume", "resume_ns")):
    v = us(key)
    print(f"    {name} {v:8.1f}us  {100*v/tot:5.1f}%")
'
}

echo "== 1 connection: SATURATED =="
run_phase "starh2 c1" 1
echo "== 8 connections: NOT saturated =="
run_phase "starh2 c8" 8

kill $S_PID 2>/dev/null
wait 2>/dev/null
