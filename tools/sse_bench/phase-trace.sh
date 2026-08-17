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
STARH2_EXECUTORS=${STARH2_EXECUTORS:-2}
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
"$STARH2" --mode tls --port 19448 --sse-interval-ms 1 --executors "$STARH2_EXECUTORS" --trace --trace-every 256 > /tmp/tr-server.log 2>&1 &
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
def delta(k):
    return b[k] - a[k]
split_n = delta("ack_split_samples")
if split_n:
    actor_us = delta("actor_ns") / split_n / 1000
    queue_us = delta("queue_ns") / split_n / 1000
    pump_us = delta("pump_ns") / split_n / 1000
    print(f"    ack split ({split_n} samples): actor={actor_us:.1f}us queue={queue_us:.1f}us pump={pump_us:.1f}us")
pump_split_n = delta("pump_split_samples")
if pump_split_n:
    write_us = delta("write_ns") / pump_split_n / 1000
    drain_us = delta("drain_ns") / pump_split_n / 1000
    print(f"    pump split ({pump_split_n} samples): queued/write={write_us:.1f}us ack-drain={drain_us:.1f}us")
handoffs = delta("handoffs")
tickets = delta("tickets")
records = delta("records")
turns = delta("emit_turns")
emit_tickets = delta("emit_tickets")
write_calls = delta("write_calls")
write_chunks = delta("write_chunks")
handoff_bytes = delta("handoff_bytes")
emit_max = delta("emit_max")
write_max = b["write_max"]
batch_1 = delta("batch_1")
batch_2 = delta("batch_2")
batch_le4 = delta("batch_le4")
batch_le8 = delta("batch_le8")
batch_le16 = delta("batch_le16")
batch_ge17 = delta("batch_ge17")
print("  transport shape:")
print(f"    handoffs={handoffs} tickets={tickets} tickets/handoff={tickets/max(handoffs,1):.2f} bytes/handoff={handoff_bytes/max(handoffs,1):.1f}")
print(f"    TLS records={records} handoffs/record={handoffs/max(records,1):.2f} emit tickets/turn={emit_tickets/max(turns,1):.2f} emit_max={emit_max}")
print(f"    socket writes={write_calls} records/write={write_chunks/max(write_calls,1):.2f} write_max={write_max}")
print(f"    batches 1={batch_1} 2={batch_2} <=4={batch_le4} <=8={batch_le8} <=16={batch_le16} >=17={batch_ge17}")
'
}

echo "== 1 connection: SATURATED =="
run_phase "starh2 c1" 1
echo "== 8 connections: NOT saturated =="
run_phase "starh2 c8" 8

kill $S_PID 2>/dev/null
wait 2>/dev/null
