#!/bin/sh
# Where does a one-shot 13-byte response actually spend its resources?
#
# Throughput-shaped (use these for the live gap, not wall waits):
#   tickets/handoff  = receipts coalesced into one queueWire. Complete oneshots
#                      attach one receipt per packed write, so this sits near 1.
#                      Packing authority is records/response.
#   tickets/emit     = receipts flushed in one drainEmit turn
#   records/response = write chunks queued per h2load success. 1.00 means a
#   second HEADERS flushed the batch again; the script exits 9 rather than
#   reporting that as a result. Packed drain turns at -m 10 sit well below 0.4.
#   inbound_records/req = leftover counter from the old record loop (stays 0).
#   encrypt/decrypt/send ns/req = Clock.awake around sendAccountedWire; encrypt
#   and decrypt clocks stay 0 because SSL_write lives in TlsPump, not the actor.
#   decrypt_loop / accAppend / accCompact = unused after TLS-as-stream.
#   allocs/request   = counting-allocator calls on the server GPA (--trace only)
#   alloc_ns/request = GPA rawAlloc wall time including two Clock.awake reads.
#                      Overstates allocator cost; sample(1) is CPU-share authority.
#
# Latency-shaped (overlapped across 500 concurrent streams; not throughput):
#   block/hold/ack/resume  ticket-correlated send-path samples
#   spawn/to_send/hpack    dispatch-sampled lifecycle (complete handlers skip spawn)
#
# Cross-check writes against h2load succeeded, not tickets. Complete oneshots
# use one receipt per packed write, so tickets track handoffs, not responses.
# `jobs` is a dispatch counter and includes /trace.
#
# Tried and rejected (do not retry without new evidence): skipping the one-shot
# ticket wait collapsed 176k -> 1.6k req/s with lock-convoy block~10ms (receipt
# is the self-clock, same as SSE). Running a complete handler on the actor while
# still spawning per response and drain-emitting per job dropped 176k -> 123k.
# Do not re-try those shapes. The live cut is one drain-turn receipt, not one
# ticket per response and not a skipped wait.
set -eu
OUT=${OUT:-/tmp/starh2-oneshot-trace}
REPO=$(cd "$(dirname "$0")/.." && pwd -P)
cd "$REPO"
./zb build starh2-bench-server -Doptimize=ReleaseFast --prefix "$OUT/starh2"
STARH2="$OUT/starh2/bin/starh2-bench-server"
PORT=19447
"$STARH2" --mode tls --port "$PORT" --trace --trace-every 64 > /tmp/oneshot-tr-server.log 2>&1 &
S_PID=$!
trap 'kill $S_PID 2>/dev/null; wait $S_PID 2>/dev/null || true' EXIT
sleep 2

read_trace() {
  curl -sk --http2 "https://127.0.0.1:${PORT}/trace"
}

before=$(read_trace)
# CPU profile is resource-demand evidence. It cannot speak for blocked waits.
# 400k is ~2s at current packed TLS rates. An 8s sample was mostly idle after
# h2load exited and understated on-CPU share (malloc included).
if command -v sample >/dev/null 2>&1; then
  sample "$S_PID" 2 -file /tmp/starh2-oneshot-cpu.txt >/tmp/starh2-oneshot-sample.log 2>&1 &
  SAMPLE_PID=$!
fi
h2load -n 400000 -c 50 -m 10 -t 4 "https://127.0.0.1:${PORT}/" >/tmp/oneshot-tr-h2load.txt
after=$(read_trace)
if [ -n "${SAMPLE_PID:-}" ]; then
  wait "$SAMPLE_PID" 2>/dev/null || true
fi
echo "h2load:"
grep -E 'finished in|requests:|status codes:' /tmp/oneshot-tr-h2load.txt | sed 's/^/  /'
echo "before: $before"
echo "after:  $after"
echo "$before" "$after" | python3 -c '
import sys, json, re, subprocess
raw = sys.stdin.read().strip()
parts = re.split(r"\}\s*\{", raw, maxsplit=1)
if len(parts) != 2:
    print("  could not split before/after JSON")
    raise SystemExit(1)
a = json.loads(parts[0] + "}")
b = json.loads("{" + parts[1])
def d(k):
    return b.get(k, 0) - a.get(k, 0)
n = d("samples")
life = d("lifecycle")
jobs = d("jobs")
writes = d("writes")
handoffs = d("handoffs")
tickets = d("tickets")
records = d("records")
emit_turns = d("emit_turns")
emit_tickets = d("emit_tickets")
allocs = d("allocs")
alloc_bytes = d("alloc_bytes")
alloc_ns = d("alloc_ns")
free_ns = d("free_ns")
text = open("/tmp/oneshot-tr-h2load.txt").read()
m = re.search(r"requests: (\d+) total, (\d+) started, (\d+) done, (\d+) succeeded", text)
if not m:
    print("  could not parse h2load succeeded count")
    raise SystemExit(6)
succeeded = int(m.group(4))
print(f"  h2load succeeded={succeeded}  jobs={jobs}  writes={writes}  tickets={tickets}  handoffs={handoffs}")
if succeeded == 0:
    print("  NO SUCCEEDED REQUESTS")
    raise SystemExit(7)
if tickets == 0:
    print("  NO TICKETS — batch counters did not fire; trace.enabled gating is wrong")
    raise SystemExit(3)
write_ratio = writes / succeeded
if write_ratio < 0.9 or write_ratio > 1.1:
    print(f"  writes/succeeded ratio {write_ratio:.3f} is outside 0.9-1.1 — denominator mismatch")
    raise SystemExit(8)
# One receipt per packed write: tickets ≈ handoffs, not ≈ succeeded.
if handoffs == 0:
    print("  NO HANDOFFS — packed writes did not fire")
    raise SystemExit(3)
th = tickets / handoffs
if th < 0.5 or th > 2.0:
    print(f"  tickets/handoff {th:.3f} is outside 0.5-2.0 — drain-turn receipt did not attach")
    raise SystemExit(8)
if n == 0:
    print("  NO SAMPLES — the send-path trace never fired")
    raise SystemExit(2)
if life == 0:
    print("  NO LIFECYCLE SAMPLES — spawn/to_send were never stamped")
    raise SystemExit(5)
def us(total, count):
    if count == 0:
        return 0.0
    return total / count / 1000.0
print("  THROUGHPUT resource (cross-checked against h2load succeeded):")
handoff_max = d("handoff_max")
handoff_bytes = d("handoff_bytes")
b1 = d("batch_1"); b2 = d("batch_2"); b4 = d("batch_le4")
b8 = d("batch_le8"); b16 = d("batch_le16"); b17 = d("batch_ge17")
print(f"    handoffs={handoffs}  mean tickets/handoff={tickets/max(handoffs,1):.2f}  max={handoff_max}  bytes/handoff={handoff_bytes/max(handoffs,1):.1f}")
print(f"    histogram: 1={b1} 2={b2} <=4={b4} <=8={b8} <=16={b16} >=17={b17}")
print(f"    records={records}  records/response={records/succeeded:.2f}")
if records / succeeded > 0.4:
    print("  PACKING REGRESSION: records/response is one-per-response again")
    print("  a second HEADERS is flushing the TLS batch; packed drain turns sit well below 0.4 at -m 10")
    raise SystemExit(9)
inbound = d("inbound_records")
decrypt_n = d("decrypt_n")
encrypt_n = d("encrypt_n")
send_n = d("send_n")
loop_n = d("decrypt_loop_n")
print("  TLS transform (clock around the live call; amortize vs succeeded):")
print(f"    inbound_records={inbound}  records/req={inbound/succeeded:.2f}  cipher_in={d("decrypt_in")/max(inbound,1):.1f} B  plain={d("decrypt_plain")/max(inbound,1):.1f} B")
print(f"    decrypt     {d("decrypt_ns")/succeeded:8.0f} ns/req  n={decrypt_n}  {d("decrypt_ns")/max(decrypt_n,1):.0f} ns/call")
print(f"    encrypt     {d("encrypt_ns")/succeeded:8.0f} ns/req  n={encrypt_n}  {d("encrypt_ns")/max(encrypt_n,1):.0f} ns/call  plain={d("encrypt_bytes")/max(encrypt_n,1):.1f} B")
print(f"    sendWire    {d("send_ns")/succeeded:8.0f} ns/req  n={send_n}  {d("send_ns")/max(send_n,1):.0f} ns/call  {d("send_bytes")/max(send_n,1):.1f} B")
print(f"    decryptLoop {d("decrypt_loop_ns")/succeeded:8.0f} ns/req  n={loop_n}  {d("decrypt_loop_ns")/max(loop_n,1):.0f} ns/entry  [decrypt+ingest+intents under session_mu]")
print(f"    ingest      {d("ingest_ns")/succeeded:8.0f} ns/req  n={d("ingest_n")}  {d("ingest_ns")/max(d("ingest_n"),1):.0f} ns/call")
print(f"    intents     {d("intent_ns")/succeeded:8.0f} ns/req  n={d("intent_n")}  {d("intent_ns")/max(d("intent_n"),1):.0f} ns/call")
acc_n = d("acc_append_n")
cmp_n = d("acc_compact_n")
print(f"    accAppend   {d("acc_append_ns")/succeeded:8.0f} ns/req  n={acc_n}  {d("acc_append_bytes")/succeeded:.1f} B/req  [socket chunk -> tls_recv_acc]")
print(f"    accCompact  {d("acc_compact_ns")/succeeded:8.0f} ns/req  n={cmp_n}  {d("acc_compact_bytes")/succeeded:.1f} B/req  leftover/record={cmp_n/max(inbound,1):.2f}")
emit_max = d("emit_max")
receipt_depth = b.get("complete_receipt_depth_max", 0)
print(f"    emit_turns={emit_turns}  mean tickets/turn={emit_tickets/max(emit_turns,1):.2f}  max={emit_max}  complete_receipt_depth={receipt_depth}")
d0 = d("receipt_depth_0"); d1 = d("receipt_depth_1"); d2 = d("receipt_depth_2")
d3 = d("receipt_depth_3"); d4 = d("receipt_depth_4")
enter_full = d("receipt_full_enter")
full_ns = d("receipt_full_ns")
full_n = d("inline_full_n")
full_sids = d("inline_full_sids")
full_max = b.get("inline_full_max", 0)
reuse_n = d("credit_reuse_n")
reuse_ns = d("credit_reuse_ns")
print("  RECEIPT CREDIT (occupancy; kill widen if inline_full stays huge at cap 4):")
print(f"    depth after each change: 0={d0} 1={d1} 2={d2} 3={d3} 4={d4}  entered_full={enter_full}")
print(f"    time at capacity: {full_ns/1000:.1f}us total  {full_ns/max(enter_full,1)/1000:.1f}us/enter")
print(f"    inline skipped (depth full): n={full_n}  mean inline_n={full_sids/max(full_n,1):.1f}  max={full_max}")
print(f"    ack→credit reuse: n={reuse_n}  mean={reuse_ns/max(reuse_n,1)/1000:.1f}us")
take_n = d("read_take_n")
left0 = d("read_left_0"); left1 = d("read_left_1"); left2 = d("read_left_2")
left3 = d("read_left_3"); left_ge4 = d("read_left_ge4")
left_sum = d("read_left_sum"); left_max = b.get("read_left_max", 0)
print("  INBOUND QUEUE (leftover read_ch after each actor take, including extras):")
print(f"    takes={take_n}  leftover: 0={left0} 1={left1} 2={left2} 3={left3} >=4={left_ge4}")
print(f"    mean leftover={left_sum/max(take_n,1):.2f}  max={left_max}  backlog_rate={(take_n-left0)/max(take_n,1):.2%}")
b1 = d("inbound_batch_1"); b2 = d("inbound_batch_2"); b3 = d("inbound_batch_3"); b4 = d("inbound_batch_4")
turns = b1 + b2 + b3 + b4
print(f"    ingest turns: 1={b1} 2={b2} 3={b3} 4={b4}  mean chunks/turn={(b1+2*b2+3*b3+4*b4)/max(turns,1):.2f}")
print(f"    allocs={allocs}  ({allocs/succeeded:.2f}/req)  bytes={alloc_bytes}  ({alloc_bytes/succeeded:.1f} B/req)")
print(f"    alloc_ns={alloc_ns}  ({alloc_ns/succeeded:.1f} ns/req)  free_ns={free_ns}  ({free_ns/succeeded:.1f} ns/req)  (vtable clock; overstates)")
nfree = d("frees"); nfreeb = d("free_bytes"); overflow = d("alloc_overflow")
print(f"    frees={nfree}  free_bytes={nfreeb}  site_overflow={overflow}")
print("    top alloc sites (addr, count, bytes, ns) — symbolicate against the bench binary:")
for site in b.get("sites") or []:
    ns = site[3] if len(site) > 3 else 0
    print(f"      {site[0]:#x}  n={site[1]}  bytes={site[2]}  ns={ns}  ({ns/max(site[1],1):.1f} ns/call)")
print("  LATENCY (overlapped wall waits; not a throughput proof):")
tot = us(d("block_ns"), n) + us(d("hold_ns"), n) + us(d("ack_ns"), n) + us(d("resume_ns"), n)
for name, key in (("block ", "block_ns"), ("hold  ", "hold_ns"), ("hpack ", "hpack_ns"), ("ack   ", "ack_ns"), ("resume", "resume_ns")):
    v = us(d(key), n)
    pct = (100*v/tot) if tot else 0
    print(f"    {name} {v:8.1f}us  {pct:5.1f}%")
print(f"    total  {tot:8.1f}us")
split_n = d("ack_split_samples")
if split_n:
    print(f"    ack split ({split_n} samples): actor={us(d("actor_ns"), split_n):.1f}us queue={us(d("queue_ns"), split_n):.1f}us pump={us(d("pump_ns"), split_n):.1f}us")
pump_split_n = d("pump_split_samples")
if pump_split_n:
    print(f"    pump split ({pump_split_n} samples): queued/write={us(d("write_ns"), pump_split_n):.1f}us ack-drain={us(d("drain_ns"), pump_split_n):.1f}us")
write_calls = d("write_calls"); write_chunks = d("write_chunks")
if write_calls:
    print(f"    WritePump: calls={write_calls} chunks={write_chunks} chunks/call={write_chunks/write_calls:.2f} max={b.get("write_max", 0)}")
spawn_ns = d("spawn_ns"); to_send_ns = d("to_send_ns")
print("  lifecycle means (dispatch-sampled, also latency):")
print(f"    spawn   {us(spawn_ns, life):8.1f}us")
print(f"    to_send {us(to_send_ns, life):8.1f}us")
'
BIN="$STARH2"
if [ -f /tmp/starh2-oneshot-cpu.txt ]; then
  echo "CPU sample (top stacks):"
  # sample(1) output: named functions with sample counts. Keep the call-tree
  # summary, not the full binary dump.
  awk '
    /^Call graph:/ {p=1}
    /^Total number in stack/ {p=0}
    p && /[:]/ {print}
  ' /tmp/starh2-oneshot-cpu.txt | head -80
  echo "CPU sample (top of stack; sample(1) is 1ms and this reactor's work is tens of us, so this is usually kevent):"
  awk '/^Sort by top of stack/,/^Binary Images/' /tmp/starh2-oneshot-cpu.txt | head -20
  python3 - <<'PY'
text = open("/tmp/starh2-oneshot-cpu.txt").read()
idx = text.find("Sort by top of stack")
end = text.find("Binary Images:")
body = text[idx:end] if idx >= 0 else ""
keys = ("malloc", "free", "nanov2", "szone", "Allocator", "AllocTrace", "rawAlloc")
hits = []
total = 0
for line in body.splitlines():
    parts = line.split()
    if not parts or not parts[-1].isdigit():
        continue
    n = int(parts[-1])
    total += n
    name = line.strip()
    if any(k.lower() in name.lower() for k in keys):
        hits.append((n, name))
print(f"  top-of-stack samples={total}")
if not hits:
    print("  malloc/GPA: 0 samples at 1ms (expected; use alloc_ns for allocator wall time)")
else:
    for n, name in hits:
        print(f"  {100.0*n/max(total,1):5.1f}%  {n}  {name}")
PY
fi
if command -v atos >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1; then
  echo "atos of top alloc sites:"
  python3 - "$BIN" "$after" <<'PY'
import json, os, subprocess, sys
binary = sys.argv[1]
after = json.loads(sys.argv[2])
addrs = [hex(s[0]) for s in after.get("sites") or [] if s]
if not addrs:
    sys.exit(0)
load = None
sample_path = "/tmp/starh2-oneshot-cpu.txt"
if os.path.exists(sample_path):
    for line in open(sample_path):
        if line.startswith("Load Address:"):
            load = line.split()[-1]
            break
cmd = ["atos", "-o", binary]
if load:
    cmd += ["-l", load]
cmd += addrs
try:
    out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
    print(out)
except subprocess.CalledProcessError as e:
    print(e.output)
    print("atos failed; addresses remain hex")
PY
fi
