#!/bin/sh
# Capture zio runtime STATE from a live wedged process with lldb, then leave
# the process alone (the caller decides whether to kill it).
#
#   tools/wedge-lldb-capture.sh <pid> [out-file]
#
# Why this exists: `sample` shows stacks, and every t-850 wedge sample looks
# the same (all threads in kevent, zero runnable). The three hypotheses the
# stacks cannot separate are: (1) the wake was never sent, (2) the wake was
# sent and not delivered (a futex signal count nobody consumed), (3) a task
# is runnable and no executor fetches it. Those live in DATA: the executors'
# run queues, the parked waiter's notify word, and the armed-timer state.
# Attach BEFORE killing a wedge; a killed wedge answers nothing.
#
# The walk: pick the thread parked in Executor.parkAndSearch, select that
# frame, and print `self.*` — Zig DWARF gives lldb the Executor, and from it
# the Runtime (queues, executors, timer state). Frame variable names track
# zio internals, so on a mismatch fall back to `frame variable` and walk by
# hand; the raw dump of every frame's variables is captured either way.
set -eu

PID=${1:?pid of the wedged process}
OUT=${2:-/tmp/wedge-lldb-$PID.txt}

PYDIR=$(mktemp -d /tmp/wedge-lldb-XXXXXX)
PYFILE=$PYDIR/wedge_state.py
cat > "$PYFILE" <<'PYEOF'
import lldb, struct
target = lldb.debugger.GetSelectedTarget()
process = target.GetProcess()
err = lldb.SBError()

for thread in process:
    for frame in thread:
        name = frame.GetFunctionName() or ''
        if ('parkAndSearch' in name or 'Executor.run' in name
                or 'waitTask' in name or 'Waiter.wait' in name
                or 'timedWait' in name or 'waitTimeout' in name):
            print('=== thread %d frame %s ===' % (thread.GetIndexID(), name))
            for var in frame.GetVariables(True, True, False, True):
                print(var)

# Enumerate EVERY live parked coroutine, not just futex waiters: scan
# anonymous memory regions for valid frame-pointer chains whose link
# registers resolve into the binary. Futex-bucket walks miss channel,
# ResetEvent, and select waiters (their wait queues are object-local);
# t-899's first analysis over-read the futex view (64 idle reaper
# workers looked like a leak, and the actually-wedged select waiter was
# invisible). Stacks are pool-recycled mmaps, so region granularity is
# the right unit.

def unwind(fp, limit=20):
    frames = []
    for _ in range(limit):
        data = process.ReadMemory(fp, 16, err)
        if err.Fail():
            break
        next_fp, lr = struct.unpack('<QQ', data)
        a = lldb.SBAddress(lr, target)
        sym = (a.GetFunction().GetName() or a.GetSymbol().GetName() or '')
        if not sym:
            break
        le = a.GetLineEntry()
        src = '%s:%d' % (le.GetFileSpec().GetFilename(), le.GetLine()) if le.IsValid() else '?'
        frames.append('%s (%s)' % (sym, src))
        if not next_fp or next_fp <= fp:
            break
        fp = next_fp
    return frames

regions = lldb.SBMemoryRegionInfoList()
regions = process.GetMemoryRegions()
info = lldb.SBMemoryRegionInfo()
seen = 0
for i in range(regions.GetSize()):
    if not regions.GetMemoryRegionAtIndex(i, info):
        continue
    if not (info.IsReadable() and info.IsWritable() and not info.IsExecutable()):
        continue
    size = info.GetRegionEnd() - info.GetRegionBase()
    if size < 0x10000 or size > 0x400000:
        continue
    if info.GetName():
        continue
    # Probe candidate frame pointers near the region top (stacks grow down;
    # a parked coroutine's frames sit just under its saved context).
    base, end = info.GetRegionBase(), info.GetRegionEnd()
    best = []
    for off in range(0x80, 0x2000, 8):
        ca = end - off
        d = process.ReadMemory(ca, 8, err)
        if err.Fail():
            continue
        v = struct.unpack('<Q', d)[0]
        if base < v < end and v > ca:
            fr = unwind(ca)
            if len(fr) > len(best):
                best = fr
    if len(best) >= 3:
        seen += 1
        print('=== live coroutine stack region 0x%x-0x%x ===' % (base, end))
        for f in best[:16]:
            print('   ', f)
print('live parked coroutine stacks found:', seen)
PYEOF

lldb -b -p "$PID" \
  -o "settings set auto-confirm true" \
  -o "thread backtrace all" \
  -o "command script import $PYFILE" \
  -o "detach" \
  > "$OUT" 2>&1
rm -rf "$PYDIR"

echo "wedge state captured to $OUT (process left running)"
