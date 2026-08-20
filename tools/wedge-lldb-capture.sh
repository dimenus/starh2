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
import lldb
target = lldb.debugger.GetSelectedTarget()
process = target.GetProcess()
for thread in process:
    for frame in thread:
        name = frame.GetFunctionName() or ''
        if ('parkAndSearch' in name or 'Executor.run' in name
                or 'waitTask' in name or 'Waiter.wait' in name
                or 'timedWait' in name or 'waitTimeout' in name):
            print('=== thread %d frame %s ===' % (thread.GetIndexID(), name))
            for var in frame.GetVariables(True, True, False, True):
                print(var)
PYEOF

lldb -b -p "$PID" \
  -o "settings set auto-confirm true" \
  -o "thread backtrace all" \
  -o "command script import $PYFILE" \
  -o "detach" \
  > "$OUT" 2>&1
rm -rf "$PYDIR"

echo "wedge state captured to $OUT (process left running)"
