#!/bin/bash
# Gate: this repo does not use std.Io CONCURRENCY abstractions. Use zio, or ask.
#
# Why. Three defects in one day, all from a std.Io abstraction whose semantics
# differ from the zio primitive underneath it:
#   t-1760  std.Io.Select.cancelDiscard threw away bytes already off the socket.
#   t-1802  std.Io.Future.cancel is request PLUS mandatory await, so a reaper
#           worker blocks forever on a handler in an uncancelable wait. zio's
#           AnyTask.cancel is setCanceled+wake and returns at once.
#   t-1802  std.Io.Event.set on an already-set event performs a release store
#           but does not futexWake and does not re-synchronize a waiter that
#           already returned, so a plain field read after it is a data race.
#
# std.Io itself is NOT banned: it is the vtable zio implements, so `io: std.Io`
# is correct. std.Io.Cancelable is an error set. net/Writer/Reader/Timestamp/
# Clock/Timeout are generic IO. Only the concurrency primitives are banned.
set -uo pipefail
# --update regenerates baseline.txt from the measured state. Use it when a
# refactor REMOVES usages, so progress does not fail the build. It never
# raises a count: a new violation still fails, even with --update.
UPDATE=0
case "${1:-}" in --update) UPDATE=1; shift;; esac
ROOT=${1:-.}
BASE="$(cd "$(dirname "$0")" && pwd)/baseline.txt"
BANNED='Event|Queue|Group|Future|Select|Mutex|Condition|Semaphore'

[ -d "$ROOT/src" ] || { echo "FATAL: no src/ under $ROOT"; exit 3; }

# Enumerate; never hardcode a file list. Recursive; a new subdirectory must not
# fall outside the scan. Portable to bash 3.2 (macOS): no mapfile, no arrays.
LIST=$(mktemp)
find "$ROOT/src" -name '*.zig' -type f | sort > "$LIST"
NFILES=$(wc -l < "$LIST" | tr -d ' ')
NLINES=$(xargs cat < "$LIST" 2>/dev/null | wc -l | tr -d ' ')

# Coverage is REPORTED, and zero is fatal. A scan that shrinks must be visible.
echo "std-io-gate: scanned $NFILES files, $NLINES lines under $ROOT/src"
[ "$NFILES" -gt 0 ] || { echo "FATAL: scanned zero files"; rm -f "$LIST"; exit 3; }

CUR=$(mktemp)
while read -r f; do
  rel=${f#"$ROOT"/}
  # Strip // comments first. A doc comment that NAMES a banned API (to explain
  # why it is banned) is not a usage, and counting it would penalise writing
  # the explanation. Only removes text, so it cannot hide real code.
  sed 's://.*$::' "$f" 2>/dev/null \
    | grep -oE "std\.Io\.($BANNED)\b" | sed "s|^|$rel |"
done < "$LIST" | sort | uniq -c | awk '{print $2, $3, $1}' | sort > "$CUR"

if [ ! -f "$BASE" ]; then
  echo "no baseline; writing measured state to $BASE"
  cp "$CUR" "$BASE"; cat "$BASE"; exit 0
fi

# Ratchet both directions. Above baseline = new violation. Below baseline =
# progress that must be recorded, so the allowance cannot silently drift back.
RC=0
NEW=0
while read -r file sym count; do
  [ -z "${file:-}" ] && continue
  was=$(awk -v f="$file" -v s="$sym" '$1==f && $2==s {print $3}' "$BASE")
  was=${was:-0}
  if [ "$count" -gt "$was" ]; then
    echo "FAIL $file: $sym used $count times, baseline $was. Use zio, or ask Ryan."
    RC=1; NEW=1
  elif [ "$count" -lt "$was" ]; then
    if [ "$UPDATE" -eq 1 ]; then
      echo "ratchet down $file: $sym $was -> $count"
    else
      echo "RATCHET $file: $sym down to $count from $was. Rerun with --update."
      RC=1
    fi
  fi
done < "$CUR"

while read -r file sym count; do
  [ -z "${file:-}" ] && continue
  now=$(awk -v f="$file" -v s="$sym" '$1==f && $2==s {print $3}' "$CUR")
  if [ -z "$now" ]; then
    if [ "$UPDATE" -eq 1 ]; then
      echo "ratchet down $file: $sym $count -> 0 (gone)"
    else
      echo "RATCHET $file: $sym is gone (was $count). Rerun with --update."
      RC=1
    fi
  fi
done < "$BASE"

# Never let --update mask a NEW violation.
if [ "$UPDATE" -eq 1 ] && [ "$NEW" -eq 0 ]; then
  cp "$CUR" "$BASE"; echo "baseline updated"; RC=0
fi

TOTAL=$(awk '{s+=$3} END {print s+0}' "$CUR")
echo "std-io-gate: $TOTAL banned usages remain (baseline $(awk '{s+=$3} END {print s+0}' "$BASE"))"
rm -f "$CUR" "$LIST"
exit $RC
