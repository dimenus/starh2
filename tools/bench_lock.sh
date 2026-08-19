# Per-box bench mutex. Source this, then wrap MEASUREMENT (never builds):
#
#   . "$(dirname "$0")/bench_lock.sh"   # adjust path per script
#   bench_lock
#   ... measure ...
#   bench_unlock                        # also call from your cleanup trap
#
# Two benches on one box corrupt each other's numbers, so every harness in
# tools/ takes this lock around the window that measures. Contention becomes
# a visible wait, never a silent corruption.
#
# Mechanics: mkdir is the atomic primitive (macOS has no flock(1)). The
# holder records its pid; a lock whose holder is dead is reclaimed. A
# process that already holds the lock (BENCH_LOCK_HELD exported) skips, so
# a matrix runner can wrap a harness that also locks.
BENCH_LOCK_DIR=${BENCH_LOCK_DIR:-/tmp/starh2-bench.lock}

bench_lock() {
  if [ "${BENCH_LOCK_HELD:-0}" = 1 ]; then
    return 0
  fi
  while ! mkdir "$BENCH_LOCK_DIR" 2>/dev/null; do
    holder=$(cat "$BENCH_LOCK_DIR/pid" 2>/dev/null || echo unknown)
    if [ "$holder" != unknown ] && ! kill -0 "$holder" 2>/dev/null; then
      # Dead holder. Reclaim; mkdir decides the race between reclaimers.
      rm -rf "$BENCH_LOCK_DIR" 2>/dev/null
      continue
    fi
    echo "bench-lock: waiting on holder pid=$holder ($BENCH_LOCK_DIR)" >&2
    sleep 5
  done
  echo $$ > "$BENCH_LOCK_DIR/pid"
  BENCH_LOCK_HELD=1
  export BENCH_LOCK_HELD
}

bench_unlock() {
  if [ "${BENCH_LOCK_HELD:-0}" != 1 ]; then
    return 0
  fi
  if [ "$(cat "$BENCH_LOCK_DIR/pid" 2>/dev/null)" = "$$" ]; then
    rm -rf "$BENCH_LOCK_DIR"
  fi
  BENCH_LOCK_HELD=0
  export BENCH_LOCK_HELD
}
