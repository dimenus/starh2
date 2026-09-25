#!/bin/sh
# Bisect the two regressions of the zio 3786083 -> 0299e57 pin move
# (captures/zio-pin-ab-0299e57) over upstream's first-parent line.
#
#   captures/zio-pin-bisect-0299e57/bisect.sh arm <zio-commit>      # build arm z<short>
#   captures/zio-pin-bisect-0299e57/bisect.sh revert <zio-commit>   # build 0299e57 minus that commit
#   captures/zio-pin-bisect-0299e57/bisect.sh unpeek                # build 0299e57 minus the #718 peek line
#   captures/zio-pin-bisect-0299e57/bisect.sh pair <arm> [<arm2>]   # run <arm2 or A> against <arm>
#
# WORK must hold treeA, treeB and bins/out-A, bins/out-B from
# captures/zio-pin-ab-0299e57/run.sh prep.
#
# # The two metrics, and nothing else
#
# M1: server CPU per SSE event at 200 streams (phase 3, cpu200).
# M2: h2load one-shot p99 at 2 executors (phase 1, oneshot-lat-e2).
# Every other row is switched off, so a step costs about five minutes.
# CPU_ROUNDS is 8, not 6, because a cpu200 row that fails closed is excluded
# and leaves its round unpaired; 8 rounds usually leave at least 6 pairs.
#
# # Which starh2 tree an arm uses
#
# zio 6087148 (#752) removed RuntimeOptions.enable_task_migration and added
# the `scheduling` build option. A commit before it builds in the arm-A tree
# (908992b + d8e46d6), whose bench server sets enable_task_migration = true
# and whose zio build defaults task-migration to true. A commit from 6087148
# on builds in the 619242f tree, which passes `.scheduling = .work_stealing`
# to zio explicitly; that matters between 6087148 and 5c35581, where zio's
# own default was single_executor. After the build, the zio options file zig
# generated must say exactly that one setting, or the arm is refused.
#
# # How an arm points at a commit
#
# `zig fetch --save=zio git+https://github.com/lalinsky/zio#<sha>` in a fresh
# copy of the tree. The hash it writes is the package the arm embeds;
# tools/zio-arm-ab.sh checks the binary against it (TREE_<arm>).
#
# A revert arm is 0299e57 with one commit reverse-applied, packed as a
# tarball and fetched the same way, so it too embeds one zio-<ver>-<hash>.
set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}
ZIO=${ZIO:-$HOME/Source/mine/zio}
BSSL=${BSSL:-$HOME/Source/oss/http2-zig-hendrik/boringssl}
SPLIT=6087148

build_in() {
  # build_in <arm> <base tree> <fetch source>
  arm=$1; base=$2; src=$3
  t=$WORK/tree-$arm
  rm -rf "$t" "$WORK/bins/out-$arm"
  mkdir -p "$t"
  (cd "$WORK/$base" && tar -cf - --exclude ./.zig-cache --exclude ./zig-out .) | (cd "$t" && tar -xf -)
  (cd "$t" && ./zb fetch --save=zio "$src")
  (cd "$t" && ./zb build starh2-bench-server -Doptimize=ReleaseFast \
    -Dtarget=x86_64-linux-musl -Dboringssl-source-path="$BSSL" --prefix "$WORK/bins/out-$arm")
  if [ "$base" = treeB ]; then
    want='pub const scheduling: []const u8 = "work_stealing";'
    got=$(grep -rh --include=options.zig 'pub const scheduling: \[\]const u8' "$t/.zig-cache/c" | sort -u)
  else
    want='pub const task_migration: bool = true;'
    got=$(grep -rh --include=options.zig 'pub const task_migration: bool' "$t/.zig-cache/c" | sort -u)
  fi
  [ "$got" = "$want" ] || { echo "arm $arm: zio options say '$got', expected '$want'" >&2; exit 1; }
  echo "arm $arm: base=$base src=$src"
  echo "arm $arm: $(grep -A2 '\.zio = ' "$t/build.zig.zon" | sed -n 's/.*\.hash = "\(.*\)".*/\1/p')"
  echo "arm $arm: $got"
  echo "arm $arm: sha256=$(shasum -a 256 "$WORK/bins/out-$arm/bin/starh2-bench-server" | cut -d' ' -f1)"
}

case "${1:-}" in
  arm)
    full=$(git -C "$ZIO" rev-parse "${2:?commit}^{commit}")
    short=$(printf '%.7s' "$full")
    if git -C "$ZIO" merge-base --is-ancestor "$SPLIT" "$full"; then base=treeB; else base=treeA; fi
    build_in "z$short" "$base" "git+https://github.com/lalinsky/zio#$full"
    ;;
  revert)
    full=$(git -C "$ZIO" rev-parse "${2:?commit}^{commit}")
    short=$(printf '%.7s' "$full")
    src=$WORK/zio-0299e57-minus-$short
    rm -rf "$src"; mkdir -p "$src"
    git -C "$ZIO" archive 0299e57 | tar -x -C "$src"
    # Source only, and `git apply`, which refuses a hunk that does not apply
    # instead of asking on a closed stdin the way `patch` does.
    git -C "$ZIO" show "$full" --format= -- src | (cd "$src" && git apply -R)
    (cd "$src" && tar -czf "$src.tar.gz" .)
    build_in "r$short" treeB "$src.tar.gz"
    ;;
  unpeek)
    # 0299e57 without #718's behaviour. eaa9823 does not reverse-apply there
    # (6087148 and a30e560 rewrote the lines around it), so this removes the
    # one line that carries its behaviour: the single-task overflow peek in
    # checkLocalWork on a non-empty ring. The rest of #718 is refill returning
    # a count and the lock-mode parameter, which no remaining caller changes
    # behaviour on. Without the line, a busy ring returns "has work" without
    # touching the overflow queue, as it did before #718.
    #
    # `unpeek <commit>` also reverse-applies that commit's src/ diff, so both
    # culprits can be backed out of one build (arm `unpeekr<short>`).
    arm=unpeek
    [ -n "${2:-}" ] && arm=unpeekr$(printf '%.7s' "$(git -C "$ZIO" rev-parse "$2^{commit}")")
    src=$WORK/zio-0299e57-$arm
    rm -rf "$src"; mkdir -p "$src"
    git -C "$ZIO" archive 0299e57 | tar -x -C "$src"
    line='            _ = self.run_queue.refill(1, .try_only);'
    n=$(grep -cxF "$line" "$src/src/runtime.zig" || true)
    [ "$n" = 1 ] || { echo "expected the #718 peek exactly once in runtime.zig, found $n" >&2; exit 1; }
    grep -vxF "$line" "$src/src/runtime.zig" > "$src/runtime.zig.new"
    mv "$src/runtime.zig.new" "$src/src/runtime.zig"
    if [ -n "${2:-}" ]; then
      git -C "$ZIO" show "$2" --format= -- src | (cd "$src" && git apply -R)
    fi
    (cd "$src" && tar -czf "$src.tar.gz" .)
    build_in "$arm" treeB "$src.tar.gz"
    ;;
  pair)
    first=${3:-A}; second=${2:?arm}
    eval "export TREE_$first=\$WORK/tree-$first"
    [ "$first" = A ] && export TREE_A=$WORK/treeA
    [ "$first" = B ] && export TREE_B=$WORK/treeB
    eval "export TREE_$second=\$WORK/tree-$second"
    [ "$second" = B ] && export TREE_B=$WORK/treeB
    ARMS="$first $second" BIN_ROOT="$WORK/bins" \
      HOST=${HOST:-ryan@100.113.184.27} \
      H2LOAD=${H2LOAD:-/home/ryan/.local/opt/nghttp2-1.70.0/bin/h2load} \
      REMOTE_DIR=${REMOTE_DIR:-/tmp/zio-pin-bisect} CLIENT_BIN="$WORK/zioab-client" \
      PHASES="${PHASES:-1 3}" PERF_ROUNDS=${PERF_ROUNDS:-6} CPU_ROUNDS=${CPU_ROUNDS:-8} \
      PERF_SSE_STREAMS="" ONESHOT_PLAIN=0 ONESHOT_LAT=1 ONESHOT_WIDTHS=2 ONESHOT_N=2000000 \
      CPU_STREAMS=200 "$REPO/tools/zio-arm-ab.sh"
    ;;
  *)
    echo "usage: WORK=<dir> $0 arm <commit> | revert <commit> | pair <arm> [<arm2>]" >&2
    exit 2
    ;;
esac
