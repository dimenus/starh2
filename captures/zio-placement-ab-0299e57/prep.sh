#!/bin/sh
# Build every arm of the placement / #718-rate session on zio 0299e57.
#
#   WORK=<dir> captures/zio-placement-ab-0299e57/prep.sh
#
# starh2 tree: 1080022 (branch starh2/zio-local-placement), exported with
# `git archive`, for every 0299e57-based arm. It adds `-Dzio-scheduling`,
# `--spawn-placement auto|local`, and prints the scheduling and placement the
# binary really uses in its ready line. Arm A is the 3786083 reference from
# captures/zio-pin-ab-0299e57 (908992b + d8e46d6), copied from A_BIN.
#
# Arms (arm names must be shell identifiers):
#   A      3786083, task migration on (reference, prebuilt)
#   WS     0299e57 stock, -Dzio-scheduling=work_stealing       (the control)
#   PL     0299e57, -Dzio-scheduling=pinned, run with --spawn-placement local
#   PA     the SAME pinned binary, run with --spawn-placement auto
#   WSN    0299e57 with #718's peek run on every 61st busy work check
# Mechanism-only arms (not in the main session):
#   WSI    0299e57 + io_uring poll counters printed every 1024th poll
#   WSIR   WSI with #747 (c8732b4) reverse-applied
#   WSR    0299e57 without the #718 peek line (as unpeek in the bisect)
#
# Each zio variant is a tarball fetched with `zig fetch --save=zio`, so every
# arm embeds exactly one zio-<ver>-<hash> and the harness checks it against
# the tree's build.zig.zon. After each build the zio options file zig
# generated must name exactly the scheduling asked for.
set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(CDPATH= cd -- "$HERE/../.." && pwd -P)
WORK=${WORK:?WORK is required}
ZIO=${ZIO:-$HOME/Source/mine/zio}
BSSL=${BSSL:-$HOME/Source/oss/http2-zig-hendrik/boringssl}
CERTS=${CERTS:-$REPO/testdata}
A_BIN=${A_BIN:?A_BIN is required: the arm-A starh2-bench-server from captures/zio-pin-ab-0299e57}
A_TREE=${A_TREE:?A_TREE is required: the arm-A tree (908992b + d8e46d6)}
L_REF=1080022
ZIO_REF=0299e57
PEEK='            _ = self.run_queue.refill(1, .try_only);'

mkdir -p "$WORK"
rm -rf "$WORK/treeL"; mkdir -p "$WORK/treeL/testdata"
git -C "$REPO" archive "$L_REF" | tar -x -C "$WORK/treeL"
cp "$CERTS/cert.pem" "$CERTS/key.pem" "$WORK/treeL/testdata/"

# zio variants ---------------------------------------------------------------

zio_base() {
  d=$WORK/zio-$1
  rm -rf "$d"; mkdir -p "$d"
  git -C "$ZIO" archive "$ZIO_REF" | tar -x -C "$d"
  echo "$d"
}
pack() { (cd "$1" && tar -czf "$1.tar.gz" .); }

# #718 at 1/61: a per-executor counter; the peek runs when it reaches a
# multiple of 61. Everything else is unchanged.
patch_718n() {
  f=$1/src/runtime.zig
  awk -v peek="$PEEK" '
    $0 == "    tick_task_count: u32 = 0," {
      print
      print "    /// Busy-ring work checks since start; the single-task overflow peek"
      print "    /// runs on every 61st, as Go polls its global run queue every 61st tick."
      print "    overflow_peek_tick: u32 = 0,"
      n1++; next
    }
    $0 == peek {
      print "            self.overflow_peek_tick +%= 1;"
      print "            if (self.overflow_peek_tick % 61 == 0) _ = self.run_queue.refill(1, .try_only);"
      n2++; next
    }
    { print }
    END { if (n1 != 1 || n2 != 1) { print "718n anchors found: " n1 + 0 " " n2 + 0 > "/dev/stderr"; exit 1 } }
  ' "$f" > "$f.new"
  mv "$f.new" "$f"
}

patch_unpeek() {
  f=$1/src/runtime.zig
  n=$(grep -cxF "$PEEK" "$f" || true)
  [ "$n" = 1 ] || { echo "expected the #718 peek once, found $n" >&2; exit 1; }
  grep -vxF "$PEEK" "$f" > "$f.new"
  mv "$f.new" "$f"
}

# Diagnostic counters in the io_uring poll: calls, zero-timeout calls,
# zero-timeout calls that found no completion, and CQEs. One plain increment
# each (a loop is polled by one thread). The bench server has no SIGTERM
# handler, so a loop is never freed when the harness stops it; the counters
# are printed every 1024th poll instead, keyed by the loop's address, and the
# last line per loop is the total to within 1023 polls.
patch_instr() {
  f=$1/src/ev/backends/linux/io_uring.zig
  awk '
    $0 == "inflight: usize = 0," {
      print
      print "stat_polls: u64 = 0,"
      print "stat_zero: u64 = 0,"
      print "stat_zero_empty: u64 = 0,"
      print "stat_cqes: u64 = 0,"
      n1++; next
    }
    $0 == "    const count = try self.ring.copy_cqes(&self.cqe_buf, 0);" {
      print
      print "    self.stat_polls += 1;"
      print "    self.stat_cqes += count;"
      print "    if (effective_timeout.value == 0) {"
      print "        self.stat_zero += 1;"
      print "        if (count == 0) self.stat_zero_empty += 1;"
      print "    }"
      print "    if (self.stat_polls % 1024 == 0) std.debug.print(\"iouring-stats loop={x} polls={d} zero={d} zero_empty={d} cqes={d}\\n\", .{ @intFromPtr(self), self.stat_polls, self.stat_zero, self.stat_zero_empty, self.stat_cqes });"
      n2++; next
    }
    { print }
    END { if (n1 != 1 || n2 != 1) { print "instr anchors found: " n1 + 0 " " n2 + 0 > "/dev/stderr"; exit 1 } }
  ' "$f" > "$f.new"
  mv "$f.new" "$f"
}

revert_747() { git -C "$ZIO" show c8732b4 --format= -- src | (cd "$1" && git apply -R); }

orig=$(zio_base orig)
d=$(zio_base 718n); patch_718n "$d"; pack "$d"
diff -u "$orig/src/runtime.zig" "$d/src/runtime.zig" | sed "s|$WORK/||g" > "$HERE/zio-718n.diff" || true
d=$(zio_base unpeek); patch_unpeek "$d"; pack "$d"
d=$(zio_base instr); patch_instr "$d"; pack "$d"
diff -u "$orig/src/ev/backends/linux/io_uring.zig" "$d/src/ev/backends/linux/io_uring.zig" | sed "s|$WORK/||g" > "$HERE/zio-instr.diff" || true
d=$(zio_base instr-r747); patch_instr "$d"; revert_747 "$d"; pack "$d"
diff -u "$orig/src/ev/backends/linux/io_uring.zig" "$d/src/ev/backends/linux/io_uring.zig" | sed "s|$WORK/||g" > "$HERE/zio-instr-r747.diff" || true

# builds -------------------------------------------------------------------------

build_arm() {
  # build_arm <arm> <scheduling> <zio tarball or "">
  arm=$1; sched=$2; src=$3
  t=$WORK/tree-$arm
  rm -rf "$t" "$WORK/bins/out-$arm"
  mkdir -p "$t"
  (cd "$WORK/treeL" && tar -cf - .) | (cd "$t" && tar -xf -)
  [ -z "$src" ] || (cd "$t" && ./zb fetch --save=zio "$src")
  (cd "$t" && ./zb build starh2-bench-server -Doptimize=ReleaseFast -Dzio-scheduling="$sched" \
    -Dtarget=x86_64-linux-musl -Dboringssl-source-path="$BSSL" --prefix "$WORK/bins/out-$arm")
  want="pub const scheduling: []const u8 = \"$sched\";"
  got=$(grep -rh --include=options.zig 'pub const scheduling: \[\]const u8' "$t/.zig-cache/c" | sort -u)
  [ "$got" = "$want" ] || { echo "arm $arm: zio options say '$got', expected '$want'" >&2; exit 1; }
  echo "arm $arm: src=${src:-pinned in $L_REF build.zig.zon}"
  echo "arm $arm: $(grep -A2 '\.zio = ' "$t/build.zig.zon" | sed -n 's/.*\.hash = "\(.*\)".*/\1/p')"
  echo "arm $arm: $got"
  echo "arm $arm: sha256=$(shasum -a 256 "$WORK/bins/out-$arm/bin/starh2-bench-server" | cut -d' ' -f1)"
}

# ONLY="WSI WSIR" rebuilds just those arms.
want() { [ -z "${ONLY:-}" ] && return 0; case " $ONLY " in *" $1 "*) return 0;; esac; return 1; }
want WS && build_arm WS work_stealing ""
want P && build_arm P pinned ""
want WSN && build_arm WSN work_stealing "$WORK/zio-718n.tar.gz"
want WSR && build_arm WSR work_stealing "$WORK/zio-unpeek.tar.gz"
want WSI && build_arm WSI work_stealing "$WORK/zio-instr.tar.gz"
want WSIR && build_arm WSIR work_stealing "$WORK/zio-instr-r747.tar.gz"

# PL and PA are one binary under two names; the harness allows that only
# because their args differ, and checks each start's ready line.
for arm in PL PA; do
  rm -rf "$WORK/bins/out-$arm"; mkdir -p "$WORK/bins/out-$arm/bin"
  cp "$WORK/bins/out-P/bin/starh2-bench-server" "$WORK/bins/out-$arm/bin/"
  ln -sfn "$WORK/tree-P" "$WORK/tree-$arm"
done
# The same two zio builds on the 619242f starh2 tree, from the bisect
# (captures/zio-pin-bisect-0299e57): B is stock 0299e57, U is 0299e57 without
# the #718 peek line. Paired with WS and WSR they separate the starh2 tree
# (619242f vs 1080022) from zio. Copied when PREV_WORK names that WORK dir.
if [ -n "${PREV_WORK:-}" ]; then
  for pair in B:out-B:treeB U:out-unpeek:tree-unpeek; do
    arm=${pair%%:*}; rest=${pair#*:}; out=${rest%%:*}; tree=${rest#*:}
    rm -rf "$WORK/bins/out-$arm"; mkdir -p "$WORK/bins/out-$arm/bin"
    cp "$PREV_WORK/bins/$out/bin/starh2-bench-server" "$WORK/bins/out-$arm/bin/"
    ln -sfn "$PREV_WORK/$tree" "$WORK/tree-$arm"
  done
fi
rm -rf "$WORK/bins/out-A"; mkdir -p "$WORK/bins/out-A/bin"
cp "$A_BIN" "$WORK/bins/out-A/bin/starh2-bench-server"
ln -sfn "$A_TREE" "$WORK/tree-A"
shasum -a 256 "$WORK"/bins/out-*/bin/starh2-bench-server
