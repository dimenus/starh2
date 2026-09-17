#!/bin/sh
# Answers one question: can zig build its bundled libcxx against this macOS SDK?
#
# Default (one argument):
#   exit 0  YES — libcxx compiled and the test binary linked
#   exit 1  NO  — zig reported `sub-compilation of libcxx failed`
#   exit 2  the probe could not run, so it has NO answer
#
# --expect-no <sdk> (ci polarity):
#   exit 0  libcxx failed, which is the expected case. One confirmation line.
#           Zig's output stays in the temp file; nothing from zig is printed.
#   exit 1  libcxx built, so the predicate now rejects an SDK that works.
#   exit 2  the probe could not run, so it has NO answer
#
# This shares no code with build.zig, so it grades build.zig's SDK choice.
#
# The probe MUST emit a real binary. `-fno-emit-bin` skips the link, zig then
# never builds libcxx at all, and the probe reports YES for an SDK that cannot
# build: measured on 2026-09-17, SDK 27.0 passed with `-fno-emit-bin` and failed
# with `-femit-bin`. A probe that answers without running is worse than none.
# --expect-no still runs that compile. It only changes what is printed and the
# exit code. Exit 0 in that mode requires the libcxx failure text in zig's
# captured output, so a skipped compile cannot look like the expected result.
set -u
expect_no=0
if [ "${1:-}" = "--expect-no" ]; then
  expect_no=1
  shift
fi
sdk="${1:?usage: macos-libcxx-probe.sh [--expect-no] <MacOSX SDK root>}"
zig="${ZIG:-zig}"
[ -f "$sdk/usr/include/math.h" ] || { echo "probe: $sdk has no usr/include/math.h" >&2; exit 2; }
tmp="$(mktemp -d)" || exit 2
trap 'rm -rf "$tmp"' EXIT
printf 'pub fn main() void {}\n' > "$tmp/empty.zig"
printf 'include_dir=%s/usr/include\nsys_include_dir=%s/usr/include\ncrt_dir=\nmsvc_lib_dir=\nkernel32_lib_dir=\ngcc_dir=\n' \
  "$sdk" "$sdk" > "$tmp/libc.txt"
"$zig" build-exe "$tmp/empty.zig" -lc++ -lc --libc "$tmp/libc.txt" \
  -femit-bin="$tmp/probe-bin" > "$tmp/out.txt" 2>&1
rc=$?
if grep -q 'sub-compilation of libcxx failed' "$tmp/out.txt"; then
  if [ "$expect_no" -eq 1 ]; then
    echo "probe: libcxx still fails on $sdk (expected)"
    exit 0
  fi
  echo "NO  $sdk"
  grep -m1 "undeclared identifier\|error:" "$tmp/out.txt" >&2
  exit 1
fi
if [ "$rc" -ne 0 ]; then
  echo "probe: zig build-exe failed for a reason that is NOT libcxx (exit $rc)" >&2
  cat "$tmp/out.txt" >&2
  exit 2
fi
# Prove the link ran. No binary means no libcxx build, so there is no answer.
[ -s "$tmp/probe-bin" ] || { echo "probe: no binary emitted, so libcxx never built" >&2; exit 2; }
if [ "$expect_no" -eq 1 ]; then
  echo "probe: predicate rejects $sdk but zig's libcxx now builds it" >&2
  exit 1
fi
echo "YES $sdk"
exit 0
