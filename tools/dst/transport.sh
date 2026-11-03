#!/bin/sh
# The DST transport gate: `./zb build dst-transport`, and part of `ci`.
#
#   tools/dst/transport.sh <control-binary> <mutant-binary>
#
# Both binaries are tests/dst_tls_lifecycle.zig; the mutant's shared
# HTTP/1.1 TLS constructor leaves the ack sink out. The gate passes only if:
#   1. the control exits 0 with its pass line (every case passed, every
#      fault and coverage floor met, inside the time budget);
#   2. the mutant exits 3 with INVARIANT=UnconsumedTlsAck. A mutant that
#      passes is a FAIL: a clean sweep against it means the witness is blind;
#   3. the mutant's printed REPLAY command, run twice, fails again with the
#      same invariant, case, step, trace and trace hash.
# Every run is under a wall-clock limit, so a wedge fails instead of hanging.
set -u

control=$1
mutant=$2
limit=120
tmp=$(mktemp -d "${TMPDIR:-/tmp}/dst-transport.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

fail() {
  echo "dst-transport: FAIL: $*" >&2
  exit 1
}

# Run "$@" under the limit; its stderr goes to file $1's log.
run_logged() {
  log=$1
  shift
  perl -e 'alarm shift; exec @ARGV' "$limit" "$@" 2>"$log"
  code=$?
  cat "$log" >&2
  return $code
}

key_lines() {
  grep -E '^(INVARIANT|CASE|STEP|TRACE|TRACE_HASH)=' "$1"
}

echo "dst-transport: control: $control" >&2
run_logged "$tmp/control.log" "$control"
code=$?
[ "$code" -eq 0 ] || fail "the control exited $code (3: an invariant failed, 4: a floor failed, 142: over the ${limit}s limit)"
grep -q '^DST-TRANSPORT PASS completed=' "$tmp/control.log" || fail "the control exited 0 without its pass line"
grep -q '^BUILD=.* mutant=none ' "$tmp/control.log" || fail "the control binary is not the unmutated build"
grep -q '^BUILD=.* mode=debug ' "$tmp/control.log" || fail "the control is not a Debug build; its oracle needs runtime safety"

echo "dst-transport: mutant: $mutant" >&2
run_logged "$tmp/mutant.log" "$mutant"
code=$?
case $code in
  3) ;;
  0) fail "the mutant passed every case: a clean sweep against the mutant is a FAIL" ;;
  *) fail "the mutant exited $code, not 3 (a named invariant); a crash or a floor is not the expected failure" ;;
esac
grep -q '^BUILD=.* mutant=no-h1-ack-sink ' "$tmp/mutant.log" || fail "the mutant binary is not the no-h1-ack-sink build"
grep -q '^BUILD=.* mode=debug ' "$tmp/mutant.log" || fail "the mutant is not a Debug build"
inv=$(sed -n 's/^INVARIANT=//p' "$tmp/mutant.log" | head -n 1)
[ "$inv" = "UnconsumedTlsAck" ] || fail "the mutant failed with $inv, not UnconsumedTlsAck"
replay=$(sed -n 's/^REPLAY: //p' "$tmp/mutant.log" | head -n 1)
[ -n "$replay" ] || fail "the mutant's failure printed no REPLAY line"
key_lines "$tmp/mutant.log" >"$tmp/mutant.key"

for n in 1 2; do
  echo "dst-transport: replay $n: $replay" >&2
  # shellcheck disable=SC2086 # the replay line is a command and its arguments
  run_logged "$tmp/replay$n.log" $replay
  code=$?
  [ "$code" -eq 3 ] || fail "replay $n exited $code, not 3"
  key_lines "$tmp/replay$n.log" >"$tmp/replay$n.key"
  if ! cmp -s "$tmp/mutant.key" "$tmp/replay$n.key"; then
    diff "$tmp/mutant.key" "$tmp/replay$n.key" >&2
    fail "replay $n did not fail identically to the corpus run"
  fi
done

echo "dst-transport: PASS: control clean; mutant caught as UnconsumedTlsAck at $(sed -n 's/^CASE=//p' "$tmp/mutant.key"), replayed identically twice" >&2
echo "dst-transport: scope: this witness proves the HTTP/1.1 TLS transport cannot regain an unread failure channel. It does not reproduce the HTTPS crash end to end, and it says nothing about HTTP/2, the scheduler, or real threads." >&2
