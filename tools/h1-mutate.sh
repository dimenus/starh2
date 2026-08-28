#!/bin/sh
# Prove the H1 battery bites. tests/h1_battery.zig flips M1–M8 one at a time
# and requires the named case to fail. M2 also trips h1-smoke hang detection:
# STARH2_H1_MUTATION=m2 omits the last chunk on /h1-once, so curl --max-time
# 3 must fail with exit 28 (timeout). Any other nonzero (compile, cert, hello)
# is not M2 proof.
set -eu
cd "$(dirname "$0")/.."
./zb build test-h1-battery
set +e
out=$(STARH2_H1_MUTATION=m2 ./zb build h1-smoke 2>&1)
status=$?
set -e
printf '%s\n' "$out"
if [ "$status" -eq 0 ]; then
    echo "h1-mutate: M2 left h1-smoke green — hang detector missed"
    exit 1
fi
case "$out" in
    *"tls /h1-once curl exit 28"*) ;;
    *)
        echo "h1-mutate: M2 failed for a reason other than /h1-once timeout"
        exit 1
        ;;
esac
echo "h1-mutate: M2 tripped h1-smoke curl 28 (expected)"
