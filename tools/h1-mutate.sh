#!/bin/sh
# Prove the H1 battery bites. tests/h1_battery.zig flips M1–M8 one at a time
# and requires the named case to fail. M2 also trips h1-smoke hang detection:
# STARH2_H1_MUTATION=m2 omits the last chunk on /h1-once, so curl --max-time
# 3 must fail and the smoke must not stay green.
set -eu
cd "$(dirname "$0")/.."
./zb build test-h1-battery
if STARH2_H1_MUTATION=m2 ./zb build h1-smoke; then
    echo "h1-mutate: M2 left h1-smoke green — hang detector missed"
    exit 1
fi
echo "h1-mutate: M2 tripped h1-smoke (expected)"
