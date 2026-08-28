#!/bin/sh
# Prove the H1 battery bites. Each mutation is a runtime hook exercised by
# tests/h1_battery.zig (M1, M8) and by flipping edge.h1.test_channel_mutation
# in those tests. This script re-runs the named battery.
set -eu
cd "$(dirname "$0")/.."
exec ./zb build test-h1-battery
