#!/bin/sh
# Prove the H1 battery bites. tests/h1_battery.zig flips M1–M8 one at a time
# and requires the named fixture to fail (two_in_one_segment, chunk terminator,
# close_honored, keep-after-400, validation_shared, flush_latency,
# head_over_bound, cl_plus). This script runs that suite.
set -eu
cd "$(dirname "$0")/.."
exec ./zb build test-h1-battery
