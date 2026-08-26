#!/usr/bin/env bash
# Run every cross-vendor arm against every corpus commit, in sequence.
#
#   run-all-cursor.sh [arm=model ...]
#
# Sequential on purpose: two cursor-agent runs on one account contend, and the
# wall time of a run is one of the three validity numbers this harness reads.
# A run that is already recorded is skipped, so a killed sweep resumes.
# Prints coverage at the end and exits non-zero if it ran and scored nothing.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
COMMITS=(f1681a4d e04e6e93 930921f4 bd7b9ed6 646a0b4c ea050293 16df720e 0c9271ce)
ARMS=("$@")
[ ${#ARMS[@]} -gt 0 ] || ARMS=(codex-high=gpt-5.3-codex-high grok-high=cursor-grok-4.6-high)

done_n=0; skip_n=0; fail_n=0
for spec in "${ARMS[@]}"; do
  arm="${spec%%=*}"; model="${spec#*=}"
  for c in "${COMMITS[@]}"; do
    if [ -s "$HERE/runs/$arm/$c/report.json" ]; then
      echo "skip $arm/$c (already recorded)"; skip_n=$((skip_n+1)); continue
    fi
    echo "run  $arm/$c ..."
    if "$HERE/run-cursor.sh" "$arm" "$model" "$c"; then
      done_n=$((done_n+1))
    else
      echo "FAILED $arm/$c" >&2; fail_n=$((fail_n+1))
    fi
  done
done
echo "sweep: ran=$done_n skipped=$skip_n failed=$fail_n"
[ $((done_n + skip_n)) -gt 0 ] || { echo "FATAL: the sweep covered nothing" >&2; exit 4; }
