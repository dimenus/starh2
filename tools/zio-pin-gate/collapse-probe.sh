#!/bin/sh
# Silent-collapse gate phase: N rounds of 200 TLS SSE streams at 1 ms
# interval against a bench server; a round that delivers under 95% of the
# ideal event count with zero client-visible failures is a SILENT COLLAPSE
# and fails the gate. Visible fail-closed rounds (failed>0) are counted but
# do not fail this phase (t-984 tracks them; they predate every pin).
#
# Runs ON the io_uring host. Args: <server-bin> <client-bin> <cert> <key> <rounds>
# Prints one verdict line per round and a final "PROBE-SUMMARY" line;
# exits 1 on any silent collapse, 2 if fewer rounds ran than asked
# (scope must be observable - a probe that ran nothing must not pass).
set -u
SRV_BIN=${1:?server bin}
CLIENT=${2:?client bin}
CERT=${3:?cert}
KEY=${4:?key}
ROUNDS=${5:-15}
D=$(mktemp -d /tmp/zio-pin-gate.XXXXXX)
"$SRV_BIN" --mode tls --port 0 --executors 2 --sse-interval-ms 1 \
  --cert "$CERT" --key "$KEY" > "$D/srv.log" 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null' EXIT
i=0; PORT=
while [ $i -lt 200 ]; do
  PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' "$D/srv.log" | head -1)
  [ -n "$PORT" ] && break
  i=$((i+1)); sleep 0.05
done
[ -n "$PORT" ] || { echo "PROBE-SUMMARY rounds=0 silent=1 (no ready line)"; exit 2; }

silent=0; visible=0; ran=0
r=1
while [ "$r" -le "$ROUNDS" ]; do
  if ! kill -0 $SRV 2>/dev/null; then
    echo "round $r: SERVER-DEAD"
    silent=$((silent+1))
    break
  fi
  out=$("$CLIENT" -url "https://127.0.0.1:$PORT/sse" -streams 200 -seconds 10 -warmup 1 -label gate 2>&1)
  ev=$(echo "$out" | sed -n 's/.*events=\([0-9]*\).*/\1/p' | head -1)
  fl=$(echo "$out" | sed -n 's/.*failed=\([0-9]*\).*/\1/p' | head -1)
  ran=$((ran+1))
  verdict=OK
  if [ "${fl:-0}" -gt 0 ]; then verdict=FAILCLOSED; visible=$((visible+1))
  elif [ "${ev:-0}" -lt 1900000 ]; then verdict=SILENT-COLLAPSE; silent=$((silent+1)); fi
  echo "round $r: $verdict events=${ev:-?} failed=${fl:-?}"
  r=$((r+1))
done
echo "PROBE-SUMMARY rounds=$ran silent=$silent visible_failclosed=$visible"
[ "$ran" -ge "$ROUNDS" ] || exit 2
[ "$silent" -eq 0 ] || exit 1
exit 0
