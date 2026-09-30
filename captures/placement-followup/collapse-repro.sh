#!/bin/sh
# Try to reproduce the PL total collapse (t-2655) on nachos and dump every
# thread's stack while it spins. yama ptrace_scope is 1, so gdb cannot attach
# to a running server; the server runs UNDER gdb instead, and SIGINT stops it
# for `thread apply all bt`. A normal run ends in ~12 s; the client is still
# waiting at 20 s only when streams have stopped delivering.
#
#   ssh ryan@nachos.trex-elevator.ts.net 'bash -s' < captures/placement-followup/collapse-repro.sh
set -u
D=/tmp/starh2-placement-followup; cd $D
for a in $(seq 1 ${ATTEMPTS:-12}); do
  rm -f gdb.out
  gdb -batch -ex "handle SIGPIPE nostop noprint pass" -ex "handle SIGSEGV nostop noprint pass" -ex run -ex "thread apply all bt 25" \
    --args ./PL-server --mode tls --port 0 --sse-interval-ms 1 --cert cert.pem --key key.pem --spawn-placement local > gdb.out 2>&1 &
  G=$!
  PORT=
  for i in $(seq 1 100); do PORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' gdb.out | head -1); [ -n "$PORT" ] && break; sleep 0.1; done
  SP=$(pgrep -f "^$D/PL-server|^./PL-server" -n)
  E=$(grep '"ready"' gdb.out | sed -n 's/.*"executors":\([0-9]*\).*/\1/p')
  # No ready line means the server never started; a probe that goes on
  # anyway reports "normal" for a run that did not happen.
  [ -n "$E" ] || { echo "attempt $a: NO READY LINE; stopping"; tail -5 gdb.out; kill $G 2>/dev/null; exit 1; }
  ./client-linux-v2 -url https://127.0.0.1:$PORT/sse -streams $((E / 2 * 250)) -conns $((E / 2)) -light-conns $E -light-streams 10 \
    -churn-workers 4 -churn-url https://127.0.0.1:$PORT/ -stagger-ms 50 -interval-ms 1 -seconds 10 -warmup 1 -label a$a > client.out 2>&1 &
  C=$!
  sleep 20
  if kill -0 $C 2>/dev/null; then
    echo "attempt $a: client still waiting at 20 s: COLLAPSE candidate; threads=$(for t in /proc/$SP/task/*; do sed 's/^.*) //' $t/stat | awk '{print $12+$13}'; done | sort -rn | head -14 | tr '\n' ',')"
    kill -INT $SP; sleep 8
    kill $C 2>/dev/null; wait $C 2>/dev/null
    grep -E "streams=|sse fair" client.out
    cp gdb.out collapse-gdb-$a.txt
    echo "stacks saved: $D/collapse-gdb-$a.txt ($(grep -c '^Thread ' gdb.out) threads)"
    kill $G 2>/dev/null; kill -9 $SP 2>/dev/null; wait $G 2>/dev/null
    break
  fi
  wait $C 2>/dev/null
  line=$(grep -E 'streams=' client.out | sed 's/.*opened=/opened=/')
  [ -n "$line" ] || { echo "attempt $a: client printed no result; stopping"; head -3 client.out; exit 1; }
  echo "attempt $a: normal: $line"
  kill -INT $SP 2>/dev/null; sleep 1; kill $G 2>/dev/null; kill -9 $SP 2>/dev/null; wait $G 2>/dev/null
done
