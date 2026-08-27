#!/bin/sh
# zio pin-qualification gate: qualify a CANDIDATE zio commit before starh2
# adopts it. Exists because five distinct bugs in zio's select/wake/claim
# machinery each cost a multi-day hunt whose only witness was a number no
# gate read (t-1002, t-1022, the #702-era conservation bugs). This runs the
# accumulated instruments against a candidate in ~30 minutes and prints one
# verdict.
#
#   tools/zio-pin-gate/gate.sh <zio-clone-path> <candidate-sha> [--local-only]
#
# Phases (each prints what it ran; running nothing fails):
#   1 fmt             zig fmt --check over the candidate tree
#   2 zio-suite       zig build test in a worktree at the candidate
#   3 examples        zig build examples for native AND x86_64-windows-gnu;
#                     zero artifacts fails (the F8 class: a POSIX-only
#                     example registered unconditionally breaks the Windows
#                     examples build, and no suite run sees it)
#   4 misuse          the two-driver misuse death test: the ONLY pass is an
#                     abort at the point of misuse (rc 134); rc 3 means two
#                     drivers ran undetected (the F10 class), rc 0 means the
#                     test exercised nothing
#   5 conservation    select-cancel + select-clobber repros, clean runs
#   6 cq-repro        the spurious-select-win reproducer:
#                       exit 134/1 -> FAIL (assert crash / error path)
#                       exit 2     -> WARN (race present, tolerated path;
#                                    acceptable only with the fork's
#                                    tolerance + starh2's re-poll in place)
#                       exit 0     -> clean (the bar for an upstream drop)
#   7 h2spec          full http2 suite twice on Darwin h2c, ReleaseSafe;
#                     5.1/12 must pass 2/2 (the #711 witness)
#   8 collapse-probe  15 rounds of 200-stream TLS SSE on the io_uring host;
#                     zero silent collapses (delivered-count gate, t-1028)
#
# --local-only skips phase 8 and the verdict is loudly INCOMPLETE, never
# PASS: a missing io_uring host is a broken gate environment, not a pass.
#
# ZIO_GATE_PHASES=fmt,examples,misuse runs only the named phases, for arming
# a phase against a known-bad candidate. A filtered run is loudly
# INCOMPLETE, never PASS: it validates the instrument, not the candidate.
set -u
ZIO=${1:?zio clone path}
SHA=${2:?candidate sha}
LOCAL_ONLY=${3:-}
REPO=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
HOST=${HOST:-nachos}
OUT=$(mktemp -d /tmp/zio-pin-gate.XXXXXX)
echo "== zio-pin-gate candidate=$SHA out=$OUT"
fail=0; warn=0
PHASES=${ZIO_GATE_PHASES:-all}
runs_phase() {
  [ "$PHASES" = "all" ] && return 0
  case ",$PHASES," in *",$1,"*) return 0 ;; *) return 1 ;; esac
}

WT="$OUT/zio-wt"
git -C "$ZIO" worktree add --detach "$WT" "$SHA" > /dev/null 2>&1 || { echo "FAIL: cannot check out $SHA in $ZIO"; exit 1; }
cleanup() { git -C "$ZIO" worktree remove --force "$WT" > /dev/null 2>&1; }
trap cleanup EXIT

if runs_phase fmt; then
echo "== phase 1: zig fmt --check"
( cd "$WT" && zig fmt --check . ) > "$OUT/fmt.log" 2>&1
if [ $? -ne 0 ]; then
  echo "  FAIL: unformatted files:"; sed 's/^/    /' "$OUT/fmt.log"; fail=1
else
  echo "  fmt clean"
fi
fi

if runs_phase suite; then
echo "== phase 2: zio suite"
( cd "$WT" && zig build test ) > "$OUT/zio-suite.log" 2>&1
p1=$(command grep -E '[0-9]+ of [0-9]+ tests passed' "$OUT/zio-suite.log" | tail -1)
echo "  ${p1:-NO RESULT LINE}"
case "$p1" in
  *"tests passed"*) case "$p1" in *failed*) fail=1;; esac ;;
  *) echo "  FAIL: suite produced no pass line"; fail=1 ;;
esac
fi

if runs_phase examples; then
echo "== phase 3: examples cross-target build"
for t in native x86_64-windows-gnu; do
  rm -rf "$OUT/ex-$t"
  ( cd "$WT" && zig build examples -Dtarget="$t" --prefix "$OUT/ex-$t" ) > "$OUT/examples-$t.log" 2>&1
  rc=$?
  n=$(ls "$OUT/ex-$t/bin" 2>/dev/null | wc -l | tr -d ' ')
  if [ "$rc" -ne 0 ]; then
    echo "  $t: FAIL (build error; the known class is a POSIX-only example registered unconditionally - see examples-$t.log)"; fail=1
  elif [ "$n" -eq 0 ]; then
    echo "  $t: FAIL (zero artifacts - a step that builds nothing must not pass)"; fail=1
  else
    echo "  $t: $n artifacts"
  fi
done
fi

# A repro package copy pointed at the worktree via the relative-symlink
# trick (build.zig.zon rejects absolute path deps).
stage_repro() { # <src-dir> <name>
  cp -R "$REPO/$1" "$OUT/$2"
  mkdir -p "$OUT/$2/deps"
  ln -sfn "$WT" "$OUT/$2/deps/zio"
  python3 - "$OUT/$2/build.zig.zon" <<'PY'
import re, sys
p = sys.argv[1]
s = open(p).read()
s2 = re.sub(r'\.zio = \.\{[^}]*\},', '.zio = .{\n            .path = "deps/zio",\n        },', s, count=1, flags=re.S)
assert s2 != s, p
open(p, 'w').write(s2)
PY
}

if runs_phase misuse; then
echo "== phase 4: two-driver misuse death test"
stage_repro tools/cq-misuse-repro misuse-repro
( cd "$OUT/misuse-repro" && zig build ) > "$OUT/misuse-build.log" 2>&1 || { echo "  FAIL: build"; fail=1; }
if [ -x "$OUT/misuse-repro/zig-out/bin/cq-misuse-repro" ]; then
  timeout 60 "$OUT/misuse-repro/zig-out/bin/cq-misuse-repro" > "$OUT/misuse-run.log" 2>&1
  rc=$?
  # The ONLY pass is the deliberate panic AT the point of misuse. An abort
  # without that message is a crash far from the cause (the pre-claims
  # protocol dies in a getResult assert), which an exit-code-only check
  # cannot tell apart - measured at 4f831ea, which aborts on a Debug
  # assert and would run silently corrupted in ReleaseFast.
  if [ "$rc" -eq 134 ] && command grep -q 'must drive the queue\|driven by two tasks' "$OUT/misuse-run.log"; then
    echo "  misuse: DETECTED (panic at the point of misuse - the single-driver contract holds)"
  else
    case "$rc" in
      134) echo "  misuse: FAIL (abort WITHOUT the misuse panic - a crash far from the cause, not detection)"; fail=1 ;;
      3)   echo "  misuse: FAIL (two drivers ran the full window undetected)"; fail=1 ;;
      0)   echo "  misuse: FAIL (exited clean - the death test exercised nothing)"; fail=1 ;;
      *)   echo "  misuse: FAIL (rc=$rc - the example itself broke; see misuse-run.log)"; fail=1 ;;
    esac
  fi
fi
fi

if runs_phase conservation; then
echo "== phase 5: conservation repros"
stage_repro tools/select-cancel-repro cancel-repro
( cd "$OUT/cancel-repro" && zig build ) > "$OUT/cancel-build.log" 2>&1 || { echo "  FAIL: build"; fail=1; }
if [ -x "$OUT/cancel-repro/zig-out/bin/select-cancel-repro" ]; then
  "$OUT/cancel-repro/zig-out/bin/select-cancel-repro" > "$OUT/cancel-run.log" 2>&1 && echo "  select-cancel: clean" || { echo "  select-cancel: FAIL (rc=$?)"; fail=1; }
  "$OUT/cancel-repro/zig-out/bin/select-clobber-repro" > "$OUT/clobber-run.log" 2>&1 && echo "  select-clobber: clean" || { echo "  select-clobber: FAIL (rc=$?)"; fail=1; }
fi

fi

if runs_phase cq-repro; then
echo "== phase 6: cq spurious-win repro (3 runs, 8 workers)"
stage_repro tools/cq-spurious-repro cq-repro
( cd "$OUT/cq-repro" && zig build ) > "$OUT/cq-build.log" 2>&1 || { echo "  FAIL: build"; fail=1; }
if [ -x "$OUT/cq-repro/zig-out/bin/cq-spurious-repro" ]; then
  worst=0
  for i in 1 2 3; do
    timeout 90 "$OUT/cq-repro/zig-out/bin/cq-spurious-repro" 8 20000 64 > "$OUT/cq-run-$i.log" 2>&1
    rc=$?
    echo "  run$i rc=$rc"
    [ "$rc" -gt "$worst" ] && worst=$rc
  done
  case "$worst" in
    0) echo "  cq-repro: CLEAN (protocol race not observed - upstream-drop bar met)" ;;
    2) echo "  cq-repro: WARN (race present, tolerated path)"; warn=1 ;;
    *) echo "  cq-repro: FAIL (rc=$worst - assert crash or error)"; fail=1 ;;
  esac
fi

fi

if runs_phase h2spec; then
echo "== phase 7: h2spec x2 (Darwin h2c, ReleaseSafe)"
SWT="$OUT/starh2-wt"
git -C "$REPO" worktree add --detach "$SWT" HEAD > /dev/null 2>&1
cleanup2() { git -C "$REPO" worktree remove --force "$SWT" > /dev/null 2>&1; cleanup; }
trap cleanup2 EXIT
mkdir -p "$SWT/deps"; ln -sfn "$WT" "$SWT/deps/zio"
python3 - "$SWT/build.zig.zon" <<'PY'
import re, sys
p = sys.argv[1]
s = open(p).read()
s2 = re.sub(r'\.zio = \.\{[^}]*\},', '.zio = .{\n            .path = "deps/zio",\n        },', s, count=1, flags=re.S)
assert s2 != s, p
open(p, 'w').write(s2)
PY
( cd "$SWT" && ./zb build starh2-conformance-server -Doptimize=ReleaseSafe \
    -Dboringssl-source-path="$HOME/Source/oss/http2-zig-hendrik/boringssl" \
    --prefix "$OUT/conf" ) > "$OUT/conf-build.log" 2>&1 || { echo "  FAIL: conformance build (candidate may lack APIs the starh2 tree requires, e.g. isDrained - see conf-build.log)"; fail=1; }
if [ -x "$OUT/conf/bin/starh2-conformance-server" ]; then
  pass512=0
  for i in 1 2; do
    "$OUT/conf/bin/starh2-conformance-server" --mode h2c --bind 127.0.0.1:0 > "$OUT/h2spec-$i-ready.txt" 2> /dev/null &
    CSRV=$!
    sleep 1
    CPORT=$(sed -n 's/.*"port":\([0-9]*\).*/\1/p' "$OUT/h2spec-$i-ready.txt")
    GODEBUG=tls13=1 "$REPO/tools/h2spec/h2spec" http2 -h 127.0.0.1 -p "$CPORT" -S -o 10 > "$OUT/h2spec-$i.txt" 2>&1
    kill $CSRV 2>/dev/null; wait $CSRV 2>/dev/null
    tail -1 "$OUT/h2spec-$i.txt" | sed 's/^/  /'
    command grep -q '✔ 12: closed: Sends a HEADERS frame' "$OUT/h2spec-$i.txt" && pass512=$((pass512+1))
  done
  echo "  5.1/12 passed $pass512/2"
  [ "$pass512" -eq 2 ] || { echo "  FAIL: 5.1/12 (the #711 witness)"; fail=1; }
fi

fi

if [ "$PHASES" != "all" ]; then
  echo "== VERDICT: INCOMPLETE (phases filtered to '$PHASES'; instrument check only, NOT a qualification)$([ $fail -ne 0 ] && echo ' - WITH FAILURES')"
  [ "$fail" -eq 0 ] || exit 1
  exit 3
fi

if [ "$LOCAL_ONLY" = "--local-only" ]; then
  echo "== phase 8: SKIPPED (--local-only)"
  echo "== VERDICT: INCOMPLETE (collapse probe did not run; this is NOT a pass)"
  exit 3
fi

echo "== phase 8: collapse probe (15 rounds on $HOST)"
( cd "$SWT" && ./zb build starh2-bench-server -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl \
    -Dboringssl-source-path="$HOME/Source/oss/http2-zig-hendrik/boringssl" \
    --prefix "$OUT/bench" ) > "$OUT/bench-build.log" 2>&1 || { echo "  FAIL: bench build (see bench-build.log)"; fail=1; }
( cd "$REPO/tools/sse_bench" && GOOS=linux GOARCH=amd64 go build -o "$OUT/client" ./client.go ) || { echo "  FAIL: client build"; fail=1; }
if [ -x "$OUT/bench/bin/starh2-bench-server" ] && [ -x "$OUT/client" ]; then
  SOCK=$(ls /private/tmp/com.apple.launchd.*/Listeners 2>/dev/null | head -1)
  export SSH_AUTH_SOCK=${SSH_AUTH_SOCK:-$SOCK}
  RD=/tmp/zio-pin-gate
  if ssh "$HOST" "mkdir -p $RD"; then
    scp -q "$OUT/bench/bin/starh2-bench-server" "$HOST:$RD/server"
    scp -q "$OUT/client" "$REPO/testdata/cert.pem" "$REPO/testdata/key.pem" "$REPO/tools/zio-pin-gate/collapse-probe.sh" "$HOST:$RD/"
    ssh "$HOST" "chmod +x $RD/server $RD/client; sh $RD/collapse-probe.sh $RD/server $RD/client $RD/cert.pem $RD/key.pem 15" > "$OUT/probe.log" 2>&1
    prc=$?
    sed 's/^/  /' "$OUT/probe.log"
    [ "$prc" -eq 0 ] || { echo "  FAIL: collapse probe rc=$prc"; fail=1; }
  else
    echo "  FAIL: $HOST unreachable (a missing io_uring host fails the gate; use --local-only for a loud INCOMPLETE)"
    fail=1
  fi
fi

echo "== VERDICT: $([ $fail -ne 0 ] && echo FAIL || { [ $warn -ne 0 ] && echo 'PASS-WITH-WARN (race tolerated, not fixed; not the upstream-drop bar)' || echo PASS-CLEAN; })"
[ "$fail" -eq 0 ] || exit 1
exit 0
