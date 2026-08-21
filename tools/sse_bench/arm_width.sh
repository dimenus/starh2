# Sourced by run.sh / mixed.sh: one scheduler width for every arm.
#
# starh2 runs with STARH2_EXECUTORS (a number, or `auto` = physical cores).
# The opponents would otherwise take their runtime defaults, which is every
# logical CPU: on a 12-core/24-thread box that is 2 (or 12) executors against
# 24 threads, and the comparison is then about width, not about the stack.
#
# OPPONENT_WIDTH=match (default) pins Go (GOMAXPROCS), Kestrel
# (DOTNET_PROCESSOR_COUNT) and hyper (TOKIO_WORKER_THREADS) to the executor
# count that starh2 reports in its ready line. OPPONENT_WIDTH=N pins exactly
# N. OPPONENT_WIDTH=default leaves the runtime defaults (the pre-normalized
# rows). Every arm prints the width it actually runs with in its ready line,
# and check_widths refuses to measure when they differ under match/N.
OPPONENT_WIDTH=${OPPONENT_WIDTH:-match}

# wait_ready LOG: print the ready line of a server, or fail after 15s.
wait_ready() {
  i=0
  while [ "$i" -lt 150 ]; do
    line=$(grep -m1 '"ready":true' "$1" 2>/dev/null)
    if [ -n "$line" ]; then
      echo "$line"
      return 0
    fi
    sleep 0.1
    i=$((i + 1))
  done
  echo "no ready line in $1 after 15s" >&2
  return 1
}

# json_int LINE KEY: the integer value of KEY in a one-line JSON object.
json_int() {
  echo "$1" | sed -n "s/.*\"$2\":\([0-9][0-9]*\).*/\1/p"
}

# starh2_width STARH2_LOG: print the executor count from the starh2 ready
# line. Capture it into STARH2_WIDTH in the caller (a function cannot set
# a caller variable from inside $(...)).
starh2_width() {
  ready=$(wait_ready "$1") || return 1
  w=$(json_int "$ready" executors)
  [ -n "$w" ] || { echo "starh2 ready line has no executors: $ready" >&2; return 1; }
  echo "$w"
}

# opponent_width STARH2_WIDTH: the width to pin the opponents to, or `default`.
opponent_width() {
  case $OPPONENT_WIDTH in
    match) echo "$1" ;;
    default) echo default ;;
    *) echo "$OPPONENT_WIDTH" ;;
  esac
}

# width_env W: the env assignments that pin the three opponents to W.
width_env() {
  if [ "$1" = default ]; then
    echo "env"
  else
    echo "env GOMAXPROCS=$1 DOTNET_PROCESSOR_COUNT=$1 TOKIO_WORKER_THREADS=$1"
  fi
}

# check_widths W GO_LOG KESTREL_LOG HYPER_LOG: read each arm's width and
# print the line; exit 1 when a pinned arm does not run at W.
check_widths() {
  want=$1
  go_w=$(json_int "$(wait_ready "$2")" width)
  kestrel_w=$(json_int "$(wait_ready "$3")" width)
  hyper_w=$(json_int "$(wait_ready "$4")" width)
  echo "== width: starh2=$STARH2_WIDTH  go=$go_w  kestrel=$kestrel_w  hyper=$hyper_w  (OPPONENT_WIDTH=$OPPONENT_WIDTH)"
  if [ "$want" != default ]; then
    for pair in "go=$go_w" "kestrel=$kestrel_w" "hyper=$hyper_w"; do
      if [ "${pair#*=}" != "$want" ]; then
        echo "arm $pair did not pin to width $want — refusing to report" >&2
        return 1
      fi
    done
  fi
  return 0
}
