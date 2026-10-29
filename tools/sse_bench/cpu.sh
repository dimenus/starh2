# Sourced by run.sh / mixed.sh.
#
# cpu_seconds PID prints the user + system CPU seconds the process has used,
# with two decimals.
#
# Linux reads /proc/PID/stat, in clock ticks (10 ms at CLK_TCK=100): procps
# `ps -o time=` prints whole seconds only, so a server that used less than a
# second read as 0.00. The comm field can hold spaces, so the fields are
# counted after its closing ") ": utime and stime are fields 14 and 15 of the
# file, $12 and $13 here. macOS has no /proc, and its `ps -o time=` prints
# hundredths ([HH:]MM:SS.hh).
cpu_seconds() {
  if [ -r "/proc/$1/stat" ]; then
    sed 's/^.*) //' "/proc/$1/stat" | awk -v tck="$(getconf CLK_TCK)" '{ printf "%.2f", ($12 + $13) / tck }'
    return
  fi
  ps -o time= -p "$1" | awk '{
    gsub(/[[:space:]]/, "", $0)
    fraction_parts = split($0, fraction, ".")
    clock_parts = split(fraction[1], clock, ":")
    total = 0
    for (i = 1; i <= clock_parts; i++) total = (total * 60) + clock[i]
    if (fraction_parts > 1) total += ("0." fraction[2])
    printf "%.2f", total
  }'
}
