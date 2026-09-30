#!/bin/sh
# Per-event io_uring poll counts from a mech747 capture (run.sh mech747).
#
#   captures/zio-placement-ab-0299e57/mech747-summary.sh mech747-rows.txt
#
# Each counted run's loop totals are summed over the executors' loops and
# divided by the events the client received. A run that failed closed is
# left out, as in the CPU rows. The counters print every 1024th poll, so each
# loop's total is short by at most 1023 polls, well under 0.1% here.
set -eu
awk '
function med(a, n,   i, j, t, tmp) {
  for (i = 1; i <= n; i++) tmp[i] = a[i]
  for (i = 1; i < n; i++) for (j = i + 1; j <= n; j++) if (tmp[j] < tmp[i]) { t = tmp[i]; tmp[i] = tmp[j]; tmp[j] = t }
  return tmp[int((n + 1) / 2)]
}
function val(k,   i) { for (i = 1; i <= NF; i++) if (index($i, k "=") == 1) return substr($i, length(k) + 2) + 0; return -1 }
$3 ~ /^cpu/ && $4 != "log" { key = $1 " " $2; ev[key] = val("events"); fl[key] = val("failed"); tk[key] = val("ticks"); order[++no] = key; arm_of[key] = $2 }
$3 ~ /^cpu/ && $4 == "log" && $5 == "iouring-stats" {
  key = $1 " " $2
  polls[key] += val("polls"); zero[key] += val("zero"); ze[key] += val("zero_empty"); cq[key] += val("cqes"); loops[key]++
}
END {
  printf "%-6s %-5s %9s %7s %7s %7s %7s %7s %6s\n", "round", "arm", "events", "ticks", "poll/ev", "zero/ev", "zemp/ev", "cqe/ev", "loops"
  for (i = 1; i <= no; i++) {
    k = order[i]
    if (fl[k] != 0 || ev[k] <= 0 || loops[k] == 0) { printf "%-12s excluded (failed=%d events=%d loops=%d)\n", k, fl[k], ev[k], loops[k]; continue }
    a = arm_of[k]; n[a]++
    p[a, n[a]] = polls[k] / ev[k]; z[a, n[a]] = zero[k] / ev[k]; e[a, n[a]] = ze[k] / ev[k]; c[a, n[a]] = cq[k] / ev[k]; t[a, n[a]] = tk[k] * 1e6 / ev[k]
    split(k, kk, " ")
    printf "%-6s %-5s %9d %7d %7.3f %7.3f %7.3f %7.3f %6d\n", kk[1], a, ev[k], tk[k], p[a, n[a]], z[a, n[a]], e[a, n[a]], c[a, n[a]], loops[k]
    if (!(a in seen)) { seen[a] = 1; arms[++na] = a }
  }
  print ""
  printf "%-5s %3s %10s %8s %8s %8s %8s\n", "arm", "n", "ticks/Mev", "poll/ev", "zero/ev", "zemp/ev", "cqe/ev"
  for (i = 1; i <= na; i++) {
    a = arms[i]
    for (j = 1; j <= n[a]; j++) { P[j] = p[a, j]; Z[j] = z[a, j]; E[j] = e[a, j]; C[j] = c[a, j]; T[j] = t[a, j] }
    printf "%-5s %3d %10.1f %8.3f %8.3f %8.3f %8.3f  (medians)\n", a, n[a], med(T, n[a]), med(P, n[a]), med(Z, n[a]), med(E, n[a]), med(C, n[a])
  }
}' "$@"
