#!/bin/sh
# Summarise the rows of tools/zio-arm-ab.sh: medians per arm per metric, and
# the burst fail-close rate with its denominator.
#
# A median, not a mean: one wedge or one scheduler hiccup moves a mean and
# says nothing about the arm. Every row stays in the raw file, so a reader
# can see the spread rather than trust the summary.
#
# A round that FAIL-CLOSED is excluded from the latency medians and counted
# separately. It opened fewer streams, so its p50 is the latency of a smaller
# workload; averaging it in makes a broken round read as a fast one.
#
# The burst rate prints as fails/rounds, never as a bare percentage: a rate
# with no denominator is not a result.
#
#   tools/zio-arm-ab-summary.sh < rows.txt
set -eu
awk '
function med(a, n,   i, j, t, tmp) {
  if (n == 0) return -1
  for (i = 1; i <= n; i++) tmp[i] = a[i]
  for (i = 1; i < n; i++) for (j = i + 1; j <= n; j++) if (tmp[j] < tmp[i]) { t = tmp[i]; tmp[i] = tmp[j]; tmp[j] = t }
  return tmp[int((n + 1) / 2)]
}
function tous(s) {
  if (s ~ /µs$/) { sub(/µs$/, "", s); return s + 0 }
  if (s ~ /ms$/) { sub(/ms$/, "", s); return (s + 0) * 1000 }
  if (s ~ /s$/)  { sub(/s$/, "", s);  return (s + 0) * 1000000 }
  return s + 0
}
function note(k) { if (!(k in seenk)) { seenk[k] = 1; rkeys[++nrk] = k } }
{ arm = $2; if (!(arm in seena)) { seena[arm] = 1; arms[++na] = arm } }

$3 ~ /^sse[0-9]+$/ && /events=/ {
  k = $1 SUBSEP arm SUBSEP $3; note(k)
  for (i = 1; i <= NF; i++) {
    if ($i ~ /^failed=/)  { val = $i; sub(/^failed=/, "", val);  fail[k] = val + 0 }
    if ($i ~ /^events=/)  { val = $i; sub(/^events=/, "", val);  ev[k] = val + 0 }
  }
}
$3 ~ /^sse[0-9]+$/ && /sse latency/ {
  k = $1 SUBSEP arm SUBSEP $3; note(k)
  for (i = 1; i <= NF; i++) {
    if ($i ~ /^p50=/) { val = $i; sub(/^p50=/, "", val); p50[k] = tous(val) }
    if ($i ~ /^p99=/) { val = $i; sub(/^p99=/, "", val); p99[k] = tous(val) }
  }
}
$3 ~ /^oneshot-e[0-9]+$/ && /req\/s/ {
  k = $1 SUBSEP arm SUBSEP $3; note(k)
  for (i = 1; i <= NF; i++) if ($(i+1) == "req/s,") { val = $i; sub(/,$/, "", val); rps[k] = val + 0 }
}
$3 ~ /^oneshot-e[0-9]+$/ && /WEDGE-OR-FAIL/ { k = $1 SUBSEP arm SUBSEP $3; note(k); wedged[k] = 1 }

$3 ~ /^cpu[0-9]+$/ {
  k = $1 SUBSEP arm SUBSEP $3; note(k)
  for (i = 1; i <= NF; i++) {
    if ($i ~ /^ticks=/)  { val = $i; sub(/^ticks=/, "", val);  tk[k] = val + 0 }
    if ($i ~ /^tck=/)    { val = $i; sub(/^tck=/, "", val);    tck[k] = val + 0 }
    if ($i ~ /^events=/) { val = $i; sub(/^events=/, "", val); ev[k] = val + 0 }
    if ($i ~ /^failed=/) { val = $i; sub(/^failed=/, "", val); fail[k] = val + 0 }
  }
}
$3 == "burst" {
  burst_n[arm]++
  if ($4 == "FAIL") burst_fail[arm]++
  for (i = 1; i <= NF; i++) {
    if ($i ~ /^overflow=/)     { val = $i; sub(/^overflow=/, "", val);     if (val != "0" && val != "absent") ovmoved[arm]++ }
    if ($i ~ /^stage_failed=/) { val = $i; sub(/^stage_failed=/, "", val); if (val != "0" && val != "absent") stmoved[arm]++ }
  }
}
END {
  if (nrk == 0 && na == 0) { print "no rows parsed - that is a failure, not a pass" > "/dev/stderr"; exit 1 }
  for (i = 1; i <= nrk; i++) {
    split(rkeys[i], p, SUBSEP); a = p[2]; m = p[3]
    g = a SUBSEP m
    if (!(g in seeng)) { seeng[g] = 1; groups[++ng] = g }
    if (m ~ /^oneshot/) {
      if (rkeys[i] in wedged) { gw[g]++ ; continue }
      if (rkeys[i] in rps) { gn[g]++; gv[g, gn[g]] = rps[rkeys[i]] }
      continue
    }
    if (m ~ /^cpu/) {
      if (fail[rkeys[i]] > 0) { gfail[g]++; continue }
      if ((rkeys[i] in tk) && (rkeys[i] in ev) && ev[rkeys[i]] > 0 && tck[rkeys[i]] > 0) {
        gn[g]++; gv[g, gn[g]] = (tk[rkeys[i]] / tck[rkeys[i]]) * 1000000 / ev[rkeys[i]]
      }
      continue
    }
    if (fail[rkeys[i]] > 0) { gfail[g]++; continue }
    if (rkeys[i] in p50) { gn[g]++; gv[g, gn[g]] = p50[rkeys[i]]; g9n[g]++; g9v[g, g9n[g]] = p99[rkeys[i]] }
    if (rkeys[i] in ev)  { gen[g]++; gev[g, gen[g]] = ev[rkeys[i]] }
  }
  printf "%-9s %-16s %12s %5s %-19s %s\n", "arm", "metric", "median", "n", "spread", "excluded"
  for (i = 1; i <= ng; i++) {
    split(groups[i], p, SUBSEP); a = p[1]; m = p[2]
    n = gn[groups[i]] + 0
    if (n > 0) {
      for (j = 1; j <= n; j++) v[j] = gv[groups[i], j]
      lo = v[1]; hi = v[1]; for (j = 1; j <= n; j++) { if (v[j] < lo) lo = v[j]; if (v[j] > hi) hi = v[j] }
      unit = (m ~ /^oneshot/) ? " req/s" : ((m ~ /^cpu/) ? " cpu-us/ev" : " p50us")
      if (m ~ /^cpu/) printf "%-9s %-16s %12.3f %5d %-19s %s\n", a, m unit, med(v, n), n, sprintf("%.3f-%.3f", lo, hi), (gfail[groups[i]] + 0) " fail-closed"
      else printf "%-9s %-16s %12.0f %5d %-19s %s\n", a, m unit, med(v, n), n, sprintf("%.0f-%.0f", lo, hi), \
        (m ~ /^oneshot/ ? (gw[groups[i]] + 0) " wedged" : (gfail[groups[i]] + 0) " fail-closed")
    }
    if (g9n[groups[i]] + 0 > 0) {
      n = g9n[groups[i]]; for (j = 1; j <= n; j++) v[j] = g9v[groups[i], j]
      lo = v[1]; hi = v[1]; for (j = 1; j <= n; j++) { if (v[j] < lo) lo = v[j]; if (v[j] > hi) hi = v[j] }
      printf "%-9s %-16s %12.0f %5d %-19s\n", a, m " p99us", med(v, n), n, sprintf("%.0f-%.0f", lo, hi)
    }
    if (gen[groups[i]] + 0 > 0) {
      n = gen[groups[i]]; for (j = 1; j <= n; j++) v[j] = gev[groups[i], j]
      printf "%-9s %-16s %12.0f %5d\n", a, m " events", med(v, n), n
    }
  }
  print ""
  print "burst fail-close (200 streams opened at once on one TLS connection):"
  printf "%-9s %8s %8s %s\n", "arm", "fails", "rounds", "counters-moved(overflow/stage)"
  for (i = 1; i <= na; i++) {
    a = arms[i]
    if (burst_n[a] + 0 == 0) continue
    printf "%-9s %8d %8d %s\n", a, burst_fail[a] + 0, burst_n[a] + 0, (ovmoved[a] + 0) "/" (stmoved[a] + 0)
  }
}
' "$@"
