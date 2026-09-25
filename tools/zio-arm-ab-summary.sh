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
# A one-shot row counts only when its `requests:` line shows every request
# succeeded. A row without that line cannot prove it did the work, so it is
# excluded and counted, not read as a rate.
#
# With EXACTLY two arms, a paired section follows: for each metric, each
# round's value per arm and the ratio second/first (arm order is first-seen,
# which is the ARMS order), then the median and range of those per-round
# ratios. Rounds are paired because they ran back to back; the ratio range is
# what an A/A run's range is compared against. CPU pairs are in clock ticks
# per million delivered events, the unit t-1061 used.
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
function pair(rd, metric, arm, v) {
  if (!(metric in seenm)) { seenm[metric] = 1; metrics[++nm] = metric }
  if (!((metric, rd) in seenr)) { seenr[metric, rd] = 1; mr[metric, ++mrn[metric]] = rd }
  if (!(arm in seenpa)) { seenpa[arm] = 1; parms[++npa] = arm }
  pv[metric, rd, arm] = v
}
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
$3 ~ /^oneshot-(lat-)?e[0-9]+$/ && /req\/s/ {
  k = $1 SUBSEP arm SUBSEP $3; note(k)
  for (i = 1; i <= NF; i++) {
    if ($(i+1) == "req/s,") { val = $i; sub(/,$/, "", val); rps[k] = val + 0 }
    if ($(i+1) == "total,") req_total[k] = $i + 0
    if ($(i+1) == "succeeded,") req_ok[k] = $i + 0
    if ($i ~ /^p50us=/) { val = $i; sub(/^p50us=/, "", val); p50[k] = val + 0 }
    if ($i ~ /^p99us=/) { val = $i; sub(/^p99us=/, "", val); p99[k] = val + 0 }
    if ($i ~ /^non200=/) { val = $i; sub(/^non200=/, "", val); non200[k] = val + 0 }
  }
}
$3 ~ /^oneshot-(lat-)?e[0-9]+$/ && /WEDGE-OR-FAIL/ { k = $1 SUBSEP arm SUBSEP $3; note(k); wedged[k] = 1 }

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
    k = rkeys[i]; rd = p[1]
    if (m ~ /^oneshot/) {
      if (k in wedged) { gw[g]++ ; continue }
      if (!(k in req_ok) || req_ok[k] != req_total[k] || req_ok[k] == 0 || non200[k] > 0) { gbad[g]++; continue }
      if (k in rps) { gn[g]++; gv[g, gn[g]] = rps[k]; pair(rd, m " rps", a, rps[k]) }
      if (k in p50) {
        g9n[g]++; g9v[g, g9n[g]] = p99[k]; g5n[g]++; g5v[g, g5n[g]] = p50[k]
        pair(rd, m " p50us", a, p50[k]); pair(rd, m " p99us", a, p99[k])
      }
      continue
    }
    if (m ~ /^cpu/) {
      if (fail[k] > 0) { gfail[g]++; continue }
      if ((k in tk) && (k in ev) && ev[k] > 0 && tck[k] > 0 && tk[k] > 0) {
        gn[g]++; gv[g, gn[g]] = (tk[k] / tck[k]) * 1000000 / ev[k]
        pair(rd, m " ticks/Mev", a, tk[k] * 1000000 / ev[k])
      }
      continue
    }
    if (fail[k] > 0) { gfail[g]++; continue }
    if (k in p50) {
      gn[g]++; gv[g, gn[g]] = p50[k]; g9n[g]++; g9v[g, g9n[g]] = p99[k]
      pair(rd, m " p50us", a, p50[k]); pair(rd, m " p99us", a, p99[k])
    }
    if (k in ev)  { gen[g]++; gev[g, gen[g]] = ev[k] }
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
        (m ~ /^oneshot/ ? (gw[groups[i]] + 0) " wedged " (gbad[groups[i]] + 0) " unproven" : (gfail[groups[i]] + 0) " fail-closed")
    }
    if (g5n[groups[i]] + 0 > 0) {
      n = g5n[groups[i]]; for (j = 1; j <= n; j++) v[j] = g5v[groups[i], j]
      lo = v[1]; hi = v[1]; for (j = 1; j <= n; j++) { if (v[j] < lo) lo = v[j]; if (v[j] > hi) hi = v[j] }
      printf "%-9s %-16s %12.0f %5d %-19s\n", a, m " p50us", med(v, n), n, sprintf("%.0f-%.0f", lo, hi)
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
  if (npa != 2) exit 0
  a1 = parms[1]; a2 = parms[2]
  print ""
  print "paired rounds, ratio = " a2 "/" a1 ":"
  for (i = 1; i <= nm; i++) {
    mt = metrics[i]; n = 0
    print mt
    for (j = 1; j <= mrn[mt]; j++) {
      rd = mr[mt, j]
      if (((mt, rd, a1) in pv) && ((mt, rd, a2) in pv) && pv[mt, rd, a1] > 0) {
        n++; x1[n] = pv[mt, rd, a1]; x2[n] = pv[mt, rd, a2]; rt[n] = x2[n] / x1[n]
        printf "  %-5s %s=%.1f %s=%.1f ratio=%.3f\n", rd, a1, x1[n], a2, x2[n], rt[n]
      } else printf "  %-5s unpaired (one arm row excluded)\n", rd
    }
    if (n == 0) continue
    lo = rt[1]; hi = rt[1]; for (j = 1; j <= n; j++) { if (rt[j] < lo) lo = rt[j]; if (rt[j] > hi) hi = rt[j] }
    printf "  pairs=%d median %s=%.1f %s=%.1f ratio-of-medians=%.3f per-round-ratio median=%.3f range=%.3f-%.3f\n", \
      n, a1, med(x1, n), a2, med(x2, n), med(x2, n) / med(x1, n), med(rt, n), lo, hi
  }
}
' "$@"
