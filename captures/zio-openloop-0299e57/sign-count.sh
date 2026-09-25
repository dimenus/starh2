#!/bin/sh
# How often does one arm's per-round value exceed another's, per metric?
#
# The open-loop percentiles swing 2-4x from run to run at 320k, so one
# round's ratio says little and the range of ratios is wide even for the A/A
# pair. A count of rounds where arm2 > arm1, next to the same count for the
# A/A pair (WS2 against WS), shows whether a direction is consistent. Under
# no difference, arm2 is above arm1 in about half the rounds; with n rounds,
# the chance of k or more is the binomial tail printed with it.
#
#   captures/zio-openloop-0299e57/sign-count.sh <arm1> <arm2> <rows files...>
set -eu
A1=$1; A2=$2; shift 2
awk -v a1="$A1" -v a2="$A2" '
  function tail(n, k,   p, i, c) { p = 0; for (i = k; i <= n; i++) { c = comb(n, i); p += c / 2 ^ n }; return p }
  function comb(n, k,   r, i) { r = 1; for (i = 1; i <= k; i++) r = r * (n - k + i) / i; return r }
  FNR == 1 { file++ }
  ($3 ~ /^open[0-9]+k-e/ || $3 ~ /^oneshot-lat-e/) && ($2 == a1 || $2 == a2) {
    for (i = 1; i <= NF; i++) {
      if ($i ~ /^p50us=/) v["p50"] = substr($i, 7) + 0
      if ($i ~ /^p99us=/) v["p99"] = substr($i, 7) + 0
      if ($i ~ /^p999us=/) v["p999"] = substr($i, 8) + 0
    }
    for (f in v) { key = file SUBSEP $1 SUBSEP $3 SUBSEP f; val[key, $2] = v[f]; keys[key] = 1; mets[$3 " " f] = 1 }
  }
  END {
    for (key in keys) {
      split(key, p, SUBSEP)
      if (!((key, a1) in val) || !((key, a2) in val)) continue
      m = p[3] " " p[4]; n[m]++
      if (val[key, a2] > val[key, a1]) up[m]++
    }
    for (m in n) printf "%-24s %s>%s in %d of %d rounds (P(>=k | no difference) = %.3f)\n", m, a2, a1, up[m] + 0, n[m], tail(n[m], up[m] + 0)
  }' "$@" | sort
