#!/bin/sh
# Probe for run-mac.sh's process-group sampler. Run in its own session.
# Arm 1 (own load): a CPU-burning `sort` child of this script must NOT count.
# Arm 2 (outside load): a busy loop in ANOTHER session must count.
OWN_PGID=$(ps -o pgid= -p $$ | tr -d ' ')
other_cpu() {
  ps -Ao pcpu=,pgid=,comm= | awk -v g="$OWN_PGID" '
    $2 == g { next }
    { t += $1; if ($1 > m) { m = $1; who = $3 } }
    END { printf "%.0f %.0f %s\n", t, m, who }'
}
echo "baseline: $(other_cpu)"
# Own load: perl busy loop named by its pipe into sort, ends by itself in 6 s.
perl -e 'my $t = time + 6; my $i = 0; while (time < $t) { $i++; print "$i\n" if $i % 1000 == 0 }' | sort > /dev/null &
sleep 4
echo "own-load-running: $(other_cpu)   own_group_top: $(ps -Ao pcpu=,pgid=,comm= | awk -v g="$OWN_PGID" '$2 == g' | sort -rn | head -1)"
wait
# Outside load: a busy loop in a new session, ends by itself in 6 s.
perl -e 'use POSIX; setsid(); my $t = time + 6; 1 while time < $t' &
sleep 4
echo "outside-load-running: $(other_cpu)"
wait
