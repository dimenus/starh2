## nachos (Linux, io_uring)

| shape / metric | WS median | A/A WS2/WS range | WSH | PA | PL | PLB |
|---|---:|---|---:|---:|---:|---:|
| SSE 500 @2 exec: outcome | 0/24 knee | WS2 0/24 knee | 0/24 knee | 12/24 knee | 3/24 knee | 0/24 knee |
| SSE 500 @2: p50 | 17.0 | 0.81-1.25 | 1.00 | 26.31* | 0.75* | 0.74* |
| SSE 500 @2: p99 | 60.0 | 0.33-2.60 | 0.96 | 14.67* | 0.70 | 0.58 |
| CPU/event, 200 streams, 1 conn | 4.2 | 0.91-1.07 | 0.98 | 0.83* | 0.58* | 0.60* |
| CPU/event, 200 streams, 10 conns | 4.3 | 0.92-1.06 | 0.97 | 0.90* | 0.72* | 0.71* |
| one-shot req/s @2 (higher better) | 812977.5 | 0.87-1.12 | 1.01 | 0.96 | 0.97 | 0.98 |
| one-shot req/s @8 (higher better) | 2420569.2 | 0.92-1.05 | 1.00 | 0.93 | 0.92 | 0.94 |
| closed-loop p50 @2 | 477.5 | 0.64-1.43 | 0.96 | 1.32 | 1.28 | 1.29 |
| closed-loop p99 @2 | 971.0 | 0.72-1.40 | 0.99 | 0.85 | 0.83 | 0.81 |
| closed-loop p99 @8 | 390.0 | 0.91-1.13 | 1.01 | 0.91* | 0.90* | 0.92 |
| open loop 320k @2: p50 | 93.0 | 0.29-1.35 | 0.86 | 1.78* | 1.72* | 1.41* |
| open loop 320k @2: p99 | 306.5 | 0.43-1.41 | 0.96 | 1.13 | 1.11 | 1.17 |
| open loop 590k @2: p50 | 229.0 | 0.52-1.66 | 1.09 | 1.26 | 1.51 | 1.29 |
| open loop 590k @2: p99 | 663.0 | 0.71-1.54 | 1.03 | 0.88 | 1.01 | 0.89 |

nachos mix, churn paced 20 ms (second session; mix: heavy = executors/2, mixm: /4; eprod = 12 executors)

| shape / metric | WS median | A/A WS2/WS range | WSH | PA | PL | PLB |
|---|---:|---|---:|---:|---:|---:|
| mix-e8 outcome | 0/12 p50>200us, 1 partial | WS2 0/12 p50>200us | 0/12 p50>200us, 1 partial | 8/12 p50>200us, 4 partial | 0/12 p50>200us, 1 partial | 0/12 p50>200us |
| mix-e8 worst heavy conn p99 | 1128.5 | 0.89-1.14 | 1.00 | 2.53* | 0.17* | 0.09* |
| mix-e8 worst light conn p99 | 121.5 | 0.87-1.46 | 0.95 | 3.87* | 0.64* | 0.55* |
| mix-e8 CPU/event | 6.6 | 0.99-1.06 | 1.00 | 0.74* | 0.45* | 0.49* |
| mix-e8 stopped streams (sum) | 8 | WS2 4 | 3 | 63 | 1 | 0 |
| mix-eprod outcome | 8/12 p50>200us, 4 partial | WS2 8/12 p50>200us, 4 partial | 6/12 p50>200us, 6 partial | 2/12 p50>200us, 10 partial | 3/12 p50>200us, 1 partial | 0/12 p50>200us |
| mix-eprod worst heavy conn p99 | 1935.0 | 0.92-1.15 | 1.00 | 1.71* | 1.54* | 0.31* |
| mix-eprod worst light conn p99 | 357.5 | 0.94-1.19 | 1.00 | 1.56* | 1.02 | 0.49* |
| mix-eprod CPU/event | 6.9 | 0.99-1.01 | 1.00 | 0.79* | 0.45* | 0.55* |
| mix-eprod stopped streams (sum) | 44 | WS2 55 | 56 | 156 | 3 | 3 |
| mixm-e8 outcome | 0/12 p50>200us | WS2 0/12 p50>200us | 0/12 p50>200us | 0/12 p50>200us | 0/12 p50>200us | 0/12 p50>200us |
| mixm-e8 worst heavy conn p99 | 145.0 | 0.18-4.29 | 0.91 | 9.11* | 0.43 | 0.35 |
| mixm-e8 worst light conn p99 | 56.5 | 0.70-1.11 | 1.00 | 3.86* | 0.81 | 0.64* |
| mixm-e8 CPU/event | 7.2 | 0.86-1.19 | 1.02 | 0.84* | 0.40* | 0.40* |
| mixm-e8 stopped streams (sum) | 0 | WS2 0 | 2 | 1 | 0 | 0 |
| mixm-eprod outcome | 1/12 p50>200us, 2 partial | WS2 0/12 p50>200us | 0/12 p50>200us, 3 partial | 11/12 p50>200us, 1 partial | 3/12 p50>200us, 1 partial | 0/12 p50>200us |
| mixm-eprod worst heavy conn p99 | 1301.5 | 0.93-1.05 | 0.99 | 1.77* | 0.11* | 0.06* |
| mixm-eprod worst light conn p99 | 108.5 | 0.75-1.49 | 0.97 | 2.98* | 0.67* | 0.53* |
| mixm-eprod CPU/event | 8.0 | 0.98-1.03 | 1.00 | 0.84* | 0.38* | 0.40* |
| mixm-eprod stopped streams (sum) | 4 | WS2 6 | 7 | 29 | 1 | 0 |

nachos mix, churn unpaced (first session)

| shape / metric | WS median | A/A WS2/WS range | WSH | PA | PL | PLB |
|---|---:|---|---:|---:|---:|---:|
| mix-e8 outcome | 6/12 p50>200us, 6 partial | WS2 9/12 p50>200us, 3 partial | 7/12 p50>200us, 5 partial | 2/12 p50>200us, 10 partial | 12/12 p50>200us | 0/12 p50>200us |
| mix-e8 worst heavy conn p99 | 1887.5 | 0.97-1.01 | 0.99 | 1.86* | 1.85* | 0.42* |
| mix-e8 worst light conn p99 | 365.5 | 0.89-1.13 | 0.99 | 1.20* | 1.05 | 0.54* |
| mix-e8 CPU/event | 7.6 | 1.00-1.01 | 1.00 | 0.79* | 0.52* | 0.75* |
| mix-e8 stopped streams (sum) | not measured | - | - | - | - | - |
| mix-eprod outcome | 3/12 p50>200us, 9 partial | WS2 1/12 p50>200us, 11 partial | 2/12 p50>200us, 10 partial | 1/12 p50>200us, 11 partial | 10/12 p50>200us, 1 partial, 1 failclosed | 10/12 p50>200us, 1 partial |
| mix-eprod worst heavy conn p99 | 2285.5 | 0.90-1.04 | 1.00 | 1.84* | 1.75* | 0.61* |
| mix-eprod worst light conn p99 | 457.5 | 0.89-1.03 | 0.98 | 1.27* | 1.99* | 1.21* |
| mix-eprod CPU/event | 7.6 | 1.00-1.00 | 1.00 | 0.80* | 0.55* | 0.76* |
| mix-eprod stopped streams (sum) | not measured | - | - | - | - | - |

## Mac (macOS, kqueue)

| shape / metric | WS median | A/A WS2/WS range | WSH | PA | PL | PLB |
|---|---:|---|---:|---:|---:|---:|
| SSE 500 @2 exec: outcome | 0/12 knee | WS2 0/12 knee | 0/12 knee | 7/12 knee | 2/12 knee | 0/12 knee |
| SSE 500 @2: p50 | 58.0 | 0.89-1.21 | 0.98 | 13.79* | 0.83* | 0.81* |
| SSE 500 @2: p99 | 169.0 | 0.62-4.96 | 0.88 | 9.06* | 0.71 | 0.71 |
| CPU/event, 200 streams, 1 conn | 4.7 | 0.93-1.03 | 0.98 | 0.92* | 0.60* | 0.60* |
| CPU/event, 200 streams, 10 conns | 6.1 | 0.91-1.07 | 0.94 | 1.07* | 0.92 | 0.93 |
| one-shot req/s @2 (higher better) | 653293.4 | 0.95-1.02 | 1.00 | 1.03* | 1.03* | 1.03* |
| one-shot req/s @8 (higher better) | 1063817.8 | 0.96-1.02 | 1.00 | 1.00 | 0.99 | 1.00 |
| closed-loop p50 @2 | 1291.0 | 0.97-1.03 | 1.01 | 1.00 | 1.00 | 1.00 |
| closed-loop p99 @2 | 2029.0 | 0.93-1.12 | 1.02 | 1.02 | 1.00 | 1.01 |
| closed-loop p99 @8 | 2277.5 | 0.99-1.07 | 1.00 | 0.98* | 0.98* | 0.98* |
| open loop 60k @2: p50 | 114.0 | 0.92-1.20 | 1.02 | 1.60* | 1.26* | 1.28* |
| open loop 60k @2: p99 | 788.0 | 0.84-1.37 | 1.02 | 1.49* | 1.38* | 1.46* |
| open loop 120k @2: p50 | 191.5 | 0.42-1.69 | 1.13 | 1.42 | 1.44 | 1.44 |
| open loop 120k @2: p99 | 1022.5 | 0.85-1.30 | 1.02 | 1.09 | 1.13 | 1.07 |

Mac mix, churn paced 20 ms (eprod = 18 executors, overloaded for every arm)

| shape / metric | WS median | A/A WS2/WS range | WSH | PA | PL | PLB |
|---|---:|---|---:|---:|---:|---:|
| mix-e8 outcome | 10/12 p50>200us, 2 partial | WS2 9/12 p50>200us, 3 partial | 11/12 p50>200us, 1 partial | 10/12 p50>200us, 2 partial | 11/12 p50>200us, 1 partial | 12/12 p50>200us |
| mix-e8 worst heavy conn p99 | 3020.0 | 0.90-1.14 | 0.96 | 1.48* | 0.71* | 0.57* |
| mix-e8 worst light conn p99 | 531.0 | 0.95-1.23 | 0.98 | 0.91* | 0.59* | 0.33* |
| mix-e8 CPU/event | 6.0 | 1.00-1.00 | 1.00 | 1.04* | 0.69* | 0.76* |
| mix-e8 stopped streams (sum) | not measured | - | - | - | - | - |
| mix-eprod outcome | 4/12 p50>200us, 8 partial | WS2 1/12 p50>200us, 11 partial | 4/12 p50>200us, 8 partial | 1/12 p50>200us, 11 partial | 2/12 p50>200us, 8 partial, 2 failclosed | 1/12 p50>200us, 11 partial |
| mix-eprod worst heavy conn p99 | 22530.5 | 0.38-1.65 | 1.10 | 1.31 | 5.13* | 8.64* |
| mix-eprod worst light conn p99 | 6598.5 | 0.66-1.82 | 0.94 | 2.38* | 2.60* | 0.15* |
| mix-eprod CPU/event | 5.5 | 0.99-1.01 | 1.00 | 1.06* | 0.66* | 0.75* |
| mix-eprod stopped streams (sum) | not measured | - | - | - | - | - |
| mixm-e8 outcome | 11/12 p50>200us, 1 partial | WS2 11/12 p50>200us, 1 partial | 12/12 p50>200us | 10/12 p50>200us, 2 partial | 0/12 p50>200us | 0/12 p50>200us |
| mixm-e8 worst heavy conn p99 | 2174.5 | 0.94-1.05 | 0.98 | 1.73* | 0.34* | 0.23* |
| mixm-e8 worst light conn p99 | 314.0 | 0.89-1.18 | 1.00 | 1.13 | 0.51* | 0.43* |
| mixm-e8 CPU/event | 9.2 | 0.98-1.01 | 0.99 | 0.98* | 0.44* | 0.46* |
| mixm-e8 stopped streams (sum) | 7 | WS2 4 | 7 | 17 | 0 | 0 |
| mixm-eprod outcome | 7/12 p50>200us, 5 partial | WS2 8/12 p50>200us, 4 partial | 9/12 p50>200us, 3 partial | 5/12 p50>200us, 7 partial | 11/12 p50>200us, 1 partial | 9/12 p50>200us, 3 partial |
| mixm-eprod worst heavy conn p99 | 4491.5 | 0.86-1.42 | 0.88 | 2.74* | 2.29* | 2.22* |
| mixm-eprod worst light conn p99 | 1178.0 | 0.76-1.49 | 0.81 | 3.24* | 0.96 | 0.22* |
| mixm-eprod CPU/event | 9.6 | 0.97-1.02 | 1.00 | 1.09* | 0.52* | 0.60* |
| mixm-eprod stopped streams (sum) | 41 | WS2 38 | 30 | 55 | 25 | 30 |
