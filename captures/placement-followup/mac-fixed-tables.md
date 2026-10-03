| shape / metric | WS median | A/A WS2/WS range | PL | PLB |
|---|---:|---|---:|---:|
| SSE 500 @2 exec: outcome | 0/16 knee | WS2 0/16 knee | 2/16 knee | 0/16 knee |
| SSE 500 @2: p50 | 55.5 | 0.93-1.04 | 0.84* | 0.85* |
| SSE 500 @2: p99 | 121.5 | 0.07-5.63 | 0.79 | 0.68 |
| CPU/event, 200 streams, 1 conn | 4.6 | 0.95-1.05 | 0.62* | 0.63* |
| CPU/event, 200 streams, 10 conns | 6.1 | 0.91-1.04 | 0.92 | 0.90* |
| one-shot req/s @2 (higher better) | 648798.5 | 0.97-1.02 | 1.02 | 1.02 |
| one-shot req/s @8 (higher better) | 1058703.9 | 0.96-1.03 | 0.99 | 0.99 |
| closed-loop p50 @2 | 1274.5 | 0.99-1.03 | 0.99 | 0.99 |
| closed-loop p99 @2 | 1959.0 | 0.94-1.28 | 0.99 | 0.99 |
| closed-loop p99 @8 | 2258.5 | 0.95-1.02 | 0.96 | 0.92* |
| open loop 60k @2: p50 | 500.0 | 0.58-2.02 | 1.28 | 1.34 |
| open loop 60k @2: p99 | 1998.5 | 0.50-1.42 | 1.11 | 1.11 |
| open loop 120k @2: p50 | 386.0 | 0.67-1.28 | 1.26 | 1.22 |
| open loop 120k @2: p99 | 1320.5 | 0.89-1.13 | 1.17* | 1.16* |
| shape / metric | WS median | A/A WS2/WS range | PL | PLB |
|---|---:|---|---:|---:|
| mix-e8 outcome | 16/16 p50>200us | WS2 16/16 p50>200us | 16/16 p50>200us | 16/16 p50>200us |
| mix-e8 worst heavy conn p99 | 2989.5 | 0.93-1.06 | 0.65* | 0.57* |
| mix-e8 worst light conn p99 | 528.0 | 0.91-1.05 | 0.63* | 0.31* |
| mix-e8 CPU/event | 6.0 | 1.00-1.00 | 0.68* | 0.75* |
| mix-e8 stopped streams (sum) | 0 | WS2 0 | 0 | 0 |
| mix-eprod outcome | 16/16 p50>200us | WS2 16/16 p50>200us | 16/16 p50>200us | 16/16 p50>200us |
| mix-eprod worst heavy conn p99 | 24995.0 | 0.22-2.18 | 8.75* | 8.04* |
| mix-eprod worst light conn p99 | 5398.0 | 0.14-1.88 | 5.41* | 0.08* |
| mix-eprod CPU/event | 5.6 | 0.99-1.01 | 0.64* | 0.73* |
| mix-eprod stopped streams (sum) | 0 | WS2 0 | 0 | 0 |
| mixm-e8 outcome | 16/16 p50>200us | WS2 16/16 p50>200us | 0/16 p50>200us | 0/16 p50>200us |
| mixm-e8 worst heavy conn p99 | 2094.5 | 0.95-1.05 | 0.20* | 0.14* |
| mixm-e8 worst light conn p99 | 309.0 | 0.82-1.15 | 0.54* | 0.42* |
| mixm-e8 CPU/event | 9.2 | 0.99-1.01 | 0.44* | 0.45* |
| mixm-e8 stopped streams (sum) | 0 | WS2 0 | 0 | 0 |
| mixm-eprod outcome | 16/16 p50>200us | WS2 16/16 p50>200us | 16/16 p50>200us | 16/16 p50>200us |
| mixm-eprod worst heavy conn p99 | 4505.5 | 0.80-1.30 | 2.25* | 1.74* |
| mixm-eprod worst light conn p99 | 939.5 | 0.66-1.42 | 1.90* | 0.27* |
| mixm-eprod CPU/event | 9.7 | 0.98-1.02 | 0.55* | 0.60* |
| mixm-eprod stopped streams (sum) | 0 | WS2 0 | 0 | 0 |
