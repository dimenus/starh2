### SSE 500 at 2 executors

| shape / metric | WS median | A/A WS2/WS range | PLC | PLH | PLS |
|---|---:|---|---:|---:|---:|
| outcome | 0/20 knee | WS2 0/20 knee | 0/20 knee | 0/20 knee | 0/20 knee |
| p50 | 17.0 | 0.75-1.19 | 0.75 (n=20) | 0.75 (n=20) | 0.76 (n=20) |
| p99 | 65.0 | 0.44-1.85 | 0.60 (n=20) | 0.64 (n=20) | 0.58 (n=20) |
| worst conn p99 | 71.0 | 0.44-1.88 | 0.60 (n=20) | 0.63 (n=20) | 0.58 (n=20) |

### mix-unpaced (8 executors = e8, 12 = eprod)

| shape / metric | WS median | A/A WS2/WS range | PLC | PLH | PLS |
|---|---:|---|---:|---:|---:|
| mix-e8 heavy saturation, rounds and conns with p50 over 200us | 10/10 sat (40/40 conns), 0 partial | WS2 10/10 sat (40/40 conns), 0 partial | 10/10 sat (24/40 conns), 0 partial | 0/10 sat (0/41 conns), 0 partial | 0/10 sat (0/40 conns), 0 partial |
| mix-e8 churn on heavy executors, median share | - | WS2 - | 0.35 (uniform 0.50), shared 0 | 0.00 (uniform 0.50), shared 0 | 0.00 (uniform 0.50), shared 0 |
| mix-e8 worst heavy conn p99 | 1917.5 | 0.99-1.02 | 0.73* (n=10) | 0.47* (n=10) | 0.54* (n=10) |
| mix-e8 worst heavy conn p99, complete-delivery pairs | 1917.5 | 0.99-1.02 | 0.73* (n=10) | 0.47* (n=10) | 0.54* (n=10) |
| mix-e8 worst light conn p99 | 385.5 | 0.93-1.12 | 0.70* (n=10) | 0.60* (n=10) | 0.58* (n=10) |
| mix-e8 worst light conn p99, complete-delivery pairs | 385.5 | 0.93-1.12 | 0.70* (n=10) | 0.60* (n=10) | 0.58* (n=10) |
| mix-e8 CPU/event | 7.6 | 1.00-1.00 | 0.68* (n=10) | 0.76* (n=10) | 0.76* (n=10) |
| mix-e8 CPU/event, complete-delivery pairs | 7.6 | 1.00-1.00 | 0.68* (n=10) | 0.76* (n=10) | 0.76* (n=10) |
| mix-e8 churn requests (higher better) | 218581.0 | 0.98-1.01 | 1.10* (n=10) | 1.35* (n=10) | 1.34* (n=10) |
| mix-e8 churn requests (higher better), complete-delivery pairs | 218581.0 | 0.98-1.01 | 1.10* (n=10) | 1.35* (n=10) | 1.34* (n=10) |
| mix-e8 stopped streams (sum) | 0 | WS2 0 | 0 | 0 | 0 |
| mix-eprod heavy saturation, rounds and conns with p50 over 200us | 10/10 sat (60/60 conns), 0 partial | WS2 10/10 sat (60/60 conns), 0 partial | 10/10 sat (50/60 conns), 0 partial | 8/10 sat (44/48 conns), 2 partial, 2 ZERO-EVENT collapse | 10/10 sat (56/60 conns), 0 partial |
| mix-eprod churn on heavy executors, median share | - | WS2 - | 0.40 (uniform 0.50), shared 0 | 0.01 (uniform 0.50), shared 0, 2 round(s) with no heavy placement to report | 0.01 (uniform 0.50), shared 0 |
| mix-eprod worst heavy conn p99 | 2371.0 | 0.94-1.16 | 0.67* (n=10) | 0.62* (n=8) | 0.62* (n=10) |
| mix-eprod worst heavy conn p99, complete-delivery pairs | 2371.0 | 0.94-1.16 | 0.67* (n=10) | 0.62* (n=8) | 0.62* (n=10) |
| mix-eprod worst light conn p99 | 482.5 | 0.84-1.10 | 1.19* (n=10) | 1.16* (n=8) | 1.31* (n=10) |
| mix-eprod worst light conn p99, complete-delivery pairs | 482.5 | 0.84-1.10 | 1.19* (n=10) | 1.16* (n=8) | 1.31* (n=10) |
| mix-eprod CPU/event | 7.5 | 1.00-1.00 | 0.66* (n=10) | 0.77* (n=8) | 0.77* (n=10) |
| mix-eprod CPU/event, complete-delivery pairs | 7.5 | 1.00-1.00 | 0.66* (n=10) | 0.77* (n=8) | 0.77* (n=10) |
| mix-eprod churn requests (higher better) | 195274.5 | 0.97-1.02 | 1.00 (n=10) | 1.34* (n=8) | 1.34* (n=10) |
| mix-eprod churn requests (higher better), complete-delivery pairs | 195274.5 | 0.97-1.02 | 1.00 (n=10) | 1.34* (n=8) | 1.34* (n=10) |
| mix-eprod stopped streams (sum) | 0 | WS2 0 | 0 | 0 | 0 |

### mixm-unpaced (8 executors = e8, 12 = eprod)

| shape / metric | WS median | A/A WS2/WS range | PLC | PLH | PLS |
|---|---:|---|---:|---:|---:|
| mixm-e8 heavy saturation, rounds and conns with p50 over 200us | 10/10 sat (20/20 conns), 0 partial | WS2 10/10 sat (20/20 conns), 0 partial | 0/10 sat (0/20 conns), 0 partial | 0/10 sat (0/20 conns), 0 partial | 0/10 sat (0/20 conns), 0 partial |
| mixm-e8 churn on heavy executors, median share | - | WS2 - | 0.19 (uniform 0.25), shared 0 | 0.00 (uniform 0.25), shared 0 | 0.00 (uniform 0.25), shared 0 |
| mixm-e8 worst heavy conn p99 | 1563.0 | 0.99-1.03 | 0.60* (n=10) | 0.19* (n=10) | 0.20* (n=10) |
| mixm-e8 worst heavy conn p99, complete-delivery pairs | 1563.0 | 0.99-1.03 | 0.60* (n=10) | 0.19* (n=10) | 0.20* (n=10) |
| mixm-e8 worst light conn p99 | 184.0 | 0.94-1.24 | 0.78* (n=10) | 0.89* (n=10) | 0.86* (n=10) |
| mixm-e8 worst light conn p99, complete-delivery pairs | 184.0 | 0.94-1.24 | 0.78* (n=10) | 0.89* (n=10) | 0.86* (n=10) |
| mixm-e8 CPU/event | 11.5 | 0.99-1.01 | 0.61* (n=10) | 0.63* (n=10) | 0.63* (n=10) |
| mixm-e8 CPU/event, complete-delivery pairs | 11.5 | 0.99-1.01 | 0.61* (n=10) | 0.63* (n=10) | 0.63* (n=10) |
| mixm-e8 churn requests (higher better) | 283420.5 | 0.99-1.01 | 1.13* (n=10) | 1.16* (n=10) | 1.16* (n=10) |
| mixm-e8 churn requests (higher better), complete-delivery pairs | 283420.5 | 0.99-1.01 | 1.13* (n=10) | 1.16* (n=10) | 1.16* (n=10) |
| mixm-e8 stopped streams (sum) | 0 | WS2 0 | 0 | 0 | 0 |
| mixm-eprod heavy saturation, rounds and conns with p50 over 200us | 10/10 sat (30/30 conns), 0 partial | WS2 10/10 sat (30/30 conns), 0 partial | 0/10 sat (0/30 conns), 0 partial | 0/10 sat (0/30 conns), 0 partial | 0/10 sat (0/30 conns), 0 partial |
| mixm-eprod churn on heavy executors, median share | - | WS2 - | 0.03 (uniform 0.25), shared 0 | 0.00 (uniform 0.25), shared 0 | 0.00 (uniform 0.25), shared 0 |
| mixm-eprod worst heavy conn p99 | 1775.5 | 0.98-1.04 | 0.61* (n=10) | 0.37* (n=10) | 0.41* (n=10) |
| mixm-eprod worst heavy conn p99, complete-delivery pairs | 1775.5 | 0.98-1.04 | 0.61* (n=10) | 0.37* (n=10) | 0.41* (n=10) |
| mixm-eprod worst light conn p99 | 207.5 | 0.80-1.25 | 0.80* (n=10) | 0.81 (n=10) | 0.81 (n=10) |
| mixm-eprod worst light conn p99, complete-delivery pairs | 207.5 | 0.80-1.25 | 0.80* (n=10) | 0.81 (n=10) | 0.81 (n=10) |
| mixm-eprod CPU/event | 10.8 | 0.99-1.01 | 0.62* (n=10) | 0.63* (n=10) | 0.63* (n=10) |
| mixm-eprod CPU/event, complete-delivery pairs | 10.8 | 0.99-1.01 | 0.62* (n=10) | 0.63* (n=10) | 0.63* (n=10) |
| mixm-eprod churn requests (higher better) | 260424.5 | 0.98-1.02 | 1.20* (n=10) | 1.22* (n=10) | 1.21* (n=10) |
| mixm-eprod churn requests (higher better), complete-delivery pairs | 260424.5 | 0.98-1.02 | 1.20* (n=10) | 1.22* (n=10) | 1.21* (n=10) |
| mixm-eprod stopped streams (sum) | 0 | WS2 0 | 0 | 0 | 0 |

### mix-paced (8 executors = e8, 12 = eprod)

| shape / metric | WS median | A/A WS2/WS range | PLC | PLH | PLS |
|---|---:|---|---:|---:|---:|
| mix-e8 heavy saturation, rounds and conns with p50 over 200us | 0/10 sat (0/40 conns), 0 partial | WS2 0/10 sat (0/40 conns), 0 partial | 0/10 sat (0/40 conns), 0 partial | 0/10 sat (0/40 conns), 0 partial | 0/10 sat (0/40 conns), 0 partial |
| mix-e8 churn on heavy executors, median share | - | WS2 - | 0.96 (uniform 0.50), shared 0 | 0.01 (uniform 0.50), shared 0 | 0.01 (uniform 0.50), shared 0 |
| mix-e8 worst heavy conn p99 | 1222.0 | 0.92-1.08 | 0.13* (n=10) | 0.09* (n=10) | 0.10* (n=10) |
| mix-e8 worst heavy conn p99, complete-delivery pairs | 1222.0 | 0.92-1.08 | 0.13* (n=10) | 0.09* (n=10) | 0.10* (n=10) |
| mix-e8 worst light conn p99 | 127.5 | 0.75-1.39 | 0.38* (n=10) | 0.47* (n=10) | 0.56* (n=10) |
| mix-e8 worst light conn p99, complete-delivery pairs | 127.5 | 0.75-1.39 | 0.38* (n=10) | 0.47* (n=10) | 0.56* (n=10) |
| mix-e8 CPU/event | 6.6 | 0.99-1.01 | 0.47* (n=10) | 0.49* (n=10) | 0.48* (n=10) |
| mix-e8 CPU/event, complete-delivery pairs | 6.6 | 0.99-1.01 | 0.47* (n=10) | 0.49* (n=10) | 0.48* (n=10) |
| mix-e8 churn requests (higher better) | 9620.0 | 1.00-1.00 | 1.00 (n=10) | 1.01* (n=10) | 1.01* (n=10) |
| mix-e8 churn requests (higher better), complete-delivery pairs | 9620.0 | 1.00-1.00 | 1.00 (n=10) | 1.01* (n=10) | 1.01* (n=10) |
| mix-e8 stopped streams (sum) | 0 | WS2 0 | 0 | 0 | 0 |
| mix-eprod heavy saturation, rounds and conns with p50 over 200us | 10/10 sat (60/60 conns), 0 partial | WS2 10/10 sat (60/60 conns), 0 partial | 0/10 sat (0/60 conns), 0 partial | 0/10 sat (0/60 conns), 0 partial | 0/10 sat (0/60 conns), 0 partial |
| mix-eprod churn on heavy executors, median share | - | WS2 - | 0.95 (uniform 0.50), shared 0 | 0.01 (uniform 0.50), shared 0 | 0.02 (uniform 0.50), shared 0 |
| mix-eprod worst heavy conn p99 | 1986.5 | 0.98-1.09 | 0.51* (n=10) | 0.32* (n=10) | 0.38* (n=10) |
| mix-eprod worst heavy conn p99, complete-delivery pairs | 1986.5 | 0.98-1.09 | 0.51* (n=10) | 0.32* (n=10) | 0.38* (n=10) |
| mix-eprod worst light conn p99 | 365.5 | 0.84-1.18 | 0.67* (n=10) | 0.69* (n=10) | 0.65* (n=10) |
| mix-eprod worst light conn p99, complete-delivery pairs | 365.5 | 0.84-1.18 | 0.67* (n=10) | 0.69* (n=10) | 0.65* (n=10) |
| mix-eprod CPU/event | 6.9 | 1.00-1.00 | 0.56* (n=10) | 0.56* (n=10) | 0.56* (n=10) |
| mix-eprod CPU/event, complete-delivery pairs | 6.9 | 1.00-1.00 | 0.56* (n=10) | 0.56* (n=10) | 0.56* (n=10) |
| mix-eprod churn requests (higher better) | 9514.5 | 1.00-1.00 | 1.00* (n=10) | 1.01* (n=10) | 1.01* (n=10) |
| mix-eprod churn requests (higher better), complete-delivery pairs | 9514.5 | 1.00-1.00 | 1.00* (n=10) | 1.01* (n=10) | 1.01* (n=10) |
| mix-eprod stopped streams (sum) | 0 | WS2 0 | 0 | 0 | 0 |

### mixm-paced (8 executors = e8, 12 = eprod)

| shape / metric | WS median | A/A WS2/WS range | PLC | PLH | PLS |
|---|---:|---|---:|---:|---:|
| mixm-e8 heavy saturation, rounds and conns with p50 over 200us | 0/10 sat (0/20 conns), 0 partial | WS2 0/10 sat (0/20 conns), 0 partial | 0/10 sat (0/20 conns), 0 partial | 0/10 sat (0/20 conns), 0 partial | 0/10 sat (0/20 conns), 0 partial |
| mixm-e8 churn on heavy executors, median share | - | WS2 - | 0.00 (uniform 0.25), shared 0 | 0.00 (uniform 0.25), shared 0 | 0.00 (uniform 0.25), shared 0 |
| mixm-e8 worst heavy conn p99 | 410.0 | 0.32-4.42 | 0.15* (n=10) | 0.15* (n=10) | 0.12* (n=10) |
| mixm-e8 worst heavy conn p99, complete-delivery pairs | 410.0 | 0.32-4.42 | 0.15* (n=10) | 0.15* (n=10) | 0.12* (n=10) |
| mixm-e8 worst light conn p99 | 59.0 | 0.77-1.36 | 0.70* (n=10) | 0.74* (n=10) | 0.64* (n=10) |
| mixm-e8 worst light conn p99, complete-delivery pairs | 59.0 | 0.77-1.36 | 0.70* (n=10) | 0.74* (n=10) | 0.64* (n=10) |
| mixm-e8 CPU/event | 7.4 | 0.84-1.23 | 0.40* (n=10) | 0.39* (n=10) | 0.39* (n=10) |
| mixm-e8 CPU/event, complete-delivery pairs | 7.4 | 0.84-1.23 | 0.40* (n=10) | 0.39* (n=10) | 0.39* (n=10) |
| mixm-e8 churn requests (higher better) | 9660.0 | 1.00-1.01 | 1.00 (n=10) | 1.00 (n=10) | 1.00 (n=10) |
| mixm-e8 churn requests (higher better), complete-delivery pairs | 9660.0 | 1.00-1.01 | 1.00 (n=10) | 1.00 (n=10) | 1.00 (n=10) |
| mixm-e8 stopped streams (sum) | 0 | WS2 0 | 0 | 0 | 0 |
| mixm-eprod heavy saturation, rounds and conns with p50 over 200us | 8/10 sat (19/30 conns), 0 partial | WS2 6/10 sat (15/30 conns), 0 partial | 0/10 sat (0/30 conns), 0 partial | 0/10 sat (0/30 conns), 0 partial | 0/10 sat (0/30 conns), 0 partial |
| mixm-eprod churn on heavy executors, median share | - | WS2 - | 0.00 (uniform 0.25), shared 0 | 0.00 (uniform 0.25), shared 0 | 0.00 (uniform 0.25), shared 0 |
| mixm-eprod worst heavy conn p99 | 1377.0 | 0.89-1.11 | 0.07* (n=10) | 0.06* (n=10) | 0.06* (n=10) |
| mixm-eprod worst heavy conn p99, complete-delivery pairs | 1377.0 | 0.89-1.11 | 0.07* (n=10) | 0.06* (n=10) | 0.06* (n=10) |
| mixm-eprod worst light conn p99 | 121.0 | 0.61-1.65 | 0.52* (n=10) | 0.53* (n=10) | 0.54* (n=10) |
| mixm-eprod worst light conn p99, complete-delivery pairs | 121.0 | 0.61-1.65 | 0.52* (n=10) | 0.53* (n=10) | 0.54* (n=10) |
| mixm-eprod CPU/event | 8.1 | 0.99-1.02 | 0.41* (n=10) | 0.39* (n=10) | 0.40* (n=10) |
| mixm-eprod CPU/event, complete-delivery pairs | 8.1 | 0.99-1.02 | 0.41* (n=10) | 0.39* (n=10) | 0.40* (n=10) |
| mixm-eprod churn requests (higher better) | 9630.0 | 1.00-1.00 | 1.00* (n=10) | 1.01* (n=10) | 1.01* (n=10) |
| mixm-eprod churn requests (higher better), complete-delivery pairs | 9630.0 | 1.00-1.00 | 1.00* (n=10) | 1.01* (n=10) | 1.01* (n=10) |
| mixm-eprod stopped streams (sum) | 0 | WS2 0 | 0 | 0 | 0 |
