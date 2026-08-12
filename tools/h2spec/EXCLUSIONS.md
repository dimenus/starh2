# h2spec exclusions (RFC 9113)

starh2 intentionally parses and ignores RFC 7540 priority dependency fields
(DESIGN §7.2 / §10). The following h2spec http2 cases exercise only those
removed semantics and are published exclusions — not expected-failure baselines
for other failures.

| Stable ID | Citation | Reason |
|-----------|----------|--------|
| http2/5.3.1/1 | RFC 7540 §5.3.1; removed by RFC 9113 | HEADERS depends on itself |
| http2/5.3.1/2 | RFC 7540 §5.3.1; removed by RFC 9113 | PRIORITY depends on itself |

All other h2spec http2 cases remain required.
