# zio-migration-repro

Standalone loopback stress example: three tasks per connection (read owner,
actor, write owner) talking through bounded `std.Io.Queue`s, with write
completion acknowledgments. Listens on `127.0.0.1` only. Depends on the
already-pinned zio revision; no Starh2, TLS, or HTTP.

This is a **non-reproducing stress example**. Both scheduler configurations
always finished on the machine below. Do not treat a green run as a bug
report.

## Pin and platform

| | |
|---|---|
| Zig | 0.16.0 (`~/.zvm/bin/zig`) |
| zio | `a2b134a7abe35d9b29e8578fad30adc7a026fb4e` (`zio-0.17.0-xHbVVKr9JQBFsFjx4hcL4z3BkqyBkonICNTz5DdJxhE2`) |
| OS | Darwin 24.6.0, arm64 (Apple M3 Pro) |
| Executors | `.auto` → 12 |

## Commands

From this directory:

```sh
../../zb build -Doptimize=ReleaseFast
./zig-out/bin/zio-migration-repro
./zig-out/bin/zio-migration-repro --no-migration
```

Default workload: 100 rounds × 50 connections × 2000 requests (10_000_000
replies). A 5 s watchdog with no reply progress exits 2 and prints `STALL`.
Success prints `PASS: … replies, migration=…`.

## Measured runs (2026-08-15)

Each run used the default workload, ReleaseFast, 12 executors.

| Configuration | Runs | Result | Wall time |
|---|---|---|---|
| default (`enable_task_migration = true`) | 8/8 | PASS, 10_000_000 replies | 34.72–34.92 s |
| `--no-migration` | 8/8 | PASS, 10_000_000 replies | 34.72–34.86 s |

No stall. This plaintext three-task topology did not stop making progress
under either scheduler setting.
