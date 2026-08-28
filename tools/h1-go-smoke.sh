#!/bin/sh
# HTTP/1.1 Go net/http oracle. Prefer `./zb build h1-go-smoke`.
set -eu
cd "$(dirname "$0")/.."
exec ./zb build h1-go-smoke "$@"
