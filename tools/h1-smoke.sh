#!/bin/sh
# HTTP/1.1 edge gate. Prefer `./zb build h1-smoke`.
set -eu
cd "$(dirname "$0")/.."
exec ./zb build h1-smoke "$@"
