#!/bin/sh
# Session-local zb: gcc-16 native crt1.o has .sframe; zig 0.16 needs an
# explicit -Dtarget so it uses the bundled CRT (tools/README.md).
ZIG="${HOME}/.zvm/bin/zig"
if [ "$1" = "build" ]; then
  shift
  has_target=0
  for a in "$@"; do
    case "$a" in
      -Dtarget=*) has_target=1 ;;
    esac
  done
  if [ "$has_target" = 0 ]; then
    exec "$ZIG" build -Dtarget=x86_64-linux-gnu "$@"
  fi
  exec "$ZIG" build "$@"
fi
exec "$ZIG" "$@"
