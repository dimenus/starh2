#!/bin/sh
# Opt-in: build starh2 against the zig-crypto std (private dimenus/zig-crypto,
# branch carmack/zig-crypto-sec-pass) instead of the std that ships with Zig.
# ./zb and `zig build` never use it; this script is the only way in.
#
#   tools/zig-crypto.sh <zig-crypto-checkout> [zig build args...]
#   tools/zig-crypto.sh --check <zig-crypto-checkout>
#
# e.g. tools/zig-crypto.sh ~/src/zig-crypto-sec-pass test
#      tools/zig-crypto.sh ~/src/zig-crypto-sec-pass bench -Doptimize=fast -- -c 50
#
# Before building, and failing loudly on any mismatch, it:
# 1. requires the checkout's lib/ to be exactly the pinned lib/ tree: the
#    pin commit present, HEAD's lib/ tree equal to the pin's, no staged,
#    unstaged or untracked changes under lib/;
# 2. requires the constant-time guard (tools/crypto_sec_pass/ctguard.*) at
#    or after its pinned commit, with those files unchanged;
# 3. runs the guard against the checkout's lib/ with the pinned zig. A pass
#    is cached in .zig-cache/zig-crypto-guard/ per lib tree, guard commit
#    and zig version, so repeat builds skip it;
# 4. runs ./zb build <args> with ZB_ZIG_LIB=<checkout>/lib, which makes zb
#    pass --zig-lib=<lib> as the first `zig build` argument (the only way
#    Zig 0.17's build frontend takes a std override) and export ZIG_LIB_DIR
#    for child zig processes.
# --check stops after step 3 (for CI). To move the pin, edit it here, in a
# reviewed commit; there is no override.
#
# Why not -Dzig-crypto=<path>: build.zig is compiled against, and run by,
# a build runner that already has its std. A -D option is read inside that
# run, too late to change the std that build.zig, the runner and every
# compile step use.
set -eu
pin=4f47b1d81c2077d5ca50a976c84ef4259b8fcefc
guard_pin=64409e6b7487c98aafe69835e2289a9bfe5c7de9
guard_files="ctguard.sh ctguard.zig ctguard.expect ctcount.awk"

die() {
  echo "zig-crypto.sh: $*" >&2
  exit 1
}
usage() {
  sed -n '6,7p' "$0" | sed 's/^# *//' >&2
  exit 2
}

check_only=0
if [ "${1:-}" = "--check" ]; then
  check_only=1
  shift
fi
[ $# -ge 1 ] || usage
case $1 in -*) usage ;; esac
co=$1
shift
[ "$check_only" -eq 0 ] || [ $# -eq 0 ] || usage

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
[ -d "$co" ] || die "no such directory: $co"
co=$(CDPATH= cd -- "$co" && pwd -P)
lib="$co/lib"
[ -f "$lib/std/std.zig" ] || die "$co is not a zig-crypto checkout (no lib/std/std.zig)"
git -C "$co" rev-parse --git-dir >/dev/null 2>&1 ||
  die "$co is not a git checkout, so its lib/ cannot be checked against the pin"

# 1. lib/ is exactly the pin's.
git -C "$co" cat-file -e "$pin^{commit}" 2>/dev/null ||
  die "pin $pin is not in $co (fetch carmack/zig-crypto-sec-pass)"
want_tree=$(git -C "$co" rev-parse "$pin:lib")
have_tree=$(git -C "$co" rev-parse "HEAD:lib")
head=$(git -C "$co" rev-parse HEAD)
[ "$have_tree" = "$want_tree" ] ||
  die "HEAD $head has a different lib/ (tree $have_tree) than pin $pin (tree $want_tree)"
git -C "$co" diff --quiet HEAD -- lib && git -C "$co" diff --cached --quiet HEAD -- lib ||
  die "$co has local changes under lib/ (git -C $co status -- lib)"
[ -z "$(git -C "$co" ls-files --others --exclude-standard -- lib)" ] ||
  die "$co has untracked files under lib/ (git -C $co status -- lib)"

# 2. The guard is the pinned one.
git -C "$co" cat-file -e "$guard_pin^{commit}" 2>/dev/null &&
  git -C "$co" merge-base --is-ancestor "$guard_pin" HEAD ||
  die "$co lacks the constant-time guard commit $guard_pin (carmack/zig-crypto-sec-pass)"
for f in $guard_files; do
  git -C "$co" diff --quiet "$guard_pin" -- "tools/crypto_sec_pass/$f" ||
    die "tools/crypto_sec_pass/$f in $co differs from guard commit $guard_pin"
done

# 3. Constant-time guard.
zv=$("$root/zb" version) || die "cannot run ./zb"
stamp="$root/.zig-cache/zig-crypto-guard/$want_tree-$guard_pin-$zv"
echo "zig-crypto.sh: lib $lib (HEAD $head, lib tree = pin $pin)" >&2
if [ -f "$stamp" ]; then
  echo "zig-crypto.sh: constant-time guard already passed for this lib/guard/zig ($stamp)" >&2
else
  echo "zig-crypto.sh: running the constant-time guard" >&2
  ZIG="$root/zb" sh "$co/tools/crypto_sec_pass/ctguard.sh" --lib "$lib" >&2 ||
    die "constant-time guard FAILED for $lib; not building"
  mkdir -p "$(dirname "$stamp")"
  : >"$stamp"
fi
[ "$check_only" -eq 0 ] || exit 0

# 4. Build.
cd "$root"
ZB_ZIG_LIB="$lib" exec "$root/zb" build "$@"
