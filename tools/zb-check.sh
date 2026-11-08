#!/bin/bash
# Check that ./zb runs exactly the pinned zig, whatever zig is first on PATH.
#
#   tools/zb-check.sh [<zb>]     (default: the repo's ./zb)
#
# `./zb build zb-check` runs it, and `ci` depends on it. It needs mise and the
# pinned zig installed through mise: the same things ./zb needs on the
# machines that run ci. Without mise it exits 2; it does not skip.
#
# Each case copies <zb> into a temporary root with its own build.zig.zon, and
# runs it with a PATH whose FIRST entry holds a fake `zig`. The fake prints
# its own version for `zig version`, and appends a line to a marker file on
# every run, so the check can see whether zb ran it.
#
# Every case runs under `env -i`: nothing of the caller's environment, mise
# config or mise state reaches it, only the installs (see run_zb). So a case
# gives the same answer from any shell, a mise-activated one included.
#
#   1. mise, pin installed, and PATH shaped like a shell where mise is
#      activated with another zig ahead of it: the fake, then the pin's
#      install directory, then mise's shims directory. `zb version` must
#      print the pin and never run the fake. (`mise exec zig@<pin> -- zig`
#      runs the fake in this shape: it leaves directories already on PATH
#      where they are. That was t-3912, with ~/.zvm/bin as the other zig.)
#   2. mise, pin not installed: zb exits non-zero with the
#      `mise install zig@<pin>` hint and never runs the fake.
#   3. no mise, no ~/.zvm, the fake on PATH has another version: zb refuses
#      it, naming both versions.
#   4. no mise, no ~/.zvm, the fake on PATH has the pin's version: zb runs it.
#   5. mise, pin installed, only the fake ahead of mise on PATH: as case 1.
#
# Exit 0: every case held. Exit 1: a case failed (each is printed). Exit 2:
# setup failed (no mise, no build.zig.zon pin).
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
zb_src=${1:-$repo/zb}
[ -f "$zb_src" ] || { echo "zb-check FATAL: no zb at $zb_src" >&2; exit 2; }
pin=$(sed -n 's/.*\.minimum_zig_version = "\([^"]*\)".*/\1/p' "$repo/build.zig.zon")
[ -n "$pin" ] || { echo "zb-check FATAL: no minimum_zig_version in $repo/build.zig.zon" >&2; exit 2; }
mise_bin=$(type -P mise || true)
[ -n "$mise_bin" ] || { echo "zb-check FATAL: no mise on PATH; this check tests zb's mise path and cannot run without it" >&2; exit 2; }
"$mise_bin" where "zig@$pin" > /dev/null 2>&1 || { echo "zb-check FATAL: mise has no zig $pin installed (mise install zig@$pin)" >&2; exit 2; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/zb-check.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
# PATH is built from these, never from /usr/bin or /bin: on Linux mise can
# live in /usr/bin, and a "no mise" case must not find it. zb itself needs
# only sed and dirname.
mkdir "$tmp/misebin" "$tmp/basebin"
ln -s "$mise_bin" "$tmp/misebin/mise"
for t in sed dirname; do
  p=$(type -P "$t") || { echo "zb-check FATAL: no $t on PATH" >&2; exit 2; }
  ln -s "$p" "$tmp/basebin/$t"
done

fake_version=0.0.0-zbcheck
missing_pin=0.0.1-zbcheck-not-installed
failed=0

# A root holding a copy of zb, a build.zig.zon pinning $2, and a fake zig
# that prints $3 for `zig version`.
make_root() {
  local dir=$tmp/$1 want=$2 says=$3
  mkdir -p "$dir/fake"
  cp "$zb_src" "$dir/zb"
  chmod +x "$dir/zb"
  printf '.{\n    .minimum_zig_version = "%s",\n}\n' "$want" > "$dir/build.zig.zon"
  cat > "$dir/fake/zig" <<EOF
#!/bin/sh
echo "\$*" >> "$dir/fake-ran"
if [ "\$1" = version ]; then echo "$says"; exit 0; fi
echo "fake zig ran: \$*" >&2
exit 97
EOF
  chmod +x "$dir/fake/zig"
}

# Where the pinned zig is installed, and mise's data directory around it
# (<data>/installs/zig/<pin>), whose shims directory case 1 needs.
pin_dir=$("$mise_bin" where "zig@$pin")
data=$(cd "$pin_dir/../../.." && pwd)
shims=$data/shims
[ -d "$shims" ] || { echo "zb-check FATAL: no mise shims directory at $shims (derived from $pin_dir)" >&2; exit 2; }

# Run the root's zb as `zb version` with PATH=$2 and nothing else from the
# caller: `env -i`. What mise does depends on more than PATH. It reads
# MISE_* and __MISE_* variables (a shell where mise is activated exports
# MISE_SHELL, __MISE_ORIG_PATH, __MISE_DIFF, __MISE_SESSION), its global and
# system config, and config files in the directories above the working one.
# A caller's activated mise once made case 1 pass for the old zb. So every
# run gets an empty HOME of its own, and with $3 = mise, mise's config,
# state and cache directories point at empty directories of the case's own,
# the config search stops at the case's root, and only the data directory
# (where the installs are) is the real one. Sets out, err, code.
run_zb() {
  local dir=$tmp/$1 mise_env=()
  mkdir -p "$dir/home" "$dir/mise-config" "$dir/mise-system" "$dir/mise-state" "$dir/mise-cache"
  if [ "$3" = mise ]; then
    mise_env=(
      MISE_DATA_DIR="$data"
      MISE_CONFIG_DIR="$dir/mise-config"
      MISE_GLOBAL_CONFIG_FILE="$dir/mise-config/config.toml"
      MISE_SYSTEM_DIR="$dir/mise-system"
      MISE_SYSTEM_CONFIG_DIR="$dir/mise-system"
      MISE_STATE_DIR="$dir/mise-state"
      MISE_CACHE_DIR="$dir/mise-cache"
      MISE_CEILING_PATHS="$dir"
    )
  fi
  set +e
  out=$(cd "$dir" && env -i HOME="$dir/home" PATH="$2" ${mise_env[@]+"${mise_env[@]}"} ./zb version 2> "$dir/stderr")
  code=$?
  set -e
  err=$(cat "$dir/stderr")
}

ok() { echo "zb-check: case $1: ok ($2)"; }
bad() { echo "zb-check: case $1: FAIL: $2"; failed=1; }
fake_ran() { [ -s "$tmp/$1/fake-ran" ]; }

# 1
make_root c1 "$pin" "$fake_version"
run_zb c1 "$tmp/c1/fake:$pin_dir/bin:$shims:$tmp/misebin:$tmp/basebin" mise
if [ "$code" -eq 0 ] && [ "$out" = "$pin" ] && ! fake_ran c1; then
  ok 1 "mise activated, fake zig ahead of it: printed $out"
else
  bad 1 "mise activated, fake zig ahead of it: exit $code, printed '$out', fake ran: $(cat "$tmp/c1/fake-ran" 2>/dev/null | tr '\n' ' ')stderr: $err"
fi

# 2
make_root c2 "$missing_pin" "$fake_version"
run_zb c2 "$tmp/c2/fake:$tmp/misebin:$tmp/basebin" mise
if [ "$code" -ne 0 ] && grep -qF "mise install zig@$missing_pin" "$tmp/c2/stderr" && ! fake_ran c2; then
  ok 2 "mise, pin not installed: exit $code with the install hint"
else
  bad 2 "mise, pin not installed: exit $code, printed '$out', fake ran: $(cat "$tmp/c2/fake-ran" 2>/dev/null | tr '\n' ' ')stderr: $err"
fi

# 3
make_root c3 "$pin" "$fake_version"
run_zb c3 "$tmp/c3/fake:$tmp/basebin" none
if [ "$code" -ne 0 ] && grep -qF "is $fake_version, build.zig.zon pins $pin" "$tmp/c3/stderr"; then
  ok 3 "no mise, wrong zig on PATH: refused"
else
  bad 3 "no mise, wrong zig on PATH: exit $code, printed '$out', stderr: $err"
fi

# 4
make_root c4 "$pin" "$pin"
run_zb c4 "$tmp/c4/fake:$tmp/basebin" none
if [ "$code" -eq 0 ] && [ "$out" = "$pin" ] && [ "$(grep -c '^version$' "$tmp/c4/fake-ran")" -eq 2 ]; then
  ok 4 "no mise, pinned zig on PATH: checked, then ran"
else
  bad 4 "no mise, pinned zig on PATH: exit $code, printed '$out', fake ran: $(cat "$tmp/c4/fake-ran" 2>/dev/null | tr '\n' ' ')stderr: $err"
fi

# 5
make_root c5 "$pin" "$fake_version"
run_zb c5 "$tmp/c5/fake:$tmp/misebin:$tmp/basebin" mise
if [ "$code" -eq 0 ] && [ "$out" = "$pin" ] && ! fake_ran c5; then
  ok 5 "mise, fake zig first on PATH: printed $out"
else
  bad 5 "mise, fake zig first on PATH: exit $code, printed '$out', fake ran: $(cat "$tmp/c5/fake-ran" 2>/dev/null | tr '\n' ' ')stderr: $err"
fi

[ "$failed" -eq 0 ] || exit 1
echo "zb-check: PASS (zb $zb_src, pin $pin, mise $mise_bin)"
