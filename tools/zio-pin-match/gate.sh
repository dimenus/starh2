#!/bin/bash
# Gate: every build.zig.zon of ours that pins zio pins the same zio.
#
#   gate.sh <repo-root>
#
# build.zig runs it as `./zb build zio-pin-match`, and `ci` depends on it.
#
# Why. zio is pinned in more than one build root: build.zig.zon, and
# tools/zio-dst/build.zig.zon, which is its own package (it declares the sim
# line in its root) and so cannot borrow the root's dependency. A zon file
# cannot include another, so the second pin is a copy, and every zio pin move
# must edit both. This gate is what makes a missed copy fail.
#
# The files are tools/our-files.sh's set, named build.zig.zon. In each, the
# `.zio = .{ ... }` dependency is read as text (full-line `//` comments are
# skipped), and it must be one of:
#   - a pin: exactly one `.url` and one `.hash`. Both must equal the root
#     build.zig.zon's, character for character.
#   - a local path: exactly one `.path` and no url or hash. That points at a
#     checkout on the machine (the upstream repro tools under tools/ do this
#     on purpose), so there is no pin to compare; the gate lists it.
# The root build.zig.zon must hold a pin.
#
# It fails toward noise. Exit 2, naming the file, when: a file has more than
# one `.zio` key; a `.zio` entry is neither shape above; a file's code
# mentions a zio url or a zio hash (`/zio#`, `/zio.git`, `"zio-`) but no
# `.zio` key was found; the root has no pin; or no build.zig.zon was found.
# A parse that finds nothing must not pass. Exit 1, naming both files and
# both values, when a pin differs from the root's.
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: gate.sh <repo-root>" >&2
  exit 2
fi
here=$(cd "$(dirname "$0")/.." && pwd)
cd "$1"

mode=$(bash "$here/our-files.sh" --mode .)
if ! listed=$(bash "$here/our-files.sh" . build.zig.zon); then
  echo "zio-pin-match FATAL: tools/our-files.sh failed" >&2
  exit 2
fi

# One line per file, fields split by 0x1f (tab is whitespace to `read`, and
# runs of it would merge the empty fields):
#   keys url_count hash_count path_count mentions url hash path
scan() {
  awk '
    /^[ \t]*\/\// { next }
    {
      if ($0 ~ /\/zio#|\/zio\.git|"zio-/) mentions++
      # Every `.zio =` or `.@"zio" =` key on the line counts, wherever it
      # starts; `.zio_dst` and `.name = .zio,` are not keys.
      line = $0
      rest = $0
      while (match(rest, /(^|[^A-Za-z0-9_])\.(zio|@"zio")[ \t]*=/)) {
        keys++
        if (keys == 1) { line = substr(rest, RSTART); inblock = 1; depth = 0; started = 0 }
        rest = substr(rest, RSTART + RLENGTH)
      }
      if (inblock) {
        text = text line "\n"
        n = split(line, ch, "")
        for (i = 1; i <= n; i++) {
          if (ch[i] == "{") { depth++; started = 1 }
          else if (ch[i] == "}") depth--
        }
        if (started && depth <= 0) inblock = 0
      }
    }
    function field(name,    re, s, v) {
      re = "\\." name "[ \t]*=[ \t]*\"[^\"]*\""
      s = text; v = ""; cnt[name] = 0
      while (match(s, re)) {
        cnt[name]++
        v = substr(s, RSTART, RLENGTH)
        sub(/^[^"]*"/, "", v); sub(/"$/, "", v)
        s = substr(s, RSTART + RLENGTH)
      }
      return v
    }
    END {
      u = field("url"); h = field("hash"); p = field("path")
      printf "%d\037%d\037%d\037%d\037%d\037%s\037%s\037%s\n", keys, cnt["url"], cnt["hash"], cnt["path"], mentions, u, h, p
    }
  ' "$1"
}

files=()
while IFS= read -r f; do
  [ -n "$f" ] || continue
  files+=("$f")
done <<EOF
$listed
EOF
if [ ${#files[@]} -eq 0 ]; then
  echo "zio-pin-match FATAL: no build.zig.zon found (set: $mode)" >&2
  exit 2
fi

bad=0
root_url=""
root_hash=""
pins=()
paths=()
for f in "${files[@]}"; do
  IFS=$'\037' read -r keys nurl nhash npath mentions url hash path <<<"$(scan "$f")" || true
  if [ "$keys" -gt 1 ]; then
    echo "zio-pin-match FATAL: $f has $keys .zio keys; cannot tell which is the dependency" >&2
    exit 2
  fi
  if [ "$keys" -eq 0 ]; then
    if [ "$mentions" -gt 0 ]; then
      echo "zio-pin-match FATAL: $f mentions a zio url or hash in $mentions line(s) but no .zio key was found; the gate cannot read it" >&2
      exit 2
    fi
    if [ "$f" = build.zig.zon ]; then
      echo "zio-pin-match FATAL: the root build.zig.zon has no .zio dependency" >&2
      exit 2
    fi
    continue
  fi
  if [ "$nurl" -eq 1 ] && [ "$nhash" -eq 1 ] && [ "$npath" -eq 0 ]; then
    if [ "$f" = build.zig.zon ]; then
      root_url=$url
      root_hash=$hash
    fi
    pins+=("$f"$'\037'"$url"$'\037'"$hash")
  elif [ "$npath" -eq 1 ] && [ "$nurl" -eq 0 ] && [ "$nhash" -eq 0 ]; then
    paths+=("$f ($path)")
  else
    echo "zio-pin-match FATAL: $f: the .zio entry has $nurl .url, $nhash .hash and $npath .path; want one url and one hash, or one path" >&2
    exit 2
  fi
done

if [ -z "$root_url" ]; then
  echo "zio-pin-match FATAL: the root build.zig.zon does not pin zio by url and hash" >&2
  exit 2
fi

for entry in "${pins[@]}"; do
  IFS=$'\037' read -r f url hash <<<"$entry"
  [ "$f" = build.zig.zon ] && continue
  if [ "$url" != "$root_url" ] || [ "$hash" != "$root_hash" ]; then
    bad=1
    echo "zio-pin-match: $f pins a different zio from build.zig.zon:" >&2
    [ "$url" = "$root_url" ] || printf '  url   build.zig.zon: %s\n        %s: %s\n' "$root_url" "$f" "$url" >&2
    [ "$hash" = "$root_hash" ] || printf '  hash  build.zig.zon: %s\n        %s: %s\n' "$root_hash" "$f" "$hash" >&2
  fi
done

if [ "$bad" -ne 0 ]; then
  echo "zio-pin-match: move every zio pin together (set: $mode)" >&2
  exit 1
fi
echo "zio-pin-match: ${#files[@]} build.zig.zon files; ${#pins[@]} pin zio, all at $root_url ($root_hash); ${#paths[@]} use a local path, not compared: ${paths[*]:-none}; set: $mode"
