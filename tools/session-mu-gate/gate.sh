#!/bin/bash
# Gate: `session_mu` and the doorbell have one door each.
#
# Why. The connection actor parks on a select that does NOT include the write
# stash or any flag a handler sets. A handler task tells the actor that shared
# state changed by ringing the doorbell. t-3567 found three handler paths that
# changed state and forgot to ring (a bare flush barrier, an empty-body send,
# a published failure); each was fixed where someone noticed it. t-3594 moved
# the ring into `unlockSession`: every task that is not the actor rings as the
# last act of releasing `session_mu`, so no handler path can change state under
# the lock and forget the wake.
#
# That only holds if nothing else touches the mutex or the doorbell. A direct
# `self.session_mu.unlock(io)` skips the ring, and so does an alias.
#
# WHAT THIS GATE IS, AND IS NOT. It catches an ACCIDENTAL direct use: the shape
# every past hang had (someone calls the mutex or the doorbell directly, or takes
# an alias of it). It does NOT stop a deliberate bypass: a name built at comptime
# outside connection.zig, a pointer to the field obtained through another type,
# or code written to defeat a text scan. Three review rounds each found new
# spellings of that kind, and the next round would find more; a text scan cannot
# close that class. The fix is a type that owns the mutex, so the wrong thing
# cannot be said (t-3600). Do not read a passing gate as proof against an
# adversary.
#
# The gate is an ALLOWLIST that fails toward noise. It reads the text of every
# .zig file in the repo (except zig-pkg/, .zig-cache/, zig-out/ and .git) with
# comments removed, and:
#
#   1. Char literals are skipped first (`'"'` is a char, not a string).
#   2. The DECODED CONTENT of every string literal and quoted identifier
#      (`@"..."`), and the content of every `\\` line string (which starts
#      anywhere a token can, not only at the start of a line), may not contain
#      `session_mu` or `doorbell`. `\xNN`, `\u{...}` and the one-character
#      escapes are decoded first, so `@"session_\x6du"` is caught; a literal
#      that does not decode is a failure. A name hidden in a string is how
#      `@field(self, "session_mu")` or `@fieldParentPtr` naming a field would get
#      through, and this rule covers both.
#   3. `session_mu` as a token may appear only in five lines of
#      src/edge/connection.zig, matched as EXACT trimmed text, and each text must
#      occur exactly as often as listed: the field declaration (once), the lock
#      call (once), the `lockUncancelable` call (once or twice: its two branches,
#      or one after a refactor) and the unlock call (once). Each line must also sit
#      in its wrapper: the nearest `fn` declaration line above it (with any of
#      pub, inline, noinline, export, extern before the `fn`) must name
#      `lockSession`, `lockSessionUncancelable`, `unlockSession` or (for the
#      doorbell send) `ring`, and the code line after the unlock must be the
#      `actor_marker` check. A helper extracted from a wrapper, or a second function
#      with the same name, adds a copy or puts the line under another name.
#   4. `doorbell` as a token may appear only in these forms. In connection.zig,
#      as exact trimmed lines, each exactly once: the field declaration, its
#      `.init` line, and the one send in `ring` (the send must sit in `ring`). Anywhere, as a whole call:
#      `<path>.doorbell.asyncReceive()`, `io_queue.chanLen(u8, &<path>.doorbell)`
#      and `io_queue.tryRecv(u8, &<path>.doorbell)` (a select arm, a length read,
#      taking a token: none can send). A call is matched with an identifier
#      boundary before it, and NOTHING may chain off it:
#        (a) the first thing before the call's first identifier must be the start
#            of the line (and then the previous code line must not end in `.`),
#            or `(`, `,` or `=`; so `alias. io_queue.chanLen(...)`, with or
#            without a space or a newline after the dot, fails;
#        (b) the next non-space character after its closing `)` must not be `.`,
#            on this line or at the start of the next one; so
#            `io_queue.tryRecv(u8, &self.doorbell).trySend(0)` and
#            `self.doorbell.asyncReceive().impl` fail.
#      Any other `doorbell` token fails: aliases (`const ch = self.doorbell;`),
#      `&(self.doorbell)`, a name split over two lines.
#   5. `@field` is not used in src/edge/connection.zig, with or without a space
#      before the `(` (a computed field name cannot be read by this gate).
#
# The matching is on exact ONE-LINE text, by design: it fails toward noise. If a
# formatter or a refactor splits an allowed call over lines, keep it on one line
# with no trailing comma (zig fmt keeps a call on one line when it has none).
#
# The scan is REPORTED, an empty scan is fatal, and the allowed uses are counted
# against a floor, so a renamed wrapper or a blind scan fails and does not pass.
set -uo pipefail
ROOT=${1:-.}

[ -d "$ROOT/src" ] || { echo "FATAL: no src/ under $ROOT"; exit 3; }

LIST=$(mktemp)
OUT=$(mktemp)
find "$ROOT" \( -path "$ROOT/zig-pkg" -o -path "$ROOT/.zig-cache" -o -path "$ROOT/zig-out" -o -path "$ROOT/.git" \) -prune -o -name '*.zig' -type f -print | sort > "$LIST"
NFILES=$(wc -l < "$LIST" | tr -d ' ')
[ "$NFILES" -gt 0 ] || { echo "FATAL: scanned zero files"; rm -f "$LIST" "$OUT"; exit 3; }
echo "session-mu-gate: scanned $NFILES files under $ROOT"

while read -r f; do
  awk -v file="$f" '
    function trim(s) { sub(/^[ \t]+/, "", s); sub(/[ \t]+$/, "", s); return s }
    function spaces(n,   s) { s = ""; while (n-- > 0) s = s " "; return s }
    function has_token(s, tok) { return match(s, "(^|[^A-Za-z_0-9])" tok "([^A-Za-z_0-9]|$)") }
    function hexval(h,   v, k, d) {
      if (length(h) == 0) return -1
      v = 0
      for (k = 1; k <= length(h); k++) {
        d = index("0123456789abcdef", tolower(substr(h, k, 1)))
        if (d == 0) return -1
        v = v * 16 + d - 1
      }
      return v
    }
    # The value of a string literal or quoted identifier, as the compiler reads
    # it: \xNN, \u{...} and the one-character escapes are decoded. A character
    # that is not a name character becomes "?". Sets bad=1 if it does not decode.
    function decode(lit,   out, i, n, c, e, h, v, cl) {
      out = ""; n = length(lit); i = 1; bad = 0
      while (i <= n) {
        c = substr(lit, i, 1)
        if (c != "\\") { out = out c; i++; continue }
        e = substr(lit, i + 1, 1)
        if (e == "n" || e == "r" || e == "t" || e == "\\" || e == "\047" || e == "\"") { out = out "?"; i += 2; continue }
        if (e == "x") {
          h = substr(lit, i + 2, 2); v = hexval(h)
          if (length(h) < 2 || v < 0) { bad = 1; return out }
          out = out ((v >= 32 && v < 127) ? sprintf("%c", v) : "?"); i += 4; continue
        }
        if (e == "u" && substr(lit, i + 2, 1) == "{") {
          cl = index(substr(lit, i + 3), "}")
          if (cl == 0) { bad = 1; return out }
          h = substr(lit, i + 3, cl - 1); v = hexval(h)
          if (v < 0) { bad = 1; return out }
          out = out ((v >= 32 && v < 127) ? sprintf("%c", v) : "?"); i += 3 + cl; continue
        }
        bad = 1; return out
      }
      return out
    }
    function check_literal(lit, what, decode_it,   val) {
      val = lit
      if (decode_it) {
        val = decode(lit)
        if (bad) { print "FAIL " file ":" NR ": a " what " does not decode (an escape the gate cannot read): " $0; return }
      }
      if (index(val, "session_mu") || index(val, "doorbell")) {
        print "FAIL " file ":" NR ": a " what " names session_mu or doorbell, spelled or by escapes (reword it): " $0
      }
    }
    # Record every whole qualified call that matches `core`, with an identifier
    # boundary before it (not a substring of a longer name). The span of each is
    # kept in cs[]/ce[]: from its first identifier to its closing paren.
    function gather(line, core,   off, st, ln, pl) {
      off = 1
      while (match(substr(line, off), "(^|[^A-Za-z_0-9.])" core)) {
        st = off + RSTART - 1; ln = RLENGTH
        pl = (substr(line, st, 1) ~ /[A-Za-z_]/) ? 0 : 1
        nspan++
        cs[nspan] = st + pl; ce[nspan] = st + ln - 1
        off = st + ln
      }
    }
    function prev_code(ln,   k) {
      for (k = ln - 1; k >= 1; k--) if (trim(code[k]) != "") return trim(code[k])
      return ""
    }
    # The name of the nearest `fn` declaration line above line ln, with any of
    # pub, inline, noinline, export, extern before it. "" if there is none.
    function enclosing_fn(ln,   k, nm) {
      for (k = ln - 1; k >= 1; k--) {
        if (code[k] ~ /^[ \t]*((pub|inline|noinline|export|extern)[ \t]+)*fn[ \t]+[A-Za-z_][A-Za-z_0-9]*/) {
          nm = code[k]
          sub(/^[ \t]*((pub|inline|noinline|export|extern)[ \t]+)*fn[ \t]+/, "", nm)
          sub(/[^A-Za-z_0-9].*$/, "", nm)
          return nm
        }
      }
      return ""
    }
    function where_ok(ln, want,   got) {
      got = enclosing_fn(ln)
      if (got != want) print "FAIL " file ":" ln ": this allowed line must sit in " want ", but the nearest fn above it is " (got == "" ? "(none)" : got) ": " src[ln]
    }
    function next_code(ln,   k) {
      for (k = ln + 1; k <= NR; k++) if (trim(code[k]) != "") return trim(code[k])
      return ""
    }
    {
      raw = $0
      c0 = ""
      n = length(raw); i = 1
      while (i <= n) {
        c = substr(raw, i, 1)
        if (c == "/" && substr(raw, i, 2) == "//") break
        if (c == "\\" && substr(raw, i, 2) == "\\\\") {
          check_literal(substr(raw, i + 2), "line string", 0)
          break
        }
        if (c == "\047") {
          j = i + 1
          while (j <= n) {
            cj = substr(raw, j, 1)
            if (cj == "\\") { j += 2; continue }
            if (cj == "\047") break
            j++
          }
          c0 = c0 "\047\047"
          i = j + 1
          continue
        }
        if (c == "\"" || (c == "@" && substr(raw, i + 1, 1) == "\"")) {
          quoted_ident = (c == "@")
          start = quoted_ident ? i + 2 : i + 1
          j = start
          while (j <= n) {
            cj = substr(raw, j, 1)
            if (cj == "\\") { j += 2; continue }
            if (cj == "\"") break
            j++
          }
          check_literal(substr(raw, start, j - start), quoted_ident ? "quoted identifier" : "string literal", 1)
          c0 = c0 (quoted_ident ? "@\"\"" : "\"\"")
          i = j + 1
          continue
        }
        c0 = c0 c
        i++
      }
      code[NR] = c0
      src[NR] = raw
    }
    END {
      conn = (file ~ /src\/edge\/connection\.zig$/)
      T_DECL = "session_mu: std.Io.Mutex = .init,"
      T_LOCK = "self.session_mu.lock(self.config.io) catch return error.Canceled;"
      T_LOCKU = "self.session_mu.lockUncancelable(io);"
      T_UNLOCK = "self.session_mu.unlock(io);"
      B_DECL = "doorbell: zio.Channel(u8) = undefined,"
      B_INIT = "self.doorbell = .init(&self.doorbell_buf);"
      B_SEND = "self.doorbell.trySend(0) catch |err| switch (err) {"
      HINT = " [the gate matches exact one-line text: keep this call on one line with no trailing comma; zig fmt keeps a call on one line when it has no trailing comma]"
      for (ln = 1; ln <= NR; ln++) {
        c = code[ln]; t = trim(c); raw = src[ln]

        if (has_token(c, "session_mu")) {
          if (conn && t == T_DECL) { n_decl++; print "ALLOWED mu " file ":" ln }
          else if (conn && t == T_LOCK) { n_lock++; where_ok(ln, "lockSession"); print "ALLOWED mu " file ":" ln }
          else if (conn && t == T_LOCKU) { n_locku++; where_ok(ln, "lockSessionUncancelable"); print "ALLOWED mu " file ":" ln }
          else if (conn && t == T_UNLOCK) {
            n_unlock++; where_ok(ln, "unlockSession")
            if (trim(next_code(ln)) != "if (actor_marker.get() != self) {") print "FAIL " file ":" ln ": the line after the session_mu unlock is not the actor_marker check: " raw
            print "ALLOWED mu " file ":" ln
          }
          else print "FAIL " file ":" ln ": session_mu outside its five exact lines: " raw HINT
        }

        if (has_token(c, "doorbell")) {
          rest = c
          used = 0
          nspan = 0
          if (conn && t == B_DECL) { n_bdecl++; rest = ""; used = 1 }
          else if (conn && t == B_INIT) { n_binit++; rest = ""; used = 1 }
          else if (conn && t == B_SEND) { n_send++; rest = ""; where_ok(ln, "ring"); print "ALLOWED bell-send " file ":" ln }
          else {
            gather(c, "[A-Za-z_][A-Za-z_0-9.]*\\.doorbell\\.asyncReceive\\(\\)")
            gather(c, "io_queue\\.chanLen\\(u8, &[A-Za-z_][A-Za-z_0-9.]*\\.doorbell\\)")
            gather(c, "io_queue\\.tryRecv\\(u8, &[A-Za-z_][A-Za-z_0-9.]*\\.doorbell\\)")
            for (q = 1; q <= nspan; q++) {
              used = 1
              pre = substr(c, 1, cs[q] - 1); sub(/[ \t]+$/, "", pre)
              if (pre == "") {
                if (prev_code(ln) ~ /\.$/) print "FAIL " file ":" ln ": a doorbell call chained off the line before it: " raw HINT
              } else if (substr(pre, length(pre), 1) !~ /[(,=]/) {
                print "FAIL " file ":" ln ": a doorbell call preceded by something other than (, , or =: " raw HINT
              }
              sfx = substr(c, ce[q] + 1); sub(/^[ \t]+/, "", sfx)
              if (sfx == "") sfx = next_code(ln)
              if (substr(sfx, 1, 1) == ".") print "FAIL " file ":" ln ": something chains off a doorbell call: " raw HINT
              rest = substr(rest, 1, cs[q] - 1) spaces(ce[q] - cs[q] + 1) substr(rest, ce[q] + 1)
            }
          }
          if (has_token(rest, "doorbell")) print "FAIL " file ":" ln ": doorbell outside its allowed forms: " raw HINT
          else if (used) print "ALLOWED bell-use " file ":" ln
        }

        if (conn && c ~ /@field([^A-Za-z_0-9]|$)/) print "FAIL " file ":" ln ": @field in connection.zig (a computed name hides session_mu): " raw
      }
      if (conn) {
        if (n_decl != 1) print "FAIL " file ": the session_mu field declaration occurs " n_decl+0 " times, expected 1"
        if (n_lock != 1) print "FAIL " file ": the session_mu lock line occurs " n_lock+0 " times, expected 1"
        if (n_locku != 1 && n_locku != 2) print "FAIL " file ": the session_mu lockUncancelable line occurs " n_locku+0 " times, expected 1 or 2"
        if (n_unlock != 1) print "FAIL " file ": the session_mu unlock line occurs " n_unlock+0 " times, expected 1"
        if (n_bdecl != 1) print "FAIL " file ": the doorbell field declaration occurs " n_bdecl+0 " times, expected 1"
        if (n_binit != 1) print "FAIL " file ": the doorbell init line occurs " n_binit+0 " times, expected 1"
        if (n_send != 1) print "FAIL " file ": the doorbell send line occurs " n_send+0 " times, expected 1"
      }
    }' "$f"
done < "$LIST" > "$OUT"

MU=$(grep -c '^ALLOWED mu ' "$OUT")
SEND=$(grep -c '^ALLOWED bell-send ' "$OUT")
USE=$(grep -c '^ALLOWED bell-use ' "$OUT")
echo "session-mu-gate: allowed session_mu lines: $MU (floor 4); doorbell send: $SEND (floor 1); other doorbell uses: $USE (floor 4)"

RC=0
if grep '^FAIL ' "$OUT"; then
  echo "session-mu-gate: FAIL. Take the lock with lockSession / lockSessionUncancelable and release it with"
  echo "unlockSession (it rings the actor). Ring with ring(). See the comment on unlockSession."
  RC=1
fi
if [ "$MU" -lt 4 ] || [ "$SEND" -lt 1 ] || [ "$USE" -lt 4 ]; then
  echo "session-mu-gate: FAIL. Fewer allowed uses than the floor: a wrapper was renamed or the scan is blind."
  RC=1
fi
rm -f "$LIST" "$OUT"
exit $RC
