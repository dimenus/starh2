#!/usr/bin/env bash
# Run ONE cross-vendor arm against ONE corpus commit.
#
#   run-cursor.sh <arm-name> <cursor-model> <commit>
#
# PROTOCOL
# `--mode ask` is the review shape: read-only, so the arm cannot edit the corpus,
# but it DOES read files, which is what this needs. The cwd is the corpus commit
# directory, and the cwd is the only thing pointing the arm at a tree.
#
# The prompt goes on STDIN and is a disposable pointer to brief.md, which is
# committed. Losing the prompt costs nothing; losing the brief would cost the run.
#
# Records, per run, the three numbers that say whether the instrument fired:
# wall seconds, raw output bytes, and the arm's own files_read count. A clean
# zero at a fraction of the usual cost is a run that did not happen.
set -euo pipefail

ARM="${1:?arm name}"
MODEL="${2:?cursor model id}"
COMMIT="${3:?corpus commit}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
BIN="${CURSOR_BIN:-$HOME/.local/bin/cursor-agent-personal}"
CDIR="$HERE/corpus/$COMMIT"
ODIR="$HERE/runs/$ARM/$COMMIT"

[ -d "$CDIR/tree" ] || { echo "FATAL: no corpus at $CDIR" >&2; exit 2; }
mkdir -p "$ODIR"

# Auth: `status` prints a success line even when the session is dead. A live
# session NAMES the account, so that is what this greps for.
"$BIN" status 2>&1 | grep -q '^✓ Logged in as ' || {
  echo "FATAL: $BIN is not logged in. Run: $BIN login" >&2; exit 2; }

cat > "$ODIR/prompt.txt" <<EOF
Read the review brief at $HERE/brief.md and follow it exactly.

Your working directory is $CDIR. It holds tree/, diff.patch and subject.txt.
Review the commit in diff.patch against the tree at $CDIR/tree.

Do not read anything under $HERE other than brief.md. In particular do not open
findings.json, runs/, or any file whose name mentions findings or recall: those
hold the answer key for this exercise and reading one voids the run.
EOF

start=$(date +%s)
set +e
( cd "$CDIR" && "$BIN" --print --output-format text --mode ask --trust --model "$MODEL" \
    < "$ODIR/prompt.txt" > "$ODIR/raw.txt" 2> "$ODIR/err.txt" )
rc=$?
set -e
end=$(date +%s)

bytes=$(wc -c < "$ODIR/raw.txt" | tr -d ' ')
secs=$((end - start))

parsed=0
if python3 "$HERE/parse_arm.py" "$ODIR/raw.txt" "$ODIR/report.json" > "$ODIR/parse.txt" 2>&1; then
  parsed=1
fi

python3 - "$ODIR" "$ARM" "$MODEL" "$COMMIT" "$rc" "$secs" "$bytes" "$parsed" <<'PY'
import json, os, sys
odir, arm, model, commit, rc, secs, byt, parsed = sys.argv[1:9]
meta = {"arm": arm, "vendor_model": model, "commit": commit,
        "exit_code": int(rc), "wall_seconds": int(secs),
        "raw_bytes": int(byt), "parsed": bool(int(parsed))}
rp = os.path.join(odir, "report.json")
if meta["parsed"]:
    r = json.load(open(rp))
    meta["n_findings"] = len(r["findings"])
    meta["n_files_read"] = len(r["files_read"])
    meta["network_used"] = r["network_used"]
json.dump(meta, open(os.path.join(odir, "meta.json"), "w"), indent=2)
print(json.dumps(meta))
PY

[ "$parsed" = 1 ] || { echo "FATAL: $ARM/$COMMIT produced no parseable report; see $ODIR/parse.txt" >&2; exit 5; }
