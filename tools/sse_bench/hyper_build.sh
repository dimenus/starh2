# Sourced by run.sh / mixed.sh. REPO and OUT must already be set.
# Builds the hyper arm into $OUT/hyper/sse-hyper. The target dir lives under
# $OUT so a stale debug binary in the tree can never be the one measured.
build_hyper() {
  command -v cargo >/dev/null 2>&1 || {
    echo "cargo is required for the hyper arm; it is not installed" >&2
    return 1
  }
  cargo build --quiet --release --locked \
    --manifest-path "$REPO/tools/sse_bench/hyper/Cargo.toml" \
    --target-dir "$OUT/hyper-target" || return 1
  mkdir -p "$OUT/hyper"
  cp "$OUT/hyper-target/release/sse-hyper" "$OUT/hyper/sse-hyper"
}
