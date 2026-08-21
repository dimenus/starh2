# Sourced by run.sh / mixed.sh. REPO and OUT must already be set.
publish_kestrel() {
  command -v dotnet >/dev/null 2>&1 || {
    echo "dotnet is required for the kestrel arm; it is not installed" >&2
    return 1
  }
  dotnet publish -nologo --verbosity quiet -c Release \
    -o "$OUT/kestrel" \
    "$REPO/tools/sse_bench/kestrel/kestrel.csproj"
}
