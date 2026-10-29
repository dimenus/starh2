# Sourced by run.sh / mixed.sh. REPO and OUT must already be set.
#
# The published sse-kestrel is a framework-dependent apphost. It looks for
# .NET in DOTNET_ROOT, else in a fixed system location, which on nachos is
# /usr/share/dotnet with no ASP.NET runtime. So DOTNET_ROOT is exported as
# the install that `dotnet` on PATH uses, the one that published the app.
# A DOTNET_ROOT the caller already set is kept.
publish_kestrel() {
  command -v dotnet >/dev/null 2>&1 || {
    echo "dotnet is required for the kestrel arm; it is not installed" >&2
    return 1
  }
  if [ -z "${DOTNET_ROOT:-}" ]; then
    aspnet_dir=$(dotnet --list-runtimes | awk '$1 == "Microsoft.AspNetCore.App" { d = $3 } END { print d }')
    aspnet_dir=${aspnet_dir#\[}
    aspnet_dir=${aspnet_dir%\]}
    case $aspnet_dir in
      */shared/Microsoft.AspNetCore.App) ;;
      *)
        echo "the dotnet on PATH ($(command -v dotnet)) has no Microsoft.AspNetCore.App runtime; the kestrel arm needs one" >&2
        return 1
        ;;
    esac
    DOTNET_ROOT=${aspnet_dir%/shared/Microsoft.AspNetCore.App}
    export DOTNET_ROOT
  fi
  echo "kestrel: DOTNET_ROOT=$DOTNET_ROOT" >&2
  dotnet publish -nologo --verbosity quiet -c Release \
    -o "$OUT/kestrel" \
    "$REPO/tools/sse_bench/kestrel/kestrel.csproj"
}
