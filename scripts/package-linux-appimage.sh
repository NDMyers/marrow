#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TARGET_TRIPLE="${TARGET_TRIPLE:-x86_64-unknown-linux-gnu}"
ARCH_LABEL="${ARCH_LABEL:-x86_64}"
BIN_PATH="${BIN_PATH:-}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/dist}"
VERSION="${VERSION:-$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "$ROOT_DIR/Cargo.toml" | head -n1)}"
APPIMAGETOOL_BIN="${APPIMAGETOOL_BIN:-$(command -v appimagetool || true)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET_TRIPLE="$2"
      shift 2
      ;;
    --bin)
      BIN_PATH="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "$TARGET_TRIPLE" != "x86_64-unknown-linux-gnu" ]]; then
  echo "AppImage packaging currently supports x86_64-unknown-linux-gnu only" >&2
  exit 1
fi

if [[ -z "$APPIMAGETOOL_BIN" ]]; then
  echo "appimagetool is required to build an AppImage" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

if [[ -z "$BIN_PATH" ]]; then
  cargo build --release --target "$TARGET_TRIPLE" --manifest-path "$ROOT_DIR/Cargo.toml"
  BIN_PATH="$ROOT_DIR/target/$TARGET_TRIPLE/release/marrow"
fi

if [[ ! -x "$BIN_PATH" ]]; then
  echo "Marrow binary not found at $BIN_PATH" >&2
  exit 1
fi

ASSET_DIR="$($ROOT_DIR/scripts/stage-linux-package-assets.sh --target "$TARGET_TRIPLE" --bin "$BIN_PATH" --desktop-exec "AppRun")"
APPDIR="$(mktemp -d)/Marrow.AppDir"
cleanup() {
  rm -rf "$APPDIR"
}
trap cleanup EXIT

mkdir -p "$APPDIR/usr/bin" "$APPDIR/usr/share/applications" "$APPDIR/usr/share/icons/hicolor/256x256/apps"
cp "$BIN_PATH" "$APPDIR/usr/bin/marrow"
cp "$ASSET_DIR/marrow.desktop" "$APPDIR/usr/share/applications/marrow.desktop"
cp "$ASSET_DIR/marrow.png" "$APPDIR/usr/share/icons/hicolor/256x256/apps/marrow.png"
cp "$ASSET_DIR/marrow.desktop" "$APPDIR/marrow.desktop"
cp "$ASSET_DIR/marrow.png" "$APPDIR/marrow.png"
cat > "$APPDIR/AppRun" <<'EOF'
#!/usr/bin/env bash
HERE="$(cd "$(dirname "$0")" && pwd)"
exec "$HERE/usr/bin/marrow" ui-app open "$@"
EOF
chmod +x "$APPDIR/AppRun"

OUTPUT_PATH="$OUT_DIR/Marrow-${VERSION}-${ARCH_LABEL}.AppImage"
ARCH="$ARCH_LABEL" "$APPIMAGETOOL_BIN" "$APPDIR" "$OUTPUT_PATH"

echo "$OUTPUT_PATH"
