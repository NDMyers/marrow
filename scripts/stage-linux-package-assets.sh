#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TARGET_TRIPLE="${TARGET_TRIPLE:-x86_64-unknown-linux-gnu}"
BIN_PATH="${BIN_PATH:-}"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/target/package-assets/linux}"
DESKTOP_EXEC="${DESKTOP_EXEC:-/usr/bin/marrow ui-app open}"

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
    --desktop-exec)
      DESKTOP_EXEC="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$BIN_PATH" ]]; then
  cargo build --release --target "$TARGET_TRIPLE" --manifest-path "$ROOT_DIR/Cargo.toml"
  BIN_PATH="$ROOT_DIR/target/$TARGET_TRIPLE/release/marrow"
fi

if [[ ! -x "$BIN_PATH" ]]; then
  echo "Marrow binary not found at $BIN_PATH" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
STAGE_ROOT="$(mktemp -d)"
cleanup() {
  rm -rf "$STAGE_ROOT"
}
trap cleanup EXIT

MARROW_UI_APP_STAGE_ROOT="$STAGE_ROOT" MARROW_DESKTOP_EXEC="$DESKTOP_EXEC" \
  "$BIN_PATH" ui-app enable >/dev/null

cp "$STAGE_ROOT/.local/share/applications/marrow.desktop" "$OUT_DIR/marrow.desktop"
cp "$STAGE_ROOT/.local/share/icons/hicolor/256x256/apps/marrow.png" "$OUT_DIR/marrow.png"

echo "$OUT_DIR"
