#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/dist}"
TARGET_TRIPLE="${TARGET_TRIPLE:-}"
BIN_PATH="${BIN_PATH:-}"
VERSION="${VERSION:-$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "$ROOT_DIR/Cargo.toml" | head -n1)}"

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

if ! command -v hdiutil >/dev/null 2>&1; then
  echo "hdiutil is required to build a DMG" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

if [[ -z "$TARGET_TRIPLE" ]]; then
  case "$(uname -m)" in
    arm64) TARGET_TRIPLE="aarch64-apple-darwin" ;;
    x86_64) TARGET_TRIPLE="x86_64-apple-darwin" ;;
    *) echo "Unsupported macOS architecture: $(uname -m)" >&2; exit 1 ;;
  esac
fi

if [[ -z "$BIN_PATH" ]]; then
  if [[ -n "$TARGET_TRIPLE" ]]; then
    cargo build --release --target "$TARGET_TRIPLE" --manifest-path "$ROOT_DIR/Cargo.toml"
    BIN_PATH="$ROOT_DIR/target/$TARGET_TRIPLE/release/marrow"
  else
    cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml"
    BIN_PATH="$ROOT_DIR/target/release/marrow"
  fi
fi

if [[ ! -x "$BIN_PATH" ]]; then
  echo "Marrow binary not found at $BIN_PATH" >&2
  exit 1
fi

ARCH_LABEL="$TARGET_TRIPLE"
STAGE_ROOT="$(mktemp -d)"
DMG_ROOT="$(mktemp -d)"
cleanup() {
  rm -rf "$STAGE_ROOT" "$DMG_ROOT"
}
trap cleanup EXIT

export MARROW_UI_APP_STAGE_ROOT="$STAGE_ROOT"
"$BIN_PATH" ui-app enable

APP_PATH="$STAGE_ROOT/Applications/Marrow.app"
if [[ ! -d "$APP_PATH" ]]; then
  echo "Expected staged app bundle at $APP_PATH" >&2
  exit 1
fi

if [[ ! -x "$APP_PATH/Contents/MacOS/marrow" ]]; then
  echo "Expected bundled binary at $APP_PATH/Contents/MacOS/marrow" >&2
  exit 1
fi

if [[ ! -x "$APP_PATH/Contents/MacOS/marrow-launcher" ]]; then
  echo "Expected bundle launcher at $APP_PATH/Contents/MacOS/marrow-launcher" >&2
  exit 1
fi

cp -R "$APP_PATH" "$DMG_ROOT/Marrow.app"
ln -s /Applications "$DMG_ROOT/Applications"

DMG_NAME="Marrow-${VERSION}-${ARCH_LABEL}.dmg"
hdiutil create \
  -volname "Marrow" \
  -srcfolder "$DMG_ROOT" \
  -ov \
  -format UDZO \
  "$OUT_DIR/$DMG_NAME"

echo "$OUT_DIR/$DMG_NAME"
