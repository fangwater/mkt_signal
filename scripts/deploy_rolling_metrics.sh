#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${1:-$HOME/crypto_mkt}"
BIN_NAME="rolling_metrics"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

EXTRA_FILES=(
  "scripts/print_rolling_metrics_thresholds.py"
  "scripts/sync_rolling_metrics_params.py"
  "docs/rolling_metrics.md"
)

for file in "${EXTRA_FILES[@]}"; do
  SRC_PATH="$ROOT_DIR/$file"
  if [[ -f "$SRC_PATH" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC_PATH" "$DEST_DIR/"
  fi
done

echo "[INFO] $BIN_NAME 部署完成。"
