#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${1:-/home/ubuntu/crypto_mkt}"
BIN_NAME="viz_server"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
CONFIG_SRC="$ROOT_DIR/config/viz.toml"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

if [[ -f "$CONFIG_SRC" ]]; then
  mkdir -p "$TARGET_DIR/config"
  cp "$CONFIG_SRC" "$TARGET_DIR/config/"
fi

echo "[INFO] $BIN_NAME 部署完成。"
