#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${1:-/home/ubuntu/crypto_mkt}"
BIN_NAME="pre_trade"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
DEST_DIR="$TARGET_DIR/bin"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $DEST_DIR"
mkdir -p "$DEST_DIR"
cp "$BIN_PATH" "$DEST_DIR/"
chmod +x "$DEST_DIR/$BIN_NAME"

echo "[INFO] $BIN_NAME 部署完成。"
