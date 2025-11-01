#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${1:-/home/ubuntu/crypto_mkt}"
BIN_NAME="persist_manager"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
CONFIG_SRC="$ROOT_DIR/config/persist_manager.toml"

echo "[INFO] 构建 $BIN_NAME (release)"
cd "$ROOT_DIR"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 确保部署目录 $TARGET_DIR 存在"
mkdir -p "$TARGET_DIR"

echo "[INFO] 复制可执行文件到 $TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

if [[ -f "$CONFIG_SRC" ]]; then
  echo "[INFO] 同步配置文件到 $TARGET_DIR/config/"
  mkdir -p "$TARGET_DIR/config"
  rsync -a "$CONFIG_SRC" "$TARGET_DIR/config/"
else
  echo "[WARN] 未找到配置文件 $CONFIG_SRC，跳过复制"
fi

echo "[INFO] $BIN_NAME 部署完成。"
