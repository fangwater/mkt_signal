#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${1:-/home/ubuntu/crypto_mkt}"
CONFIG_SRC="$ROOT_DIR/config"
DASHBOARD_SRC="$ROOT_DIR/dashboard"

echo "[INFO] 部署 viz 相关配置与静态资源到 $TARGET_DIR"

if [[ ! -d "$CONFIG_SRC" ]]; then
  echo "[ERROR] 未找到目录: $CONFIG_SRC" >&2
  exit 1
fi

if [[ ! -d "$DASHBOARD_SRC" ]]; then
  echo "[ERROR] 未找到目录: $DASHBOARD_SRC" >&2
  exit 1
fi

mkdir -p "$TARGET_DIR/config"
echo "[INFO] 同步配置文件到 $TARGET_DIR/config"
cp -a "$CONFIG_SRC/." "$TARGET_DIR/config/"

mkdir -p "$TARGET_DIR/dashboard"
echo "[INFO] 同步 dashboard 静态资源到 $TARGET_DIR/dashboard"
cp -a "$DASHBOARD_SRC/." "$TARGET_DIR/dashboard/"

echo "[INFO] viz 资源部署完成。"
