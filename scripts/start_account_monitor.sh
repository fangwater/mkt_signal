#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_PATH="${DIR}/account_monitor"
PM2_NAME="account_monitor"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "[ERROR] 未找到可执行文件 $BIN_PATH"
  exit 1
fi

echo "[INFO] 使用 PM2 启动 $PM2_NAME"
npx pm2 delete "$PM2_NAME" 2>/dev/null
npx pm2 start "$BIN_PATH" --name "$PM2_NAME"

echo "[INFO] $PM2_NAME 已启动"
