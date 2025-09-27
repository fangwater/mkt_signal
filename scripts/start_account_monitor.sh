#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_PATH="${DIR}/account_monitor"
ENV_FILE="${DIR}/env.sh"
PM2_NAME="account_monitor"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "[ERROR] 未找到可执行文件 $BIN_PATH"
  exit 1
fi

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

echo "[INFO] 使用 PM2 启动 $PM2_NAME"
pm2 delete "$PM2_NAME" 2>/dev/null
pm2 start "$BIN_PATH" --name "$PM2_NAME"

echo "[INFO] $PM2_NAME 已启动"
