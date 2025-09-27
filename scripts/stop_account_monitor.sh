#!/usr/bin/env bash
set -euo pipefail

PM2_NAME="account_monitor"

if npx pm2 describe "$PM2_NAME" >/dev/null 2>&1; then
  echo "[INFO] 停止 $PM2_NAME"
  npx pm2 delete "$PM2_NAME"
else
  echo "[WARN] 未找到 PM2 进程 $PM2_NAME"
fi
