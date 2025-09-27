#!/usr/bin/env bash
set -euo pipefail

PM2_NAME_PREFIX="mkt_pub_"

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <exchange1> [exchange2 ...]"
  echo "示例: $0 binance okex"
  exit 1
fi

for exchange in "$@"; do
  name="${PM2_NAME_PREFIX}${exchange}"
  if pm2 describe "$name" >/dev/null 2>&1; then
    echo "[INFO] 停止 $name"
    pm2 delete "$name"
  else
    echo "[WARN] 未找到 PM2 进程 $name"
  fi
 done
