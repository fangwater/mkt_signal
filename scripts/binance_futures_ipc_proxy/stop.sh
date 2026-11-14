#!/usr/bin/env bash
set -euo pipefail

NAME="binance_futures_ipc_proxy"
if command -v pm2 >/dev/null 2>&1; then
  PM2=(pm2)
else
  PM2=(npx pm2)
fi
"${PM2[@]}" delete "$NAME" --namespace "$NAME"
