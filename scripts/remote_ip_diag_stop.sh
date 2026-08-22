#!/usr/bin/env bash
# 用 PM2 停止并移除 remote_ip_diag。
set -euo pipefail

NAME="remote_ip_diag"

pm2 stop "$NAME" >/dev/null 2>&1 || true
pm2 delete "$NAME" >/dev/null 2>&1 || true
pm2 save >/dev/null 2>&1 || true

echo "stopped and removed $NAME"
