#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"
APP_NAME="${PM2_NAME:-okx_rate_cache_${NAMESPACE}}"

echo "[INFO] 停止 okx_rate_cache (name=${APP_NAME}, namespace=${NAMESPACE})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] 已停止。"
