#!/usr/bin/env bash
set -euo pipefail

PROC_NAME="${ORDER_EXPORT_PULLER_PROC:-order_export_puller}"
NAMESPACE="${ORDER_EXPORT_PULLER_NAMESPACE:-order_export_puller}"

if [[ -n "${PM2_BIN:-}" ]]; then
  PM2_CMD=( $PM2_BIN )
elif command -v pm2 >/dev/null 2>&1; then
  PM2_CMD=( pm2 )
else
  PM2_CMD=( npx pm2 )
fi

echo "[INFO] Stopping ${PROC_NAME} (namespace=${NAMESPACE})"
"${PM2_CMD[@]}" delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] Done"
