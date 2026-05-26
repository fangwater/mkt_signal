#!/usr/bin/env bash
set -euo pipefail

PROC_NAME="${ORDER_EXPORT_PULLER_PROC:-order_export_puller}"
NAMESPACE="${ORDER_EXPORT_PULLER_NAMESPACE:-order_export_puller}"

echo "[INFO] Stopping ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] Done"
