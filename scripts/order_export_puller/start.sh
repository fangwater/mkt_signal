#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/order_export_puller.py"
CONFIG="${ORDER_EXPORT_PULLER_CONFIG:-${SCRIPT_DIR}/config.toml}"
PROC_NAME="${ORDER_EXPORT_PULLER_PROC:-order_export_puller}"
NAMESPACE="${ORDER_EXPORT_PULLER_NAMESPACE:-order_export_puller}"
LOG_LEVEL="${ORDER_EXPORT_PULLER_LOG_LEVEL:-INFO}"
PYTHON_BIN="${ORDER_EXPORT_PULLER_PYTHON:-python3}"

if [[ ! -f "$CONFIG" ]]; then
  echo "[ERROR] config not found: $CONFIG" >&2
  echo "        copy config.example.toml or set ORDER_EXPORT_PULLER_CONFIG" >&2
  exit 1
fi

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE}, config=${CONFIG})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

npx pm2 start "$PY_SCRIPT" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  --interpreter "$PYTHON_BIN" \
  -- \
  --config "$CONFIG" \
  --log-level "$LOG_LEVEL"

echo ""
echo "[INFO] Started ${PROC_NAME}"
echo "Logs:   npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
