#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/order_export_puller.py"
CONFIG="${ORDER_EXPORT_PULLER_CONFIG:-${SCRIPT_DIR}/config.toml}"
PROC_NAME="${ORDER_EXPORT_PULLER_PROC:-order_export_puller}"
NAMESPACE="${ORDER_EXPORT_PULLER_NAMESPACE:-order_export_puller}"
LOG_LEVEL="${ORDER_EXPORT_PULLER_LOG_LEVEL:-INFO}"
PYTHON_BIN="${ORDER_EXPORT_PULLER_PYTHON:-python3}"

# Prefer a globally-installed pm2; fall back to `npx pm2` if absent.
# Override explicitly with PM2_BIN=... (can be "pm2", "npx pm2", or a full path).
if [[ -n "${PM2_BIN:-}" ]]; then
  PM2_CMD=( $PM2_BIN )
elif command -v pm2 >/dev/null 2>&1; then
  PM2_CMD=( pm2 )
else
  PM2_CMD=( npx pm2 )
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "[ERROR] config not found: $CONFIG" >&2
  echo "        copy config.example.toml or set ORDER_EXPORT_PULLER_CONFIG" >&2
  exit 1
fi

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE}, config=${CONFIG})"
echo "[INFO] using pm2: ${PM2_CMD[*]}"
"${PM2_CMD[@]}" delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

"${PM2_CMD[@]}" start "$PY_SCRIPT" \
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
