#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/env.sh" || -x "${SCRIPT_DIR}/order_export_server" ]]; then
  BASE_DIR="${SCRIPT_DIR}"
else
  BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

if [[ -f "${BASE_DIR}/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BASE_DIR}/env.sh"
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/order_export_server"
  "${SCRIPT_DIR}/order_export_server"
  "${BASE_DIR}/target/release/order_export_server"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] order_export_server binary not found. Build/deploy first." >&2
  exit 1
fi

ENV_NAME="$(basename "${BASE_DIR}")"
BIND="${ORDER_EXPORT_SERVER_BIND:-0.0.0.0}"
PORT="${ORDER_EXPORT_SERVER_PORT:-8821}"
RETENTION_HOURS="${ORDER_EXPORT_SERVER_RETENTION_HOURS:-3}"
NAMESPACE="${ENV_NAME}"
PROC_NAME="${ENV_NAME}_order_export_server"
RUST_LOG="${RUST_LOG:-info}"

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE}, port=${PORT}, retention=${RETENTION_HOURS}h)"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
    --name "$PROC_NAME" \
    --namespace "$NAMESPACE" \
    -- \
    --bind "$BIND" \
    --port "$PORT" \
    --base-dir "$BASE_DIR" \
    --retention-hours "$RETENTION_HOURS"
)

echo ""
echo "[INFO] Started order_export_server"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
echo "Endpoint: http://${BIND}:${PORT}/latest"
