#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/dashboard.env" || -f "${SCRIPT_DIR}/fr_signal_dashboard" ]]; then
  BASE_DIR="${SCRIPT_DIR}"
else
  BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

ENV_FILE_CANDIDATES=(
  "${BASE_DIR}/dashboard.env"
  "${BASE_DIR}/config/dashboard.env"
)

for env_file in "${ENV_FILE_CANDIDATES[@]}"; do
  if [[ -f "$env_file" ]]; then
    # shellcheck disable=SC1090
    source "$env_file"
    break
  fi
done

BIN_CANDIDATES=(
  "${BASE_DIR}/fr_signal_dashboard"
  "${SCRIPT_DIR}/fr_signal_dashboard"
  "${BASE_DIR}/target/release/fr_signal_dashboard"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] fr_signal_dashboard binary not found. Build/deploy first." >&2
  exit 1
fi

EXCHANGE="${FR_DASHBOARD_EXCHANGE:-${1:-}}"
BIND="${FR_DASHBOARD_BIND:-0.0.0.0}"
PORT="${FR_DASHBOARD_PORT:-6305}"
WS_PATH="${FR_DASHBOARD_WS_PATH:-/ws}"
NAMESPACE="${PM2_NAMESPACE:-dashboard}"
PROC_NAME="${PM2_NAME:-fr_signal_dashboard}"
RUST_LOG="${RUST_LOG:-info}"

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] missing exchange. Set FR_DASHBOARD_EXCHANGE in dashboard.env or pass it as the first arg." >&2
  exit 1
fi

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE}, exchange=${EXCHANGE}, port=${PORT})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  --exchange "$EXCHANGE" \
  --bind "$BIND" \
  --port "$PORT" \
  --ws-path "$WS_PATH"

echo ""
echo "[INFO] Started fr_signal_dashboard"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
