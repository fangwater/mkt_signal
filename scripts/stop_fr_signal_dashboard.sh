#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/dashboard.env" || -f "${SCRIPT_DIR}/fr_signal_dashboard" ]]; then
  BASE_DIR="${SCRIPT_DIR}"
else
  BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

if [[ -f "${BASE_DIR}/dashboard.env" ]]; then
  # shellcheck disable=SC1090
  source "${BASE_DIR}/dashboard.env"
fi

NAMESPACE="${PM2_NAMESPACE:-dashboard}"
PROC_NAME="${PM2_NAME:-fr_signal_dashboard}"

if npx pm2 describe "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Stopping ${PROC_NAME} (namespace=${NAMESPACE})"
  npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE"
else
  echo "[INFO] ${PROC_NAME} not running in namespace ${NAMESPACE}"
fi

echo "[INFO] Remaining processes: npx pm2 status --namespace ${NAMESPACE}"
