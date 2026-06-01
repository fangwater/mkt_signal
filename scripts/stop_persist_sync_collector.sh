#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/env.sh" || -x "${SCRIPT_DIR}/persist_sync_collector" ]]; then
  BASE_DIR="${SCRIPT_DIR}"
else
  BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

if [[ -f "${BASE_DIR}/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BASE_DIR}/env.sh"
fi

ENV_NAME="$(basename "${BASE_DIR}")"
NAMESPACE="${PM2_NAMESPACE:-${ENV_NAME}}"
SYNC_PROC_NAME="${PERSIST_SYNC_PM2_NAME:-${PM2_NAME:-${ENV_NAME}_persist_sync_collector}}"
READ_PROC_NAME="${PERSIST_READ_PM2_NAME:-${ENV_NAME}_persist_read_server}"

stop_proc() {
  local proc_name="$1"
  if npx pm2 describe "$proc_name" --namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "[INFO] Stopping ${proc_name} (namespace=${NAMESPACE})"
    npx pm2 delete "$proc_name" --namespace "$NAMESPACE"
  else
    echo "[INFO] ${proc_name} not running in namespace ${NAMESPACE}"
  fi
}

stop_proc "$READ_PROC_NAME"
stop_proc "$SYNC_PROC_NAME"

echo "[INFO] Remaining processes: npx pm2 status --namespace ${NAMESPACE}"
