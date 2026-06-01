#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/env.sh" || -f "${SCRIPT_DIR}/persist_config_server.py" ]]; then
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
PROC_NAME="${PERSIST_CONFIG_PM2_NAME:-${ENV_NAME}_persist_config_server}"

if [[ -n "${PM2_BIN:-}" ]]; then
  PM2_CMD=( $PM2_BIN )
elif command -v pm2 >/dev/null 2>&1; then
  PM2_CMD=( pm2 )
else
  PM2_CMD=( npx pm2 )
fi

if "${PM2_CMD[@]}" describe "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Stopping ${PROC_NAME} (namespace=${NAMESPACE})"
  "${PM2_CMD[@]}" delete "$PROC_NAME" --namespace "$NAMESPACE"
else
  echo "[INFO] ${PROC_NAME} not running in namespace ${NAMESPACE}"
fi

echo "[INFO] Remaining processes: ${PM2_CMD[*]} status --namespace ${NAMESPACE}"
