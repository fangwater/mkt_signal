#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="persist_exporter"
PROC_NAME="persist_auto_exporter_public"

BIN_PATH="${BASE_DIR}/persist_auto_exporter"
CFG_PATH="${BASE_DIR}/config/persist_auto_exporter.toml"
STATE_PATH="${BASE_DIR}/persist_auto_exporter_state.toml"

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "[ERROR] binary not found: ${BIN_PATH}" >&2
  exit 1
fi
if [[ ! -f "${CFG_PATH}" ]]; then
  echo "[ERROR] config not found: ${CFG_PATH}" >&2
  exit 1
fi

echo "[INFO] restarting ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "${PROC_NAME}" --namespace "${NAMESPACE}" >/dev/null 2>&1 || true

cd "${BASE_DIR}"
RUST_LOG="${RUST_LOG:-info}" npx pm2 start "${BIN_PATH}" \
  --name "${PROC_NAME}" \
  --namespace "${NAMESPACE}" \
  -- --config "${CFG_PATH}" --state-file "${STATE_PATH}"

echo "[INFO] started ${PROC_NAME}"
echo "[INFO] logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"

