#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="persist_exporter"
PROC_NAME="persist_admin_server"

BIN_PATH="${BASE_DIR}/persist_admin_server"
CFG_PATH="${BASE_DIR}/config/persist_auto_exporter.toml"
BIND_ADDR="${PERSIST_ADMIN_BIND:-0.0.0.0:10331}"

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
  -- --config "${CFG_PATH}" --bind "${BIND_ADDR}"

echo "[INFO] started ${PROC_NAME} bind=${BIND_ADDR}"
echo "[INFO] logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"

