#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="persist_exporter"
PROC_NAME="persist_admin_server"

echo "[INFO] stopping ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "${PROC_NAME}" --namespace "${NAMESPACE}" >/dev/null 2>&1 || true
echo "[INFO] stopped ${PROC_NAME}"

