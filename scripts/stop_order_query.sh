#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"
APP_NAME="${PM2_NAME:-order_query}"

echo "[INFO] Deleting ${APP_NAME} (namespace=${NAMESPACE})"
if npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE"; then
  echo "[INFO] Deleted ${APP_NAME}"
else
  echo "[WARN] ${APP_NAME} not found in namespace ${NAMESPACE}"
fi

echo "[INFO] Remaining: npx pm2 status --namespace ${NAMESPACE}"

