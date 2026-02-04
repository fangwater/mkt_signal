#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

PROC_NAME="${PM2_NAME:-mm_trade_engine_$(echo "${BASE_DIR##*/}" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/_/g')}"

if npx pm2 describe "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Stopping ${PROC_NAME} (namespace=${NAMESPACE})"
  npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE"
else
  echo "[WARN] PM2 process not found: ${PROC_NAME} (namespace=${NAMESPACE})"
fi
