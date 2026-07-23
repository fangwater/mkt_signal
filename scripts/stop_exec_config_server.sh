#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
dir_name="$(basename "$BASE_DIR")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
NAMESPACE="${PM2_NAMESPACE:-$dir_name}"
APP_NAME="${PM2_NAME:-exec_config_server_${dir_tag}}"

echo "[INFO] Stopping exec_config_server name=${APP_NAME} namespace=${NAMESPACE}"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
