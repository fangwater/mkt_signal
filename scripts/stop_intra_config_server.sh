#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

dir_name="$(basename "$BASE_DIR")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
APP_NAME="${PM2_NAME:-intra_config_server_${dir_tag}}"

echo "[INFO] 停止 intra_config_server (name=${APP_NAME}, namespace=${NAMESPACE})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] 已停止。"
