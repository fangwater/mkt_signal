#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"
MM_NAME_LIB="${SCRIPT_DIR}/mm_process_name.sh"

if [[ -f "$MM_NAME_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$MM_NAME_LIB"
fi

dir_name="$(basename "$BASE_DIR")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
DEFAULT_APP_NAME="mm_config_server_${dir_tag}"
if type mm_default_proc_name >/dev/null 2>&1; then
  if inferred_name="$(mm_default_proc_name cfg "$dir_name" 2>/dev/null)" && [[ -n "$inferred_name" ]]; then
    DEFAULT_APP_NAME="$inferred_name"
  fi
fi
APP_NAME="${PM2_NAME:-$DEFAULT_APP_NAME}"
LEGACY_APP_NAME="mm_config_server_${dir_tag}"

echo "[INFO] 停止 mm_config_server (name=${APP_NAME}, namespace=${NAMESPACE})"
if [[ "$LEGACY_APP_NAME" != "$APP_NAME" ]]; then
  npx pm2 delete "$LEGACY_APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
fi
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] 已停止。"
