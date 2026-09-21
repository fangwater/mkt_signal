#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
[[ -f "${BASE_DIR}/env.sh" ]] && source "${BASE_DIR}/env.sh"
APP_SCRIPT="${SCRIPT_DIR}/cta_special_config_server.py"
[[ -f "$APP_SCRIPT" ]] || { echo "[ERROR] missing $APP_SCRIPT" >&2; exit 1; }
[[ -f "${BASE_DIR}/config/cta_special.json" ]] || { echo "[ERROR] missing config/cta_special.json" >&2; exit 1; }
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2; exit 1; }
PYTHON_BIN="${PYTHON_BIN:-python3}"
dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-cta_special_cfg_${dir_tag}}"
BIND="${CTA_SPECIAL_CONFIG_BIND:-127.0.0.1}"
PORT="${CTA_SPECIAL_CONFIG_PORT:-19182}"
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
printf '{"apps":[{"name":"%s","script":"%s","args":["%s","--bind","%s","--port","%s","--config","config/cta_special.json","--index","web/cta_special_config/index.html"],"cwd":"%s"}]}\n' \
  "$PROC_NAME" "$PYTHON_BIN" "$APP_SCRIPT" "$BIND" "$PORT" "$BASE_DIR" >"$cfg_file"
PMDAEMON_NAME="$PROC_NAME" "${SCRIPT_DIR}/stop_cta_special_config_server.sh"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$PROC_NAME"
echo "[INFO] started $PROC_NAME at http://${BIND}:${PORT}"
