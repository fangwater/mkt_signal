#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
[[ -f "${BASE_DIR}/env.sh" ]] && source "${BASE_DIR}/env.sh"
APP_SCRIPT="${SCRIPT_DIR}/cta_spread_dashboard.py"
[[ -f "$APP_SCRIPT" ]] || { echo "[ERROR] missing $APP_SCRIPT" >&2; exit 1; }
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2; exit 1; }
PYTHON_BIN="${PYTHON_BIN:-python3}"
dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-cta_spread_dash_${dir_tag}}"
BIND="${CTA_SPREAD_DASHBOARD_BIND:-127.0.0.1}"
PORT="${CTA_SPREAD_DASHBOARD_PORT:-10186}"
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
printf '{"apps":[{"name":"%s","script":"%s","args":["%s","--bind","%s","--port","%s","--config","config/cta_spread.json","--status","run/cta_spread_status.json","--index","web/cta_spread/index.html"],"cwd":"%s"}]}\n' \
  "$PROC_NAME" "$PYTHON_BIN" "$APP_SCRIPT" "$BIND" "$PORT" "$BASE_DIR" >"$cfg_file"
PMDAEMON_NAME="$PROC_NAME" "${SCRIPT_DIR}/stop_cta_spread_dashboard.sh"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$PROC_NAME"
echo "[INFO] started $PROC_NAME at http://${BIND}:${PORT}"
