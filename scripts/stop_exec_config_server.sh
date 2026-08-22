#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROCESS_MATCH_LIB="${SCRIPT_DIR}/process_match_lib.sh"
# shellcheck disable=SC1090
source "$PROCESS_MATCH_LIB"
dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
APP_NAME="${PMDAEMON_NAME:-exec_cfg_${dir_tag}}"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: ${PMDAEMON_BIN}" >&2; exit 1; }

echo "[INFO] Stopping exec_config_server name=${APP_NAME}"
"$PMDAEMON_BIN" delete "$APP_NAME" >/dev/null 2>&1 || true

leaked_pids=()
for comm in python python3; do
  mapfile -t matched < <(safe_find_running_pids "$comm" "$BASE_DIR" "exec_config_server.py" || true)
  leaked_pids+=("${matched[@]}")
done
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true
fi
echo "[INFO] Stopped ${APP_NAME}"
