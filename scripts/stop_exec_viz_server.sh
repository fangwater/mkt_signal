#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1090
source "${SCRIPT_DIR}/process_match_lib.sh"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: ${PMDAEMON_BIN}" >&2; exit 1; }
dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-exec_vz_${dir_tag}}"
"$PMDAEMON_BIN" delete "$PROC_NAME" >/dev/null 2>&1 || true
mapfile -t leaked_pids < <(safe_find_running_pids "viz_server" "$BASE_DIR" || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true
  sleep 1
fi
mapfile -t leaked_pids < <(safe_find_running_pids "viz_server" "$BASE_DIR" || true)
[[ ${#leaked_pids[@]} -eq 0 ]] || { echo "[ERROR] viz_server did not stop: ${leaked_pids[*]}" >&2; exit 1; }
echo "[INFO] Stopped ${PROC_NAME}"
