#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROCESS_MATCH_LIB="${SCRIPT_DIR}/process_match_lib.sh"
# shellcheck disable=SC1090
source "$PROCESS_MATCH_LIB"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: ${PMDAEMON_BIN}" >&2; exit 1; }
dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-exec_pt_${dir_tag}}"
"$PMDAEMON_BIN" delete "$PROC_NAME" >/dev/null 2>&1 || true

mapfile -t leaked_pids < <(safe_find_running_pids "exec-pre-trade" "$BASE_DIR" || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true
  deadline=$((SECONDS + 6))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(safe_find_running_pids "exec-pre-trade" "$BASE_DIR" || true)
    [[ ${#leaked_pids[@]} -eq 0 ]] && break
    sleep 1
  done
fi
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[ERROR] exec-pre-trade did not stop: ${leaked_pids[*]}" >&2
  exit 1
fi
echo "[INFO] Stopped ${PROC_NAME}"
