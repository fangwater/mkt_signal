#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"
MM_NAME_LIB="${SCRIPT_DIR}/../scripts/mm_process_name.sh"
PROCESS_MATCH_LIB="${SCRIPT_DIR}/../scripts/process_match_lib.sh"

if [[ -f "$MM_NAME_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$MM_NAME_LIB"
fi
if [[ -f "$PROCESS_MATCH_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$PROCESS_MATCH_LIB"
fi

DIR_NAME="${BASE_DIR##*/}"
DIR_TAG="$(echo "${DIR_NAME}" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/_/g')"
DEFAULT_PROC_NAME="mm_persist_manager_${DIR_TAG}"
if type mm_default_proc_name >/dev/null 2>&1; then
  if inferred_name="$(mm_default_proc_name pm "$DIR_NAME" 2>/dev/null)" && [[ -n "$inferred_name" ]]; then
    DEFAULT_PROC_NAME="$inferred_name"
  fi
fi
PROC_NAME="${PMDAEMON_NAME:-$DEFAULT_PROC_NAME}"
LEGACY_PROC_NAME="mm_persist_manager_${DIR_TAG}"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

find_running_pids() {
  safe_find_running_pids "persist_manager" "$BASE_DIR"
}

cleanup_leaked() {
  mapfile -t leaked_pids < <(find_running_pids || true)
  if [[ ${#leaked_pids[@]} -eq 0 ]]; then
    return 0
  fi

  echo "[WARN] Found leaked persist_manager process after pmdaemon delete: pids=${leaked_pids[*]}"
  echo "[INFO] Sending SIGTERM to leaked process(es)"
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true

  local deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(find_running_pids || true)
    if [[ ${#leaked_pids[@]} -eq 0 ]]; then
      break
    fi
    sleep 1
  done

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout, sending SIGKILL: pids=${leaked_pids[*]}"
    kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
    sleep 1
    mapfile -t leaked_pids < <(find_running_pids || true)
  fi

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[ERROR] Failed to kill leaked persist_manager process(es): pids=${leaked_pids[*]}" >&2
    return 1
  fi

  echo "[INFO] Leaked process cleanup done"
  return 0
}

echo "[INFO] Stopping ${PROC_NAME}"
if [[ "$LEGACY_PROC_NAME" != "$PROC_NAME" ]]; then
  "${PMDAEMON[@]}" delete "$LEGACY_PROC_NAME" >/dev/null 2>&1 || true
fi
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  echo "[INFO] Stopped ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found"
fi

cleanup_leaked

echo "To view remaining processes: ${PMDAEMON[*]} list"
