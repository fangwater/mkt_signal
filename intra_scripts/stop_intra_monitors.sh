#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

PROCESS_MATCH_LIB="${BASE_DIR}/scripts/process_match_lib.sh"
if [[ -f "$PROCESS_MATCH_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$PROCESS_MATCH_LIB"
fi
INTRA_MONITOR_PROCESS_LIB="${SCRIPT_DIR}/intra_monitor_process_lib.sh"
if [[ -f "$INTRA_MONITOR_PROCESS_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$INTRA_MONITOR_PROCESS_LIB"
else
  echo "[ERROR] intra monitor process helper not found: $INTRA_MONITOR_PROCESS_LIB" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

ensure_pmdaemon() {
  if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
    echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
    exit 1
  fi
}

ensure_pmdaemon

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

EXCHANGE=""
ENV_TAG="intra"
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="${BASH_REMATCH[2]//-/_}"
elif [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
fi
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi
if [[ -z "$EXCHANGE" && -n "${OPEN_VENUE:-}" ]]; then
  EXCHANGE="${OPEN_VENUE%%-*}"
fi
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 无法确定 exchange (dir=$dir_name)"
  exit 1
fi

PROC_NAME="intra_am_${EXCHANGE}_${ENV_TAG}"

echo "[INFO] Stopping $PROC_NAME"
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  echo "[INFO] Stopped $PROC_NAME"
else
  echo "[WARN] Process not found: $PROC_NAME"
fi
intra_cleanup_leaked_account_monitor "$BASE_DIR" "$EXCHANGE" "$KILL_WAIT_SECS"

echo "[INFO] Status: ${PMDAEMON[*]} list"
