#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENUE_DIR_REGEX='^([a-z0-9]+-(futures|margin|spot|swap|perp|perpetual)|(binance|bitget)-coin-futures)$'
PROCESS_MATCH_LIB="${SCRIPT_DIR}/process_match_lib.sh"

if [[ $# -gt 0 && "$1" != "--help" && "$1" != "-h" ]]; then
  echo "[ERROR] unsupported arguments: $*" >&2
  exit 1
fi
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: stop_intra_factor_model_1m_pub.sh"
  exit 0
fi
if [[ ! -f "$PROCESS_MATCH_LIB" ]]; then
  echo "[ERROR] missing process helper: $PROCESS_MATCH_LIB" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$PROCESS_MATCH_LIB"

venue="$(basename "${BASE_DIR}" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ $VENUE_DIR_REGEX ]]; then
  echo "[ERROR] cannot infer venue from deployment directory: ${BASE_DIR}" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
name="intra_factor_model_1m_pub_${venue}"
"$PMDAEMON_BIN" delete "$name" >/dev/null 2>&1 || true

mapfile -t leaked_pids < <(safe_find_running_pids "$name" "$BASE_DIR" "--venue ${venue}" || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[WARN] stopping leaked ${name} PID(s): ${leaked_pids[*]}"
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true
  deadline=$((SECONDS + ${KILL_WAIT_SECS:-6}))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(safe_find_running_pids "$name" "$BASE_DIR" "--venue ${venue}" || true)
    [[ ${#leaked_pids[@]} -eq 0 ]] && break
    sleep 1
  done
  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] force-stopping leaked ${name} PID(s): ${leaked_pids[*]}"
    kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
  fi
fi

echo "[INFO] stopped ${name}"
