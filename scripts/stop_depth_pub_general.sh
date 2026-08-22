#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESS_MATCH_LIB="${SCRIPT_DIR}/process_match_lib.sh"

if [[ -f "$PROCESS_MATCH_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$PROCESS_MATCH_LIB"
fi

usage() {
  cat <<'USAGE'
Usage:
  stop_depth_pub_general.sh
USAGE
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 不支持参数: $*" >&2
      usage >&2
      exit 1
      ;;
  esac
fi

if command -v pm2 >/dev/null 2>&1; then
  PM2=(pm2)
elif command -v npx >/dev/null 2>&1; then
  PM2=(npx pm2)
else
  PM2=()
fi

PROC_NAME="dp_general"
NAMESPACE="depth_pub"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

if [[ ${#PM2[@]} -gt 0 ]]; then
  "${PM2[@]}" delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
  "${PM2[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
fi

find_pids() {
  if declare -F safe_find_running_pids >/dev/null 2>&1; then
    safe_find_running_pids "depth_pub_general" || true
  else
    pgrep -f '[d]epth_pub_general' || true
  fi
}

mapfile -t pids < <(find_pids)
if [[ ${#pids[@]} -gt 0 ]]; then
  echo "[WARN] Found leaked depth_pub_general: ${pids[*]}"
  kill "${pids[@]}" >/dev/null 2>&1 || true
  deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t pids < <(find_pids)
    if [[ ${#pids[@]} -eq 0 ]]; then
      break
    fi
    sleep 1
  done
  if [[ ${#pids[@]} -gt 0 ]]; then
    kill -9 "${pids[@]}" >/dev/null 2>&1 || true
  fi
fi

echo "[INFO] stopped ${PROC_NAME}"
