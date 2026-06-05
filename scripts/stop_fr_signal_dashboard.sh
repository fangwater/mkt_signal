#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/env.sh" || -f "${SCRIPT_DIR}/fr_signal_dashboard" ]]; then
  BASE_DIR="${SCRIPT_DIR}"
else
  BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

ENV_NAME="$(basename "${BASE_DIR}")"
NAMESPACE="${ENV_NAME}"
PROC_NAME="${ENV_NAME}_fr_signal_dashboard"

if [[ -f "${BASE_DIR}/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BASE_DIR}/env.sh"
fi

find_env_dashboard_pids() {
  local bin_path="${BASE_DIR}/fr_signal_dashboard"
  local dashboard_port="${FR_DASHBOARD_PORT:-}"
  local proc pid arg0 exe has_port i
  local argv=()

  for proc in /proc/[0-9]*; do
    pid="${proc##*/}"
    [[ "$pid" != "$$" ]] || continue
    [[ -r "${proc}/cmdline" ]] || continue

    argv=()
    mapfile -d '' -t argv < "${proc}/cmdline" 2>/dev/null || true
    [[ "${#argv[@]}" -gt 0 ]] || continue

    arg0="${argv[0]}"
    if [[ "$arg0" != "$bin_path" ]]; then
      exe="$(readlink -f "${proc}/exe" 2>/dev/null || true)"
      exe="${exe% (deleted)}"
      [[ "$exe" == "$bin_path" ]] || continue
    fi

    if [[ -n "$dashboard_port" ]]; then
      has_port=0
      for ((i = 0; i < ${#argv[@]}; i++)); do
        if [[ "${argv[$i]}" == "--port" && "${argv[$((i + 1))]:-}" == "$dashboard_port" ]]; then
          has_port=1
          break
        fi
        if [[ "${argv[$i]}" == "--port=${dashboard_port}" ]]; then
          has_port=1
          break
        fi
      done
      [[ "$has_port" -eq 1 ]] || continue
    fi

    echo "$pid"
  done
}

if npx pm2 describe "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Stopping ${PROC_NAME} (namespace=${NAMESPACE})"
  npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE"
else
  echo "[INFO] ${PROC_NAME} not running in namespace ${NAMESPACE}"
fi

mapfile -t STRAY_PIDS < <(find_env_dashboard_pids)
if [[ "${#STRAY_PIDS[@]}" -gt 0 ]]; then
  echo "[INFO] Stopping unmanaged fr_signal_dashboard pids for ${ENV_NAME}: ${STRAY_PIDS[*]}"
  kill "${STRAY_PIDS[@]}" 2>/dev/null || true

  for _ in {1..50}; do
    REMAINING_PIDS=()
    for pid in "${STRAY_PIDS[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        REMAINING_PIDS+=("$pid")
      fi
    done
    [[ "${#REMAINING_PIDS[@]}" -eq 0 ]] && break
    sleep 0.1
  done

  if [[ "${#REMAINING_PIDS[@]}" -gt 0 ]]; then
    echo "[WARN] Force killing unmanaged fr_signal_dashboard pids for ${ENV_NAME}: ${REMAINING_PIDS[*]}"
    kill -KILL "${REMAINING_PIDS[@]}" 2>/dev/null || true
  fi
fi

echo "[INFO] Remaining processes: npx pm2 status --namespace ${NAMESPACE}"
