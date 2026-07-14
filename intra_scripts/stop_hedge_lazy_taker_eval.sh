#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${BASE_DIR}/env.sh"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

dir_name="$(basename "${BASE_DIR}")"
env_tag="${dir_name#binance-intra-}"
env_tag="${env_tag//-/_}"
if [[ "${env_tag}" == "${dir_name}" ]]; then
  env_tag="intra"
fi

PROC_NAME="${HEDGE_LAZY_TAKER_EVAL_PROCESS_NAME:-intra_hlte_binance_${env_tag}}"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"
BIN_PATH="${BASE_DIR}/hedge_lazy_taker_eval"

if [[ "${PMDAEMON_BIN}" != */* ]] && ! command -v "${PMDAEMON_BIN}" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: ${PMDAEMON_BIN}" >&2
  exit 1
fi

find_running_pids() {
  ps -eo pid=,args= | awk -v bin="${BIN_PATH}" '$2 == bin { print $1 }'
}

echo "[INFO] stopping ${PROC_NAME}"
if "${PMDAEMON_BIN}" delete "${PROC_NAME}" >/dev/null 2>&1; then
  echo "[INFO] removed ${PROC_NAME} from pmdaemon"
else
  echo "[WARN] process not found in pmdaemon: ${PROC_NAME}"
fi

mapfile -t leaked_pids < <(find_running_pids)
if [[ ${#leaked_pids[@]} -eq 0 ]]; then
  echo "[INFO] stopped ${PROC_NAME}"
  exit 0
fi

echo "[WARN] found residual process(es): ${leaked_pids[*]}"
echo "[INFO] sending SIGTERM"
kill "${leaked_pids[@]}" >/dev/null 2>&1 || true

deadline=$((SECONDS + KILL_WAIT_SECS))
while [[ $SECONDS -lt $deadline ]]; do
  mapfile -t leaked_pids < <(find_running_pids)
  if [[ ${#leaked_pids[@]} -eq 0 ]]; then
    break
  fi
  sleep 1
done

if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[WARN] SIGTERM timeout, sending SIGKILL: ${leaked_pids[*]}"
  kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
  sleep 1
  mapfile -t leaked_pids < <(find_running_pids)
fi

if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[ERROR] failed to stop residual process(es): ${leaked_pids[*]}" >&2
  exit 1
fi

echo "[INFO] stopped ${PROC_NAME}"
