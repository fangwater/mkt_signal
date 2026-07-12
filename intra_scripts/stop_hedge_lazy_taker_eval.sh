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

if "${PMDAEMON_BIN}" delete "${PROC_NAME}" >/dev/null 2>&1; then
  echo "[INFO] stopped ${PROC_NAME}"
else
  echo "[WARN] process not found: ${PROC_NAME}"
fi
