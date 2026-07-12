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
if [[ ! "${dir_name,,}" =~ ^binance[-_]intra([-_].+)?$ ]]; then
  echo "[ERROR] hedge_lazy_taker_eval only supports a binance-intra environment: ${dir_name}" >&2
  exit 1
fi

BIN_PATH="${BASE_DIR}/hedge_lazy_taker_eval"
if [[ ! -x "${BIN_PATH}" ]]; then
  echo "[ERROR] binary not found: ${BIN_PATH}" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
if [[ "${PMDAEMON_BIN}" != */* ]] && ! command -v "${PMDAEMON_BIN}" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: ${PMDAEMON_BIN}" >&2
  exit 1
fi

env_tag="${dir_name#binance-intra-}"
env_tag="${env_tag//-/_}"
if [[ "${env_tag}" == "${dir_name}" ]]; then
  env_tag="intra"
fi
PROC_NAME="${HEDGE_LAZY_TAKER_EVAL_PROCESS_NAME:-intra_hlte_binance_${env_tag}}"
CONFIG_PATH="${HEDGE_LAZY_TAKER_EVAL_CONFIG:-${BASE_DIR}/config/mkt_cfg.yaml}"
OUTPUT_DIR="${HEDGE_LAZY_TAKER_EVAL_OUTPUT_DIR:-${BASE_DIR}/data/hedge_lazy_taker_eval}"
DELAY_MS="${HEDGE_LAZY_TAKER_EVAL_DELAY_MS:-2}"
BUFFER_SECS="${HEDGE_LAZY_TAKER_EVAL_BUFFER_SECS:-600}"
MAX_POINTS="${HEDGE_LAZY_TAKER_EVAL_MAX_POINTS_PER_SYMBOL:-200000}"

args=(
  --config "${CONFIG_PATH}"
  --output-dir "${OUTPUT_DIR}"
  --delay-ms "${DELAY_MS}"
  --buffer-secs "${BUFFER_SECS}"
  --max-points-per-symbol "${MAX_POINTS}"
)
if [[ -n "${HEDGE_LAZY_TAKER_EVAL_CORE:-}" ]]; then
  args+=(--core "${HEDGE_LAZY_TAKER_EVAL_CORE}")
fi
if [[ -n "${HEDGE_LAZY_TAKER_EVAL_SYMBOLS:-}" ]]; then
  args+=(--symbols "${HEDGE_LAZY_TAKER_EVAL_SYMBOLS}")
fi

shell_quote() {
  printf '%q' "${1}"
}

json_escape() {
  printf '%s' "${1}" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cmd="if [[ -f $(shell_quote "${ENV_FILE}") ]]; then source $(shell_quote "${ENV_FILE}"); fi; exec $(shell_quote "${BIN_PATH}")"
for arg in "${args[@]}"; do
  cmd+=" $(shell_quote "${arg}")"
done

cfg_file="$(mktemp)"
trap 'rm -f "${cfg_file}" >/dev/null 2>&1 || true' EXIT
cat >"${cfg_file}" <<JSON
{
  "apps": [
    {
      "name": "$(json_escape "${PROC_NAME}")",
      "script": "/bin/bash",
      "args": ["-lc", "$(json_escape "${cmd}")"],
      "cwd": "$(json_escape "${BASE_DIR}")",
      "env": {
        "RUST_LOG": "$(json_escape "${RUST_LOG:-info}")",
        "IPC_NAMESPACE": "$(json_escape "${IPC_NAMESPACE:-default}")"
      }
    }
  ]
}
JSON

"${SCRIPT_DIR}/stop_hedge_lazy_taker_eval.sh"
"${PMDAEMON_BIN}" --config "${cfg_file}" start --name "${PROC_NAME}"

echo "[INFO] started ${PROC_NAME}"
echo "[INFO] output: ${OUTPUT_DIR}"
