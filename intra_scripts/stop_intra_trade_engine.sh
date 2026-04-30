#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

ensure_pmdaemon() {
  if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
    echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
    echo "[HINT] install with: cargo install pmdaemon" >&2
    exit 1
  fi
}

usage() {
  cat <<'USAGE'
用法: intra_scripts/stop_intra_trade_engine.sh

说明:
  - 同所期现：从目录名 <exchange>-intra-<tag> 推断 exchange / env_tag
  - 停止 pmdaemon 进程：intra_te_<exchange>_<env>
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ensure_pmdaemon

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

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
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 exchange (dir=$dir_name)，期望 <exchange>-intra-<tag>"
  exit 1
fi

PROC_NAME="intra_te_${EXCHANGE}_${ENV_TAG}"

echo "[INFO] Stopping $PROC_NAME"
if "${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1; then
  echo "[INFO] Stopped $PROC_NAME"
else
  echo "[WARN] Process not found: $PROC_NAME"
fi

echo "[INFO] Status: ${PMDAEMON[*]} list"
