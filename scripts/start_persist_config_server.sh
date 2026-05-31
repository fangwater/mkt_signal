#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/env.sh" || -f "${SCRIPT_DIR}/persist_config_server.py" ]]; then
  BASE_DIR="${SCRIPT_DIR}"
else
  BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

if [[ -f "${BASE_DIR}/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BASE_DIR}/env.sh"
fi

default_config_path() {
  local candidates=(
    "${BASE_DIR}/persist.toml"
    "${BASE_DIR}/config/persist.toml"
  )
  local cand
  for cand in "${candidates[@]}"; do
    if [[ -f "$cand" ]]; then
      echo "$cand"
      return 0
    fi
  done
  echo "${BASE_DIR}/persist.toml"
}

find_server() {
  local candidates=(
    "${BASE_DIR}/persist_config_server.py"
    "${SCRIPT_DIR}/persist_config_server.py"
    "${BASE_DIR}/scripts/persist_config_server.py"
    "${SCRIPT_DIR}/../scripts/persist_config_server.py"
  )
  local cand
  for cand in "${candidates[@]}"; do
    if [[ -f "$cand" ]]; then
      echo "$cand"
      return 0
    fi
  done
  return 1
}

DEFAULT_CONFIG="$(default_config_path)"
CONFIG_PATH="${1:-${PERSIST_CONFIG_SERVER_CONFIG:-${PERSIST_CONFIG:-${DEFAULT_CONFIG}}}}"
if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[ERROR] config not found: ${CONFIG_PATH}" >&2
  exit 1
fi
CONFIG_ABS="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"

SERVER_SCRIPT="$(find_server || true)"
if [[ -z "$SERVER_SCRIPT" ]]; then
  echo "[ERROR] persist_config_server.py not found. Deploy scripts first." >&2
  exit 1
fi

ENV_NAME="$(basename "${BASE_DIR}")"
NAMESPACE="${PM2_NAMESPACE:-${ENV_NAME}}"
PROC_NAME="${PERSIST_CONFIG_PM2_NAME:-${ENV_NAME}_persist_config_server}"
BIND="${PERSIST_CONFIG_BIND:-127.0.0.1}"
PORT="${PERSIST_CONFIG_PORT:-8830}"
PYTHON="${PYTHON:-python3}"

EXTRA_ARGS=()
if [[ -n "${PERSIST_SYNC_STATUS_PATH:-}" ]]; then
  EXTRA_ARGS+=(--status "${PERSIST_SYNC_STATUS_PATH}")
fi

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE}, config=${CONFIG_ABS})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  npx pm2 start "$PYTHON" \
    --name "$PROC_NAME" \
    --namespace "$NAMESPACE" \
    -- \
    "$SERVER_SCRIPT" \
    --config "$CONFIG_ABS" \
    --bind "$BIND" \
    --port "$PORT" \
    "${EXTRA_ARGS[@]}"
)

echo ""
echo "[INFO] Started persist_config_server"
echo "URL: http://${BIND}:${PORT}"
echo "Namespace: ${NAMESPACE}"
echo "Process: ${PROC_NAME}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
