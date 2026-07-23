#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${BASE_DIR}/env.sh"
CFG_ENV_FILE="${BASE_DIR}/config/exec_config_server.env"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi
if [[ -f "$CFG_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CFG_ENV_FILE"
fi

APP_SCRIPT="${SCRIPT_DIR}/exec_config_server.py"
if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] exec_config_server.py not found: $APP_SCRIPT" >&2
  exit 1
fi

dir_name="$(basename "$BASE_DIR")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
ENV_NAME="${ENV_NAME:-$dir_name}"
VENUE="${VENUE:-${EXEC_VENUE:-}}"
if [[ -z "$VENUE" ]]; then
  echo "[ERROR] VENUE or EXEC_VENUE is required" >&2
  exit 1
fi

BIND="${BIND:-0.0.0.0}"
PORT="${PORT:-18161}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
NAMESPACE="${PM2_NAMESPACE:-$dir_name}"
APP_NAME="${PM2_NAME:-exec_config_server_${dir_tag}}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x "/home/ubuntu/jupyter_env/bin/python" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

echo "[INFO] Starting exec_config_server env=${ENV_NAME} venue=${VENUE} port=${PORT}"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
(
  cd "$BASE_DIR"
  npx pm2 start "$PYTHON_BIN" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    -- \
    "$APP_SCRIPT" \
    --bind "$BIND" \
    --port "$PORT" \
    --redis-url "$REDIS_URL" \
    --env-name "$ENV_NAME" \
    --venue "$VENUE"
)

echo "[INFO] Started: npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
