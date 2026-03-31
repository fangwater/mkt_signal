#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

APP_SCRIPT="${SCRIPT_DIR}/tlen_config_server.py"
if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] 未找到 tlen_config_server.py (期望路径: $APP_SCRIPT)" >&2
  exit 1
fi

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-6322}"
DEFAULT_VENUE="${DEFAULT_VENUE:-${DEFAULT_OPEN_VENUE:-binance-futures}}"
APP_NAME="${PM2_NAME:-tlen_config_server_shared}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x "/home/ubuntu/jupyter_env/bin/python" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python"
  elif [[ -x "/home/ubuntu/jupyter_env/bin/python3" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "(:|\\])${port}\$"
    return $?
  fi
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  if command -v netstat >/dev/null 2>&1; then
    netstat -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "(:|\\])${port}\$"
    return $?
  fi
  return 1
}

npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
sleep 1

if port_in_use "$PORT"; then
  echo "[ERROR] Port ${PORT} is already in use; aborting." >&2
  exit 1
fi

echo "[INFO] 启动 tlen_config_server (port=${PORT}, namespace=${NAMESPACE}, app=${APP_NAME})"
(
  cd "$BASE_DIR"
  HOST="$HOST" PORT="$PORT" DEFAULT_VENUE="$DEFAULT_VENUE" \
  npx pm2 start "$PYTHON_BIN" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    -- \
    "$APP_SCRIPT" \
    --host "$HOST" \
    --port "$PORT" \
    --default-venue "$DEFAULT_VENUE"
)

echo "[INFO] 已启动：npx pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] 日志：npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] 页面：http://${HOST}:${PORT}"
