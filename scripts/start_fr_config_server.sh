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
CFG_ENV_FILE="${BASE_DIR}/config/fr_config_server.env"
if [[ -f "$CFG_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CFG_ENV_FILE"
fi

APP_SCRIPT="${SCRIPT_DIR}/fr_config_server.py"
if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] 未找到 fr_config_server.py (期望路径: $APP_SCRIPT)"
  exit 1
fi

dir_name="$(basename "$BASE_DIR")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
detected_exchange=""
case "$dir_name" in
  *okex*|*OKEX*) detected_exchange="okex" ;;
  *bybit*|*BYBIT*) detected_exchange="bybit" ;;
  *bitget*|*BITGET*) detected_exchange="bitget" ;;
  *gate*|*GATE*) detected_exchange="gate" ;;
  *binance*|*BINANCE*) detected_exchange="binance" ;;
esac

EXCHANGE="${EXCHANGE:-${detected_exchange:-okex}}"
HOST="${HOST:-0.0.0.0}"
DEFAULT_EXCHANGE="${DEFAULT_EXCHANGE:-$EXCHANGE}"
APP_NAME="${PM2_NAME:-fr_config_server_${dir_tag}}"
if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x "/home/ubuntu/jupyter_env/bin/python" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python"
  elif [[ -x "/home/ubuntu/jupyter_env/bin/python3" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

default_port_for_exchange() {
  case "$1" in
    okex) echo "18011" ;;
    gate) echo "18021" ;;
    binance) echo "18031" ;;
    bybit) echo "18041" ;;
    bitget) echo "18051" ;;
    *) echo "18011" ;;
  esac
}

PORT="${PORT:-$(default_port_for_exchange "$EXCHANGE")}"

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

if port_in_use "$PORT"; then
  echo "[ERROR] Port ${PORT} is already in use; aborting." >&2
  exit 1
fi

echo "[INFO] 启动 fr_config_server (exchange=${DEFAULT_EXCHANGE}, port=${PORT}, namespace=${NAMESPACE})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  PORT="$PORT" HOST="$HOST" DEFAULT_EXCHANGE="$DEFAULT_EXCHANGE" \
  npx pm2 start "$PYTHON_BIN" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    -- \
    "$APP_SCRIPT" \
    --host "$HOST" \
    --port "$PORT" \
    --default-exchange "$DEFAULT_EXCHANGE"
)

echo "[INFO] 已启动：pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] 日志：npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
