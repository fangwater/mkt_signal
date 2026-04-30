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
CFG_ENV_FILE="${BASE_DIR}/config/intra_config_server.env"
if [[ -f "$CFG_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CFG_ENV_FILE"
fi

APP_SCRIPT="${SCRIPT_DIR}/intra_config_server.py"
if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] 未找到 intra_config_server.py (期望路径: $APP_SCRIPT)"
  exit 1
fi

normalize_exchange() {
  local ex="${1,,}"
  [[ "$ex" == "okx" ]] && ex="okex"
  echo "$ex"
}

exchange_default_port() {
  case "$1" in
    binance) echo "18131" ;;
    okex)    echo "18132" ;;
    bybit)   echo "18133" ;;
    bitget)  echo "18134" ;;
    gate)    echo "18135" ;;
    *)       echo "18130" ;;
  esac
}

dir_name="$(basename "$BASE_DIR")"
dir_lc="${dir_name,,}"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"

EXCHANGE=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra([-_].+)?$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
fi
EXCHANGE="$(normalize_exchange "${EXCHANGE:-}")"
if [[ -z "$EXCHANGE" && -n "${OPEN_VENUE:-}" ]]; then
  EXCHANGE="$(normalize_exchange "${OPEN_VENUE%%-*}")"
fi
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 无法确定 exchange (dir=$dir_name)"
  exit 1
fi

DEFAULT_OPEN_VENUE="${DEFAULT_OPEN_VENUE:-${OPEN_VENUE:-${EXCHANGE}-margin}}"
DEFAULT_HEDGE_VENUE="${DEFAULT_HEDGE_VENUE:-${HEDGE_VENUE:-${EXCHANGE}-futures}}"
DEFAULT_EXCHANGE="${DEFAULT_EXCHANGE:-$EXCHANGE}"
HOST="${HOST:-0.0.0.0}"
APP_NAME="${PM2_NAME:-intra_config_server_${dir_tag}}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x "/home/ubuntu/jupyter_env/bin/python" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python"
  elif [[ -x "/home/ubuntu/jupyter_env/bin/python3" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

PORT="${PORT:-$(exchange_default_port "$EXCHANGE")}"

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

echo "[INFO] 启动 intra_config_server (exchange=${EXCHANGE}, port=${PORT}, namespace=${NAMESPACE})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  PORT="$PORT" HOST="$HOST" DEFAULT_EXCHANGE="$DEFAULT_EXCHANGE" \
  DEFAULT_OPEN_VENUE="$DEFAULT_OPEN_VENUE" DEFAULT_HEDGE_VENUE="$DEFAULT_HEDGE_VENUE" \
  npx pm2 start "$PYTHON_BIN" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    -- \
    "$APP_SCRIPT" \
    --host "$HOST" \
    --port "$PORT" \
    --default-exchange "$DEFAULT_EXCHANGE" \
    --default-open-venue "$DEFAULT_OPEN_VENUE" \
    --default-hedge-venue "$DEFAULT_HEDGE_VENUE"
)

echo "[INFO] 已启动：pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] 日志：npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
