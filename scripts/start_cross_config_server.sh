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
CFG_ENV_FILE="${BASE_DIR}/config/cross_config_server.env"
if [[ -f "$CFG_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CFG_ENV_FILE"
fi

APP_SCRIPT="${SCRIPT_DIR}/cross_config_server.py"
if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] 未找到 cross_config_server.py (期望路径: $APP_SCRIPT)"
  exit 1
fi

normalize_exchange() {
  local ex="${1,,}"
  [[ "$ex" == "okx" ]] && ex="okex"
  echo "$ex"
}

infer_pair_from_name() {
  local name="${1,,}"
  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?$ ]]; then
    echo "$(normalize_exchange "${BASH_REMATCH[1]}"),$(normalize_exchange "${BASH_REMATCH[2]}")"
  fi
}

exchange_rank() {
  case "$1" in
    binance) echo "1" ;;
    okex) echo "2" ;;
    bybit) echo "3" ;;
    bitget) echo "4" ;;
    gate) echo "5" ;;
    *) echo "0" ;;
  esac
}

default_port_for_pair() {
  local open_ex="$1"
  local hedge_ex="$2"
  if [[ "$open_ex" == "binance" && "$hedge_ex" == "binance" ]]; then
    echo "18151"
    return
  fi
  local open_rank hedge_rank
  open_rank="$(exchange_rank "$open_ex")"
  hedge_rank="$(exchange_rank "$hedge_ex")"
  if [[ "$open_rank" == "0" || "$hedge_rank" == "0" ]]; then
    echo "18111"
    return
  fi
  echo $((18100 + open_rank * 10 + hedge_rank))
}

dir_name="$(basename "$BASE_DIR")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"

if [[ -z "${DEFAULT_OPEN_VENUE:-}" || -z "${DEFAULT_HEDGE_VENUE:-}" ]]; then
  if inferred="$(infer_pair_from_name "$dir_name")" && [[ -n "$inferred" ]]; then
    if [[ "${inferred%%,*}" == "${inferred##*,}" ]]; then
      DEFAULT_OPEN_VENUE="${DEFAULT_OPEN_VENUE:-${inferred%%,*}-margin}"
      DEFAULT_HEDGE_VENUE="${DEFAULT_HEDGE_VENUE:-${inferred##*,}-futures}"
    else
      DEFAULT_OPEN_VENUE="${DEFAULT_OPEN_VENUE:-${inferred%%,*}-futures}"
      DEFAULT_HEDGE_VENUE="${DEFAULT_HEDGE_VENUE:-${inferred##*,}-futures}"
    fi
  fi
fi

open_exchange="$(normalize_exchange "${DEFAULT_OPEN_VENUE%%-*}")"
hedge_exchange="$(normalize_exchange "${DEFAULT_HEDGE_VENUE%%-*}")"
DEFAULT_EXCHANGE="${DEFAULT_EXCHANGE:-${open_exchange:-okex}}"
HOST="${HOST:-0.0.0.0}"
APP_NAME="${PM2_NAME:-cross_config_server_${dir_tag}}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x "/home/ubuntu/jupyter_env/bin/python" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python"
  elif [[ -x "/home/ubuntu/jupyter_env/bin/python3" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python3"
  else
    PYTHON_BIN="python3"
  fi
fi

PORT="${PORT:-$(default_port_for_pair "$open_exchange" "$hedge_exchange")}"

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

echo "[INFO] 启动 cross_config_server (open=${DEFAULT_OPEN_VENUE:-"-"}, hedge=${DEFAULT_HEDGE_VENUE:-"-"}, port=${PORT}, namespace=${NAMESPACE})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  PORT="$PORT" HOST="$HOST" DEFAULT_EXCHANGE="$DEFAULT_EXCHANGE" \
  DEFAULT_OPEN_VENUE="${DEFAULT_OPEN_VENUE:-}" DEFAULT_HEDGE_VENUE="${DEFAULT_HEDGE_VENUE:-}" \
  npx pm2 start "$PYTHON_BIN" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    -- \
    "$APP_SCRIPT" \
    --host "$HOST" \
    --port "$PORT" \
    --default-exchange "$DEFAULT_EXCHANGE" \
    --default-open-venue "${DEFAULT_OPEN_VENUE:-}" \
    --default-hedge-venue "${DEFAULT_HEDGE_VENUE:-}"
)

echo "[INFO] 已启动：pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] 日志：npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
