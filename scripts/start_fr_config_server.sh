#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

APP_SCRIPT="${SCRIPT_DIR}/fr_config_server.py"
if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] 未找到 fr_config_server.py (期望路径: $APP_SCRIPT)"
  exit 1
fi

dir_name="$(basename "$BASE_DIR")"
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
PORT="${PORT:-8000}"
DEFAULT_EXCHANGE="${DEFAULT_EXCHANGE:-$EXCHANGE}"
APP_NAME="${PM2_NAME:-fr_config_server_${EXCHANGE}}"

echo "[INFO] 启动 fr_config_server (exchange=${DEFAULT_EXCHANGE}, port=${PORT}, namespace=${NAMESPACE})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

PORT="$PORT" HOST="$HOST" DEFAULT_EXCHANGE="$DEFAULT_EXCHANGE" \
npx pm2 start "$APP_SCRIPT" \
  --name "$APP_NAME" \
  --namespace "$NAMESPACE" \
  --interpreter python3 \
  -- \
  --host "$HOST" \
  --port "$PORT" \
  --default-exchange "$DEFAULT_EXCHANGE"

echo "[INFO] 已启动：pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] 日志：npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
