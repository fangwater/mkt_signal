#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

APP_SCRIPT="${BASE_DIR}/order_query/run_server.sh"
if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] 未找到 order_query/run_server.sh (期望路径: $APP_SCRIPT)"
  exit 1
fi

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-18080}"
API_BASE="${API_BASE:-}"
DATA_DIR="${DATA_DIR:-${BASE_DIR}/data/order_query}"

APP_NAME="${PM2_NAME:-order_query}"

if [[ -z "$API_BASE" ]]; then
  if [[ "$PORT" =~ ^[0-9]+$ ]] && [[ "$PORT" -ge 10000 ]]; then
    API_BASE="http://127.0.0.1:$((PORT - 10000))"
  else
    API_BASE="http://127.0.0.1:8089"
  fi
fi

echo "[INFO] 启动 order_query (port=${PORT}, namespace=${NAMESPACE})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

HOST="$HOST" PORT="$PORT" API_BASE="$API_BASE" DATA_DIR="$DATA_DIR" \
npx pm2 start "$APP_SCRIPT" \
  --name "$APP_NAME" \
  --namespace "$NAMESPACE" \
  --interpreter none \
  -- \
  --host "$HOST" \
  --port "$PORT" \
  --api-base "$API_BASE" \
  --data-dir "$DATA_DIR"

echo "[INFO] 已启动：npx pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] 日志：npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
