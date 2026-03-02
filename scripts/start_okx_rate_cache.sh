#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

RATE_HOST="127.0.0.1"
RATE_PORT="28901"
EXPECTED_RATE_URL="http://${RATE_HOST}:${RATE_PORT}/rates"
APP_NAME="${PM2_NAME:-okx_rate_cache_${NAMESPACE}}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
APP_SCRIPT="${SCRIPT_DIR}/okx_rate_cache_server.py"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] 未找到 okx_rate_cache_server.py (期望路径: $APP_SCRIPT)" >&2
  exit 1
fi

if [[ "${NAMESPACE}" != okex_fr_* ]]; then
  echo "[WARN] 当前 namespace 不是 okex_fr_*: ${NAMESPACE}" >&2
fi

if [[ -n "${OKEX_LOAN_RATE_URL:-}" && "${OKEX_LOAN_RATE_URL}" != "${EXPECTED_RATE_URL}" ]]; then
  echo "[ERROR] OKEX_LOAN_RATE_URL 必须固定为 ${EXPECTED_RATE_URL}（当前: ${OKEX_LOAN_RATE_URL}）" >&2
  echo "[ERROR] 请更新 ${ENV_FILE} 后重试。" >&2
  exit 1
fi

echo "[INFO] 启动 okx_rate_cache (name=${APP_NAME}, namespace=${NAMESPACE}, bind=${RATE_HOST}:${RATE_PORT})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  npx pm2 start "$PYTHON_BIN" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    -- \
    "$APP_SCRIPT" \
    --host "$RATE_HOST" \
    --port "$RATE_PORT"
)

echo "[INFO] 已启动: npx pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] 日志: npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] 健康检查: curl ${EXPECTED_RATE_URL}"
