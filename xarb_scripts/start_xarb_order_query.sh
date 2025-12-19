#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PM2_NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

APP_SCRIPT="${BASE_DIR}/order_query/run_server.sh"
if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] order_query/run_server.sh not found in ${BASE_DIR}. Deploy first."
  exit 1
fi

usage() {
  cat <<'EOF'
用法: xarb_scripts/start_xarb_order_query.sh [--port 18080] [--host 0.0.0.0] [--api-base http://127.0.0.1:8089]

说明:
  - 会基于部署目录名推断 open/hedge exchange（目录名需形如 <open>-<hedge>-xarb-...）。
  - 会以 PM2 启动 1 个进程：order_query_xarb_<open>_<hedge>
  - 默认 data_dir: ./data/order_query （相对部署目录）
  - Python 解释器由 order_query/run_server.sh 选择（默认优先 /home/ubuntu/jupyter_env/bin/python，也可通过环境变量 PYTHON_BIN 覆盖）
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-18080}"
API_BASE="${API_BASE:-}"
DATA_DIR="${DATA_DIR:-${BASE_DIR}/data/order_query}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="${2:-0.0.0.0}"
      shift 2
      ;;
    --port)
      PORT="${2:-18080}"
      shift 2
      ;;
    --api-base)
      API_BASE="${2:-http://127.0.0.1:8089}"
      shift 2
      ;;
    --data-dir)
      DATA_DIR="${2:-${BASE_DIR}/data/order_query}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$API_BASE" ]]; then
  if [[ "$PORT" =~ ^[0-9]+$ ]] && [[ "$PORT" -ge 10000 ]]; then
    API_BASE="http://127.0.0.1:$((PORT - 10000))"
  else
    API_BASE="http://127.0.0.1:8089"
  fi
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

OPEN_EXCHANGE=""
HEDGE_EXCHANGE=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
  OPEN_EXCHANGE="${BASH_REMATCH[1]}"
  HEDGE_EXCHANGE="${BASH_REMATCH[2]}"
fi

if [[ "$OPEN_EXCHANGE" == "okx" ]]; then
  OPEN_EXCHANGE="okex"
fi
if [[ "$HEDGE_EXCHANGE" == "okx" ]]; then
  HEDGE_EXCHANGE="okex"
fi

if [[ -z "$OPEN_EXCHANGE" || -z "$HEDGE_EXCHANGE" || "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 open/hedge (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
  exit 1
fi

PROC_NAME="order_query_xarb_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}"

echo "[INFO] Restarting ${PROC_NAME} (namespace=${PM2_NAMESPACE} port=${PORT})"
npx pm2 delete "$PROC_NAME" --namespace "$PM2_NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  HOST="$HOST" PORT="$PORT" API_BASE="$API_BASE" DATA_DIR="$DATA_DIR" \
  npx pm2 start "$APP_SCRIPT" \
    --name "$PROC_NAME" \
    --namespace "$PM2_NAMESPACE" \
    --interpreter none \
    -- \
    --host "$HOST" \
    --port "$PORT" \
    --api-base "$API_BASE" \
    --data-dir "$DATA_DIR"
)

echo ""
echo "[INFO] Started order_query"
echo "Namespace: ${PM2_NAMESPACE}"
echo "URL: http://127.0.0.1:${PORT}/"
echo "Logs: npx pm2 logs --namespace ${PM2_NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${PM2_NAMESPACE}"
