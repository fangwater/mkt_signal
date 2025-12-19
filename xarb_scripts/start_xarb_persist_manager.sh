#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# 可选：设置 PM2 namespace（默认使用部署目录名，可用环境变量覆盖）
PM2_NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

BIN_CANDIDATES=(
  "${BASE_DIR}/persist_manager"
  "${SCRIPT_DIR}/persist_manager"
  "${BASE_DIR}/target/release/persist_manager"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] persist_manager binary not found. Build/deploy first."
  exit 1
fi

usage() {
  cat <<'EOF'
用法: xarb_scripts/start_xarb_persist_manager.sh --port <PORT>

说明:
  - 需要在部署目录存在 env.sh（包含 IPC_NAMESPACE），否则 persist_manager 会 panic。
  - 会基于部署目录名推断 open/hedge exchange（目录名需形如 <open>-<hedge>-xarb-...）。
  - 会以 PM2 启动 1 个进程：persist_manager_xarb_<open>_<hedge>

示例:
  ./xarb_scripts/start_xarb_persist_manager.sh --port 8088
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

PORT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      PORT="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done
if [[ -z "$PORT" ]]; then
  echo "[ERROR] 缺少必填参数：--port <PORT>"
  usage
  exit 1
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[ERROR] 未找到 env.sh：${ENV_FILE}"
  echo "[ERROR] 请先生成并配置：scripts/deploy_setup_env_xarb.sh --env-name $(basename "${BASE_DIR}") --open-venue <...> --hedge-venue <...>"
  exit 1
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

PROC_NAME="persist_manager_xarb_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}"
RUST_LOG="${RUST_LOG:-info}"

mkdir -p "${BASE_DIR}/data/persist_manager" >/dev/null 2>&1 || true

echo "[INFO] Restarting ${PROC_NAME} (namespace=${PM2_NAMESPACE} port=${PORT})"
npx pm2 delete "$PROC_NAME" --namespace "$PM2_NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
    --name "$PROC_NAME" \
    --namespace "$PM2_NAMESPACE" \
    -- \
    --port "$PORT"
)

echo ""
echo "[INFO] Started persist_manager"
echo "Namespace: ${PM2_NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${PM2_NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${PM2_NAMESPACE}"
