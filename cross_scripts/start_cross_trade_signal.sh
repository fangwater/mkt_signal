#!/usr/bin/env bash
set -euo pipefail

# cross trade_signal 启动脚本：
# - 依赖部署目录名推断 cross pair（目录名需形如 <open>-<hedge>-cross-<env>）
# - 若存在 env.sh，会自动 source（用于 account mode / venue / credentials / RUST_LOG）
# - 进程名: cross_<open>_<hedge>_<env>_trade_signal
# - 使用 PM2 namespace（默认=部署目录名，可用 PM2_NAMESPACE 覆盖）

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[WARN] 未找到 env.sh：${ENV_FILE}"
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/trade_signal"
  "${SCRIPT_DIR}/trade_signal"
  "${BASE_DIR}/target/release/trade_signal"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] trade_signal binary not found. Deploy/build first."
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

OPEN_EX=""
HEDGE_EX=""
ENV_TAG=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
  OPEN_EX="${BASH_REMATCH[1]}"
  HEDGE_EX="${BASH_REMATCH[2]}"
  ENV_TAG="${BASH_REMATCH[3]}"
fi
if [[ "$OPEN_EX" == "okx" ]]; then
  OPEN_EX="okex"
fi
if [[ "$HEDGE_EX" == "okx" ]]; then
  HEDGE_EX="okex"
fi
if [[ -z "$OPEN_EX" || -z "$HEDGE_EX" || -z "$ENV_TAG" ]]; then
  echo "[ERROR] not an cross env dir: ${dir_name} (expect <open>-<hedge>-cross-<env>)"
  exit 1
fi
ENV_TAG="$(printf '%s' "$ENV_TAG" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
if [[ -z "$ENV_TAG" ]]; then
  echo "[ERROR] invalid cross env tag in dir: ${dir_name}"
  exit 1
fi

if [[ "$OPEN_EX" == "$HEDGE_EX" ]]; then
  LEGACY_PM2_TAG="${OPEN_EX}_std"
else
  LEGACY_PM2_TAG="${OPEN_EX}_${HEDGE_EX}"
fi
PROC_NAME="cross_${OPEN_EX}_${HEDGE_EX}_${ENV_TAG}_trade_signal"
LEGACY_PROC_NAME="trade_signal_${LEGACY_PM2_TAG}"
RUST_LOG="${RUST_LOG:-info}"
QUEUE_POSITION_ENABLED="${TRADE_SIGNAL_ENABLE_QUEUE_POSITION:-0}"
QUEUE_POSITION_ENABLED="${QUEUE_POSITION_ENABLED,,}"
case "$QUEUE_POSITION_ENABLED" in
  1|true|yes|on|0|false|no|off) ;;
  *) echo "[ERROR] TRADE_SIGNAL_ENABLE_QUEUE_POSITION must be a boolean" >&2; exit 1 ;;
esac

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$LEGACY_PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" TRADE_SIGNAL_ENABLE_QUEUE_POSITION="${QUEUE_POSITION_ENABLED}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  --cwd "$BASE_DIR"

echo ""
echo "[INFO] Started trade_signal (open=${OPEN_EX} hedge=${HEDGE_EX} env=${ENV_TAG} queue_position=${QUEUE_POSITION_ENABLED})"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
