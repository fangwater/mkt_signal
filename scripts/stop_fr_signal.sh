#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# 可选：设置 PM2 namespace（默认使用部署目录名，可用环境变量覆盖）
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

# 根据部署目录名推断 exchange（可通过参数覆盖）
dir_name="$(basename "${BASE_DIR}")"
DEFAULT_EXCHANGE=""
case "$dir_name" in
  *okex*|*OKEX*) DEFAULT_EXCHANGE="okex" ;;
  *bybit*|*BYBIT*) DEFAULT_EXCHANGE="bybit" ;;
  *bitget*|*BITGET*) DEFAULT_EXCHANGE="bitget" ;;
  *gate*|*GATE*) DEFAULT_EXCHANGE="gate" ;;
  *binance*|*BINANCE*) DEFAULT_EXCHANGE="binance" ;;
esac

EXCHANGE="${1:-$DEFAULT_EXCHANGE}"
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 未提供 exchange，且无法从目录名推断 (dir=${dir_name})"
  echo "用法: $0 [binance|okex|bybit|bitget|gate]"
  exit 1
fi

PROC_NAME="fr_signal_${EXCHANGE}"

echo "[INFO] Deleting ${PROC_NAME} (namespace=${NAMESPACE})"
if npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE"; then
  echo "[INFO] Deleted ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found in namespace ${NAMESPACE}"
fi

echo ""
echo "[INFO] Remaining processes: npx pm2 status --namespace ${NAMESPACE}"
