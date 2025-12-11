#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# 可选：设置 PM2 namespace（默认使用部署目录名，可用环境变量覆盖）
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

# 候选二进制位置：部署目录优先，其次源码目录
BIN_CANDIDATES=(
  "${SCRIPT_DIR}/fr_signal"
  "${SCRIPT_DIR}/../fr_signal"
  "${SCRIPT_DIR}/target/release/fr_signal"
  "${SCRIPT_DIR}/../target/release/fr_signal"
  "${BASE_DIR}/fr_signal"
  "${BASE_DIR}/target/release/fr_signal"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] fr_signal binary not found. Build first with: cargo build --release --bin fr_signal"
  exit 1
fi

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
RUST_LOG="${RUST_LOG:-info}"

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  --exchange "$EXCHANGE"

echo ""
echo "[INFO] Started fr_signal for exchange=${EXCHANGE}"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
