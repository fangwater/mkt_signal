#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# 可选：设置 PM2 namespace（默认使用部署目录名，可用环境变量覆盖）
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

BIN_CANDIDATES=(
  "${BASE_DIR}/account_monitor"
  "${BASE_DIR}/scripts/account_monitor"
  "${BASE_DIR}/target/release/account_monitor"
  "${SCRIPT_DIR}/account_monitor"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] account_monitor binary not found. Deploy/build first." >&2
  echo "[ERROR] Expected one of:" >&2
  printf '  - %s\n' "${BIN_CANDIDATES[@]}" >&2
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
case "$dir_name" in
  okex_fr_*|*okex*|*OKEX*) EXCHANGE="okex" ;;
  binance_fr_*|*binance*|*BINANCE*) EXCHANGE="binance" ;;
  gate_fr_*|*gate*|*GATE*) EXCHANGE="gate" ;;
  *)
    echo "[ERROR] 无法从部署目录名推断 exchange: ${dir_name} (期望 okex_fr_* / binance_fr_* / gate_fr_*)" >&2
    exit 1
    ;;
esac

PM2_NAME="${PM2_NAME:-account_monitor_${dir_tag}}"

echo "[INFO] 使用 PM2 启动 ${PM2_NAME} (namespace: ${NAMESPACE})"
npx pm2 delete "$PM2_NAME" --namespace "$NAMESPACE" 2>/dev/null || true
npx pm2 start "$BIN_PATH" --name "$PM2_NAME" --namespace "$NAMESPACE"

echo "[INFO] ${PM2_NAME} 已启动"
echo "[INFO] Logs: npx pm2 logs --namespace ${NAMESPACE} ${PM2_NAME}"
