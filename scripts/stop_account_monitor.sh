#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

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

if npx pm2 describe "$PM2_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] 停止 $PM2_NAME (namespace: $NAMESPACE)"
  npx pm2 delete "$PM2_NAME" --namespace "$NAMESPACE"
else
  echo "[WARN] 未找到 PM2 进程 $PM2_NAME (namespace: $NAMESPACE)"
fi
