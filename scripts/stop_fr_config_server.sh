#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

dir_name="$(basename "$BASE_DIR")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
detected_exchange=""
case "$dir_name" in
  *okex*|*OKEX*) detected_exchange="okex" ;;
  *bybit*|*BYBIT*) detected_exchange="bybit" ;;
  *bitget*|*BITGET*) detected_exchange="bitget" ;;
  *gate*|*GATE*) detected_exchange="gate" ;;
  *binance*|*BINANCE*) detected_exchange="binance" ;;
esac

EXCHANGE="${EXCHANGE:-${detected_exchange:-okex}}"
APP_NAME="${PM2_NAME:-fr_config_server_${dir_tag}}"

echo "[INFO] 停止 fr_config_server (name=${APP_NAME}, namespace=${NAMESPACE})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] 已停止。"
