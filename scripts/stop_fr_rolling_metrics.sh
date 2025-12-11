#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dir_name="$(basename "$(cd "$SCRIPT_DIR/.." && pwd)")"

exchange=""
case "$dir_name" in
  *binance*|*BINANCE*) exchange="binance" ;;
  *okex*|*OKEX*) exchange="okex" ;;
  *bybit*|*BYBIT*) exchange="bybit" ;;
  *bitget*|*BITGET*) exchange="bitget" ;;
  *gate*|*GATE*) exchange="gate" ;;
esac

if [[ -z "$exchange" ]]; then
  echo "[ERROR] 无法从目录名推断交易所 (dir=$dir_name)"
  exit 1
fi

PM2_NAME="fr_rm_${exchange}"
echo "[INFO] Stopping $PM2_NAME"
npx pm2 delete "$PM2_NAME" >/dev/null 2>&1 || true
