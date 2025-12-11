#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_CANDIDATES=(
  "${SCRIPT_DIR}/../rolling_metrics"
  "${SCRIPT_DIR}/rolling_metrics"
  "${SCRIPT_DIR}/../target/release/rolling_metrics"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] rolling_metrics binary not found. Build first."
  exit 1
fi

dir_name="$(basename "$(cd "$SCRIPT_DIR/.." && pwd)")"
exchange=""
case "$dir_name" in
  *binance*|*BINANCE*) exchange="binance" ;;
  *okex*|*OKEX*) exchange="okex" ;;
  *bybit*|*BYBIT*) exchange="bybit" ;;
  *bitget*|*BITGET*) exchange="bitget" ;;
  *gate*|*GATE*) exchange="gate" ;;
  *)
    echo "[ERROR] 无法从目录名推断交易所 (dir=$dir_name)，请在脚本中补充映射。"
    exit 1
    ;;
esac

case "$exchange" in
  binance)
    OPEN_VENUE="binance-margin"
    HEDGE_VENUE="binance-futures"
    ;;
  okex)
    OPEN_VENUE="okex-margin"
    HEDGE_VENUE="okex-futures"
    ;;
  bybit)
    OPEN_VENUE="bybit-margin"
    HEDGE_VENUE="bybit-futures"
    ;;
  bitget)
    OPEN_VENUE="bitget-margin"
    HEDGE_VENUE="bitget-futures"
    ;;
  gate)
    OPEN_VENUE="gate-margin"
    HEDGE_VENUE="gate-futures"
    ;;
  *)
    echo "[ERROR] 未知交易所: $exchange"
    exit 1
    ;;
esac

PM2_NAME="fr_rm_${exchange}"

echo "[INFO] Restarting $PM2_NAME (open=$OPEN_VENUE hedge=$HEDGE_VENUE)"
npx pm2 delete "$PM2_NAME" >/dev/null 2>&1 || true

npx pm2 start "$BIN_PATH" \
  --name "$PM2_NAME" \
  -- \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE"

echo "[INFO] Started $PM2_NAME. Logs: npx pm2 logs $PM2_NAME"
