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
  echo "[ERROR] rolling_metrics binary not found. Build/deploy first."
  exit 1
fi

dir_name="$(basename "$(cd "$SCRIPT_DIR/.." && pwd)")"
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

case "$OPEN_EXCHANGE" in
  binance|okex|bybit|bitget|gate) ;;
  *)
    echo "[ERROR] 无法从目录名推断 open exchange (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
    exit 1
    ;;
esac
case "$HEDGE_EXCHANGE" in
  binance|okex|bybit|bitget|gate) ;;
  *)
    echo "[ERROR] 无法从目录名推断 hedge exchange (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
    exit 1
    ;;
esac
if [[ "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] xarb 需要跨所：open=$OPEN_EXCHANGE hedge=$HEDGE_EXCHANGE"
  exit 1
fi

OPEN_VENUE="${OPEN_EXCHANGE}-futures"
HEDGE_VENUE="${HEDGE_EXCHANGE}-futures"
PM2_NAME="xarb_rm_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}"

echo "[INFO] Restarting $PM2_NAME (open=$OPEN_VENUE hedge=$HEDGE_VENUE)"
npx pm2 delete "$PM2_NAME" >/dev/null 2>&1 || true

npx pm2 start "$BIN_PATH" \
  --name "$PM2_NAME" \
  -- \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE"

echo "[INFO] Started $PM2_NAME. Logs: npx pm2 logs $PM2_NAME"

