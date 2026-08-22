#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP="${SCRIPT_DIR}/bitget_position_tier_sidecar.py"

PRODUCT_TYPE="${PRODUCT_TYPE:-USDT-FUTURES}"
if [[ "$PRODUCT_TYPE" == "COIN-FUTURES" ]]; then
  DEFAULT_PM2_NAME="bitget_position_tier_sidecar_coin"
else
  DEFAULT_PM2_NAME="bitget_position_tier_sidecar"
fi
PM2_NAME="${PM2_NAME:-$DEFAULT_PM2_NAME}"
PM2_NAMESPACE="${PM2_NAMESPACE:-risk_sidecar}"
POOL_KEY="${POOL_KEY:-bitget_position_tier_pool:envs}"
CACHE_KEY="${CACHE_KEY:-bitget_position_tier_cache:${PRODUCT_TYPE}}"
BATCH_SIZE="${BATCH_SIZE:-3}"
INTERVAL_SEC="${INTERVAL_SEC:-20}"
SYMBOL_COOLDOWN_SEC="${SYMBOL_COOLDOWN_SEC:-1800}"
SYMBOL_SLEEP_MS="${SYMBOL_SLEEP_MS:-150}"
TIMEOUT="${TIMEOUT:-10}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-0}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"

pm2_args=(
  start "$APP"
  --name "$PM2_NAME"
  --namespace "$PM2_NAMESPACE"
  --interpreter python3
  --
  --pool-key "$POOL_KEY"
  --cache-key "$CACHE_KEY"
  --product-type "$PRODUCT_TYPE"
  --batch-size "$BATCH_SIZE"
  --interval-sec "$INTERVAL_SEC"
  --symbol-cooldown-sec "$SYMBOL_COOLDOWN_SEC"
  --symbol-sleep-ms "$SYMBOL_SLEEP_MS"
  --timeout "$TIMEOUT"
  --redis-host "$REDIS_HOST"
  --redis-port "$REDIS_PORT"
  --redis-db "$REDIS_DB"
)

if [[ -n "$REDIS_PASSWORD" ]]; then
  pm2_args+=(--redis-password "$REDIS_PASSWORD")
fi

echo "[INFO] starting ${PM2_NAMESPACE}:${PM2_NAME}"
echo "[INFO] pool_key=${POOL_KEY} cache_key=${CACHE_KEY} redis=${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB}"
REDIS_HOST="$REDIS_HOST" REDIS_PORT="$REDIS_PORT" REDIS_DB="$REDIS_DB" REDIS_PASSWORD="$REDIS_PASSWORD" \
  npx pm2 "${pm2_args[@]}"
