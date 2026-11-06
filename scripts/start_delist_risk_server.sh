#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi
CFG_FILE="${BASE_DIR}/config/delist_risk_server.env"
if [[ -f "$CFG_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CFG_FILE"
fi
CTA_DB_FILE="${BASE_DIR}/config/cta_manager_reader.env"
if [[ -f "$CTA_DB_FILE" ]]; then
  if [[ "$(stat -c '%u:%a' "$CTA_DB_FILE")" != "$(id -u):600" ]]; then
    echo "[ERROR] CTA Manager reader config must be owned by the service user and mode 600" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$CTA_DB_FILE"
fi

FR_DINGTALK_FILE="${BASE_DIR}/config/fr_dingtalk.env"
if [[ -f "$FR_DINGTALK_FILE" ]]; then
  if [[ "$(stat -c '%u:%a' "$FR_DINGTALK_FILE")" != "$(id -u):600" ]]; then
    echo "[ERROR] FR DingTalk config must be owned by the service user and mode 600" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$FR_DINGTALK_FILE"
fi

CPU_PREFIX=()
if [[ -n "${DELIST_CORE:-}" ]]; then
  TASKSET_BIN="$(command -v taskset || true)"
  if [[ -z "$TASKSET_BIN" ]] || ! "$TASKSET_BIN" -c "$DELIST_CORE" true; then
    echo "[ERROR] DELIST_CORE requires taskset and an available CPU list" >&2
    exit 1
  fi
  CPU_PREFIX=("$TASKSET_BIN" -c "$DELIST_CORE")
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/delist_risk_server"
  "${BASE_DIR}/target/release/delist_risk_server"
)
BIN_PATH=""
for candidate in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$candidate" ]]; then
    BIN_PATH="$candidate"
    break
  fi
done
if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] delist_risk_server binary not found; build/deploy first" >&2
  exit 1
fi
APP_COMMAND=("${CPU_PREFIX[@]}" "$BIN_PATH")

BIND="${DELIST_BIND:-127.0.0.1:8787}"
PORT="${BIND##*:}"
APP_NAME="${PM2_NAME:-delist_risk_server}"
SG_TUNNEL_NAME="${DELIST_SG_REDIS_TUNNEL_NAME:-delist_sg_redis_tunnel}"
SG_TUNNEL_PORT="${DELIST_SG_REDIS_TUNNEL_PORT:-16379}"
BOOK_PATH="${BASE_DIR}/data/delist_risk.json"
mkdir -p "${BASE_DIR}/data"

port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "(:|\\])${port}\$"
    return $?
  fi
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  return 1
}

stop_existing_app() {
  npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
  for _ in {1..10}; do
    if ! port_in_use "$PORT"; then
      break
    fi
    sleep 1
  done
  if port_in_use "$PORT"; then
    echo "[ERROR] port ${PORT} is still in use; aborting" >&2
    exit 1
  fi
}

ARGS=(
  --bind "$BIND"
  --access-config "${DELIST_ACCESS_CONFIG:-${BASE_DIR}/config/delist_access.json}"
  --book "$BOOK_PATH"
  --days "${DELIST_DAYS:-30}"
  --announcement-interval-secs "${DELIST_ANNOUNCEMENT_INTERVAL_SECS:-86400}"
  --official-interval-secs "${DELIST_OFFICIAL_INTERVAL_SECS:-86400}"
  --listing-interval-secs "${DELIST_LISTING_INTERVAL_SECS:-86400}"
  --position-risk-interval-secs "${DELIST_POSITION_RISK_INTERVAL_SECS:-60}"
  --unimmr-list-interval-secs "${DELIST_UNIMMR_LIST_INTERVAL_SECS:-60}"
  --position-risk-threshold-usdt "${DELIST_POSITION_RISK_THRESHOLD_USDT:-50}"
  --position-snapshot-max-age-secs "${DELIST_POSITION_SNAPSHOT_MAX_AGE_SECS:-120}"
  --exposure-interval-secs "${DELIST_EXPOSURE_INTERVAL_SECS:-10}"
  --exposure-max-age-secs "${DELIST_EXPOSURE_MAX_AGE_SECS:-90}"
  --exposure-persistence-secs "${DELIST_EXPOSURE_PERSISTENCE_SECS:-30}"
  --data-persistence-secs "${DELIST_DATA_PERSISTENCE_SECS:-60}"
  --exposure-asset-limit-usdt "${DELIST_EXPOSURE_ASSET_LIMIT_USDT:-100}"
  --exposure-account-limit-usdt "${DELIST_EXPOSURE_ACCOUNT_LIMIT_USDT:-500}"
  --nav-strategies-url "${DELIST_NAV_STRATEGIES_URL:-http://127.0.0.1:4191/nav-api/strategies}"
  --nav-strategy-interval-secs "${DELIST_NAV_STRATEGY_INTERVAL_SECS:-60}"
  --flatten-window-hours "${DELIST_FLATTEN_WINDOW_HOURS:-24}"
  --flatten-manual-threshold-usdt "${DELIST_FLATTEN_MANUAL_THRESHOLD_USDT:-1000}"
  --flatten-env-root "${DELIST_FLATTEN_ENV_ROOT:-/home/ubuntu}"
  --flatten-timeout-secs "${DELIST_FLATTEN_TIMEOUT_SECS:-300}"
  --snapshot-base-url "${DELIST_SNAPSHOT_BASE_URL:-http://127.0.0.1:4191}"
  --redis-backup-dir "${DELIST_REDIS_BACKUP_DIR:-${BASE_DIR}/data/redis_backups}"
  --redis-backup-keep "${DELIST_REDIS_BACKUP_KEEP:-30}"
  --redis-backup-env-root "${DELIST_REDIS_BACKUP_ENV_ROOT:-${BASE_DIR}/..}"
  --redis-backup-sg-ssh-host "${DELIST_SG_REDIS_SSH_HOST:-sg}"
  --cta-backup-dir "${DELIST_CTA_BACKUP_DIR:-${BASE_DIR}/data/cta_backups}"
  --mm-backup-dir "${DELIST_MM_BACKUP_DIR:-${BASE_DIR}/data/mm_backups}"
  --cta-factor-env "${DELIST_CTA_FACTOR_ENV:-${DELIST_REDIS_BACKUP_ENV_ROOT:-${BASE_DIR}/..}/cta-prep-binance-futures}"
  --cta-spread-factor-env "${DELIST_CTA_SPREAD_FACTOR_ENV:-${DELIST_REDIS_BACKUP_ENV_ROOT:-${BASE_DIR}/..}/cta-factor-spread}"
  --llm-max "${DELIST_LLM_MAX:-0}"
  --web-dir "${BASE_DIR}/web/delist_risk"
)
if [[ "${DELIST_FR_NOTIFY_ENABLED:-1}" == "1" && -n "${FR_DINGTALK_WEBHOOK_URL:-}" ]]; then
  ARGS+=(--fr-notify)
fi
if [[ -n "${DELIST_FORCE_LLM_IDS:-}" ]]; then
  ARGS+=(--force-llm-ids "${DELIST_FORCE_LLM_IDS}")
fi
if [[ -n "${DELIST_NAV_USERNAME:-}" ]]; then
  ARGS+=(--nav-username "${DELIST_NAV_USERNAME}")
fi
if [[ -n "${DELIST_NAV_PASSWORD:-}" ]]; then
  ARGS+=(--nav-password "${DELIST_NAV_PASSWORD}")
fi
if [[ -n "${DELIST_REDIS_URL:-}" ]]; then
  ARGS+=(--redis "${DELIST_REDIS_URL}")
fi
if [[ -n "${DELIST_SG_FLATTEN_SSH_HOST:-}" ]]; then
  ARGS+=(--sg-flatten-ssh-host "${DELIST_SG_FLATTEN_SSH_HOST}")
fi
if [[ -n "${DELIST_SG_REDIS_URL:-}" ]]; then
  if [[ "${DELIST_SG_REDIS_URL}" != "redis://127.0.0.1:${SG_TUNNEL_PORT}/0" ]]; then
    echo "[ERROR] DELIST_SG_REDIS_URL must use redis://127.0.0.1:${SG_TUNNEL_PORT}/0" >&2
    echo "[ERROR] keep the SG Redis listener private and use the managed SSH tunnel" >&2
    exit 1
  fi
  SG_SSH_HOST="${DELIST_SG_REDIS_SSH_HOST:-sg}"
  SSH_BIN="$(command -v ssh || true)"
  if [[ -z "$SSH_BIN" ]]; then
    echo "[ERROR] ssh is required for the SG Redis tunnel" >&2
    exit 1
  fi
  # PM2 --namespace selects the whole namespace: delete the old app before
  # creating its tunnel, or the later app cleanup deletes the new tunnel too.
  stop_existing_app
  npx pm2 delete "$SG_TUNNEL_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
  SG_FORWARDS=(-L "127.0.0.1:${SG_TUNNEL_PORT}:127.0.0.1:6379")
  if [[ -n "${DELIST_SG_WEB_TUNNEL_PORT:-}" ]]; then
    if [[ ! "$DELIST_SG_WEB_TUNNEL_PORT" =~ ^[0-9]+$ ]] || (( DELIST_SG_WEB_TUNNEL_PORT < 1024 || DELIST_SG_WEB_TUNNEL_PORT > 65535 )); then
      echo "[ERROR] invalid DELIST_SG_WEB_TUNNEL_PORT" >&2
      exit 1
    fi
    SG_FORWARDS+=(-L "127.0.0.1:${DELIST_SG_WEB_TUNNEL_PORT}:127.0.0.1:4191")
  fi
  SG_COMMAND=("${CPU_PREFIX[@]}" "$SSH_BIN")
  npx pm2 start "${SG_COMMAND[0]}" --name "$SG_TUNNEL_NAME" --namespace "$NAMESPACE" --interpreter none -- \
    "${SG_COMMAND[@]:1}" \
    -NT \
    -o BatchMode=yes \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    "${SG_FORWARDS[@]}" \
    "$SG_SSH_HOST"
  tunnel_ready=0
  for _ in {1..10}; do
    if (echo > "/dev/tcp/127.0.0.1/${SG_TUNNEL_PORT}") >/dev/null 2>&1; then
      tunnel_ready=1
      break
    fi
    sleep 1
  done
  if [[ "$tunnel_ready" -ne 1 ]]; then
    echo "[ERROR] SG Redis SSH tunnel did not listen on 127.0.0.1:${SG_TUNNEL_PORT}" >&2
    exit 1
  fi
  echo "[INFO] SG Redis tunnel ready: 127.0.0.1:${SG_TUNNEL_PORT} -> ${SG_SSH_HOST}:127.0.0.1:6379"
  ARGS+=(--sg-redis "${DELIST_SG_REDIS_URL}")
else
  stop_existing_app
fi
if [[ "${DELIST_SKIP_LLM:-0}" == "1" ]]; then
  ARGS+=(--skip-llm)
fi
if [[ "${DELIST_SKIP_ANNOUNCEMENTS:-0}" == "1" ]]; then
  ARGS+=(--skip-announcements)
fi
if [[ "${DELIST_SKIP_OFFICIAL:-0}" == "1" ]]; then
  ARGS+=(--skip-official)
fi
if [[ "${DELIST_SKIP_WS:-0}" == "1" ]]; then
  ARGS+=(--skip-ws)
fi
if [[ "${DELIST_SKIP_REDIS_BACKUP:-0}" == "1" ]]; then
  ARGS+=(--skip-redis-backup)
fi
if [[ -n "${DELIST_REDIS_BACKUP_COS_TARGET:-}" ]]; then
  ARGS+=(--redis-backup-cos-target "${DELIST_REDIS_BACKUP_COS_TARGET}")
fi
if [[ -n "${DELIST_CTA_BACKUP_COS_TARGET:-}" ]]; then
  ARGS+=(--cta-backup-cos-target "${DELIST_CTA_BACKUP_COS_TARGET}")
fi
if [[ -n "${DELIST_MM_BACKUP_COS_TARGET:-}" ]]; then
  ARGS+=(--mm-backup-cos-target "${DELIST_MM_BACKUP_COS_TARGET}")
fi
if [[ -n "${DELIST_REDIS_BACKUP_COS_TARGET:-}${DELIST_CTA_BACKUP_COS_TARGET:-}${DELIST_MM_BACKUP_COS_TARGET:-}" ]]; then
  ARGS+=(--redis-backup-cos-config "${DELIST_REDIS_BACKUP_COS_CONFIG:-${HOME}/.config/rclone/fr-cos.conf}")
fi
if [[ "${DELIST_AUTO_REMOVE_REDIS:-0}" == "1" ]]; then
  ARGS+=(--auto-remove-redis)
fi
if [[ "${DELIST_AUTO_DUMP_POSITION_RISK:-1}" == "1" ]]; then
  ARGS+=(--auto-dump-position-risk)
fi
if [[ "${DELIST_AUTO_FLATTEN_POSITION_RISK:-0}" == "1" ]]; then
  ARGS+=(--auto-flatten-position-risk)
fi

if [[ -z "${BINANCE_API_KEY:-}" || -z "${BINANCE_API_SECRET:-}" ]]; then
  unset BINANCE_API_KEY BINANCE_API_SECRET
fi

echo "[INFO] starting delist_risk_server app=${APP_NAME} namespace=${NAMESPACE} bind=${BIND}"
(
  cd "$BASE_DIR"
  DELIST_PG_URL="${DELIST_PG_URL:-}" \
  DELIST_CTA_JP_DATABASE_URL="${DELIST_CTA_JP_DATABASE_URL:-}" \
  DELIST_LLM_API_URL="${DELIST_LLM_API_URL:-}" \
  DELIST_LLM_API_KEY="${DELIST_LLM_API_KEY:-}" \
  DELIST_LLM_MODEL="${DELIST_LLM_MODEL:-gpt-5.6-luna}" \
  DELIST_LLM_REASONING_EFFORT="${DELIST_LLM_REASONING_EFFORT:-xhigh}" \
  DELIST_LLM_BACKUP_API_URL="${DELIST_LLM_BACKUP_API_URL:-}" \
  DELIST_LLM_BACKUP_API_KEY="${DELIST_LLM_BACKUP_API_KEY:-}" \
  DELIST_LLM_BACKUP_MODEL="${DELIST_LLM_BACKUP_MODEL:-}" \
  DELIST_LLM_BACKUP_REASONING_EFFORT="${DELIST_LLM_BACKUP_REASONING_EFFORT:-}" \
  DELIST_LLM_HTTP_HEADER="${DELIST_LLM_HTTP_HEADER:-}" \
  DELIST_FLATTEN_API_TOKEN="${DELIST_FLATTEN_API_TOKEN:-}" \
  PRE_TRADE_NOTIFICATION_URL="${PRE_TRADE_NOTIFICATION_URL:-}" \
  PRE_TRADE_NOTIFICATION_TIMEOUT_MS="${PRE_TRADE_NOTIFICATION_TIMEOUT_MS:-}" \
  NOTIFICATION_API_TOKEN="${NOTIFICATION_API_TOKEN:-}" \
  FR_DINGTALK_WEBHOOK_URL="${FR_DINGTALK_WEBHOOK_URL:-}" \
  FR_DINGTALK_SECRET="${FR_DINGTALK_SECRET:-}" \
  BINANCE_API_KEY="${BINANCE_API_KEY:-}" \
  BINANCE_API_SECRET="${BINANCE_API_SECRET:-}" \
  BITGET_PUBLIC_API_BASE="${BITGET_PUBLIC_API_BASE:-}" \
  OPENAI_API_KEY="${OPENAI_API_KEY:-}" \
  OPENAI_BASE_URL="${OPENAI_BASE_URL:-}" \
  RUST_LOG="${RUST_LOG:-info}" \
  npx pm2 start "${APP_COMMAND[0]}" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    --time \
    --restart-delay 3000 \
    --kill-timeout 10000 \
    -- "${APP_COMMAND[@]:1}" "${ARGS[@]}"
)

echo "[INFO] started: npx pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] logs: npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] health: http://${BIND}/healthz"
echo "[INFO] status: http://${BIND}/status"
