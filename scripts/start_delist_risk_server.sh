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

BIND="${DELIST_BIND:-0.0.0.0:8787}"
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

ARGS=(
  --bind "$BIND"
  --book "$BOOK_PATH"
  --days "${DELIST_DAYS:-30}"
  --announcement-interval-secs "${DELIST_ANNOUNCEMENT_INTERVAL_SECS:-3600}"
  --official-interval-secs "${DELIST_OFFICIAL_INTERVAL_SECS:-10800}"
  --llm-max "${DELIST_LLM_MAX:-0}"
  --web-dir "${BASE_DIR}/web/delist_risk"
)
if [[ -n "${DELIST_FORCE_LLM_IDS:-}" ]]; then
  ARGS+=(--force-llm-ids "${DELIST_FORCE_LLM_IDS}")
fi
if [[ -n "${DELIST_REDIS_URL:-}" ]]; then
  ARGS+=(--redis "${DELIST_REDIS_URL}")
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
  npx pm2 delete "$SG_TUNNEL_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
  npx pm2 start "$SSH_BIN" --name "$SG_TUNNEL_NAME" --namespace "$NAMESPACE" --interpreter none -- \
    -NT \
    -o BatchMode=yes \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -L "127.0.0.1:${SG_TUNNEL_PORT}:127.0.0.1:6379" \
    "$SG_SSH_HOST"
  sleep 1
  if ! (echo > "/dev/tcp/127.0.0.1/${SG_TUNNEL_PORT}") >/dev/null 2>&1; then
    echo "[ERROR] SG Redis SSH tunnel did not listen on 127.0.0.1:${SG_TUNNEL_PORT}" >&2
    exit 1
  fi
  echo "[INFO] SG Redis tunnel ready: 127.0.0.1:${SG_TUNNEL_PORT} -> ${SG_SSH_HOST}:127.0.0.1:6379"
  ARGS+=(--sg-redis "${DELIST_SG_REDIS_URL}")
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

if [[ -z "${BINANCE_API_KEY:-}" || -z "${BINANCE_API_SECRET:-}" ]]; then
  unset BINANCE_API_KEY BINANCE_API_SECRET
fi

echo "[INFO] starting delist_risk_server app=${APP_NAME} namespace=${NAMESPACE} bind=${BIND}"
(
  cd "$BASE_DIR"
  DELIST_PG_URL="${DELIST_PG_URL:-}" \
  DELIST_LLM_API_URL="${DELIST_LLM_API_URL:-}" \
  DELIST_LLM_API_KEY="${DELIST_LLM_API_KEY:-}" \
  DELIST_LLM_MODEL="${DELIST_LLM_MODEL:-gpt-5.6-luna}" \
  DELIST_LLM_REASONING_EFFORT="${DELIST_LLM_REASONING_EFFORT:-xhigh}" \
  DELIST_LLM_BACKUP_API_URL="${DELIST_LLM_BACKUP_API_URL:-}" \
  DELIST_LLM_BACKUP_API_KEY="${DELIST_LLM_BACKUP_API_KEY:-}" \
  DELIST_LLM_BACKUP_MODEL="${DELIST_LLM_BACKUP_MODEL:-}" \
  DELIST_LLM_BACKUP_REASONING_EFFORT="${DELIST_LLM_BACKUP_REASONING_EFFORT:-}" \
  DELIST_LLM_HTTP_HEADER="${DELIST_LLM_HTTP_HEADER:-}" \
  OPENAI_API_KEY="${OPENAI_API_KEY:-}" \
  OPENAI_BASE_URL="${OPENAI_BASE_URL:-}" \
  RUST_LOG="${RUST_LOG:-info}" \
  npx pm2 start "$BIN_PATH" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    --time \
    --restart-delay 3000 \
    --kill-timeout 10000 \
    -- "${ARGS[@]}"
)

echo "[INFO] started: npx pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] logs: npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] health: http://${BIND}/healthz"
echo "[INFO] status: http://${BIND}/status"
