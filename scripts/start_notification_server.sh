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
CFG_FILE="${BASE_DIR}/config/notification_server.env"
if [[ -f "$CFG_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CFG_FILE"
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/notification_server"
  "${BASE_DIR}/target/release/notification_server"
)
BIN_PATH=""
for candidate in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$candidate" ]]; then
    BIN_PATH="$candidate"
    break
  fi
done
if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] notification_server binary not found; build/deploy first" >&2
  exit 1
fi

HOST="${NOTIFICATION_HOST:-127.0.0.1}"
PORT="${NOTIFICATION_PORT:-18100}"
DRY_RUN="${NOTIFICATION_DRY_RUN:-0}"
case "${DRY_RUN,,}" in
  1|true|yes|on) DRY_RUN_ENABLED=1 ;;
  0|false|no|off) DRY_RUN_ENABLED=0 ;;
  *)
    echo "[ERROR] invalid NOTIFICATION_DRY_RUN=${DRY_RUN}; expected true/false or 1/0" >&2
    exit 1
    ;;
esac
if [[ "$DRY_RUN_ENABLED" -eq 0 ]]; then
  if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
    echo "[ERROR] TELEGRAM_BOT_TOKEN is required unless NOTIFICATION_DRY_RUN=1" >&2
    exit 1
  fi
  if [[ -z "${TELEGRAM_CHAT_ID:-}" ]]; then
    echo "[ERROR] TELEGRAM_CHAT_ID is required unless NOTIFICATION_DRY_RUN=1" >&2
    exit 1
  fi
fi

APP_NAME="${PM2_NAME:-notification_server}"

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

echo "[INFO] starting notification_server app=${APP_NAME} namespace=${NAMESPACE} addr=${HOST}:${PORT} provider=telegram dry_run=${DRY_RUN}"
(
  cd "$BASE_DIR"
  NOTIFICATION_HOST="$HOST" \
  NOTIFICATION_PORT="$PORT" \
  NOTIFICATION_DRY_RUN="$DRY_RUN" \
  NOTIFICATION_API_TOKEN="${NOTIFICATION_API_TOKEN:-}" \
  NOTIFICATION_QUEUE_CAPACITY="${NOTIFICATION_QUEUE_CAPACITY:-1024}" \
  NOTIFICATION_MAX_MESSAGE_CHARS="${NOTIFICATION_MAX_MESSAGE_CHARS:-4096}" \
  NOTIFICATION_REQUEST_TIMEOUT_MS="${NOTIFICATION_REQUEST_TIMEOUT_MS:-5000}" \
  NOTIFICATION_RETRY_ATTEMPTS="${NOTIFICATION_RETRY_ATTEMPTS:-3}" \
  NOTIFICATION_RETRY_BASE_DELAY_MS="${NOTIFICATION_RETRY_BASE_DELAY_MS:-500}" \
  TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}" \
  TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}" \
  TELEGRAM_MESSAGE_THREAD_ID="${TELEGRAM_MESSAGE_THREAD_ID:-}" \
  TELEGRAM_DISABLE_NOTIFICATION="${TELEGRAM_DISABLE_NOTIFICATION:-0}" \
  RUST_LOG="${RUST_LOG:-info}" \
  npx pm2 start "$BIN_PATH" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    --time \
    --restart-delay 3000 \
    --kill-timeout 10000
)

echo "[INFO] started: npx pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] logs: npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] health: http://${HOST}:${PORT}/healthz"
