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

APP_NAME="${PM2_NAME:-delist_risk_server}"
SG_TUNNEL_NAME="${DELIST_SG_REDIS_TUNNEL_NAME:-delist_sg_redis_tunnel}"

echo "[INFO] stopping delist_risk_server app=${APP_NAME} namespace=${NAMESPACE}"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
npx pm2 delete "$SG_TUNNEL_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] stopped"
