#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ -f "${BASE_DIR}/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${BASE_DIR}/env.sh"
fi
if [[ "${PRODUCT_TYPE:-USDT-FUTURES}" == "COIN-FUTURES" ]]; then
  DEFAULT_PM2_NAME="bitget_position_tier_sidecar_coin"
else
  DEFAULT_PM2_NAME="bitget_position_tier_sidecar"
fi
PM2_NAME="${PM2_NAME:-$DEFAULT_PM2_NAME}"
echo "[INFO] stopping ${PM2_NAME}"
npx pm2 delete "$PM2_NAME" </dev/null >/dev/null 2>&1 || true
