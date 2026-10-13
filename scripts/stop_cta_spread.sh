#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_NAME="$(basename "$BASE_DIR")"
EXECUTE=0
if [[ $# -eq 1 && "$1" == "--execute" ]]; then
  EXECUTE=1
elif [[ $# -ne 0 ]]; then
  echo "Usage: scripts/stop_cta_spread.sh [--execute]" >&2
  exit 1
fi
[[ "$ENV_NAME" =~ ^binance-cta-spread-[a-z0-9][a-z0-9_-]*$ ]] || {
  echo "[ERROR] CTA spread env must be named binance-cta-spread-<tag>: $ENV_NAME" >&2
  exit 1
}
echo "[PLAN] env=${ENV_NAME} exchange=binance symbol_scope=config/cta_spread.json live_mutation=stop-processes"
echo "[PLAN] signal -> factor publisher -> dashboard -> config server"
if [[ "$EXECUTE" -ne 1 ]]; then
  echo "[DRY-RUN] no process was stopped; pass --execute to proceed"
  exit 0
fi

cd "$BASE_DIR"
"${BASE_DIR}/scripts/stop_cta_spread_signal.sh"
"${BASE_DIR}/scripts/stop_cta_spread_factor_model_1m_pub.sh"
"${BASE_DIR}/scripts/stop_cta_spread_dashboard.sh"
"${BASE_DIR}/scripts/stop_cta_spread_config_server.sh"
echo "[INFO] CTA spread stack stopped"
