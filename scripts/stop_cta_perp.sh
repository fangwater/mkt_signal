#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_NAME="$(basename "$BASE_DIR")"
EXECUTE=0
if [[ $# -eq 1 && "$1" == "--execute" ]]; then
  EXECUTE=1
elif [[ $# -ne 0 ]]; then
  echo "Usage: scripts/stop_cta_perp.sh [--execute]" >&2
  exit 1
fi
[[ "$ENV_NAME" =~ ^binance-cta-perp-[a-z0-9][a-z0-9_-]*$ ]] || {
  echo "[ERROR] CTA perp env must be named binance-cta-perp-<tag>: $ENV_NAME" >&2
  exit 1
}
echo "[PLAN] env=${ENV_NAME} exchange=binance symbol_scope=config/cta_perp.json live_mutation=stop-processes"
echo "[PLAN] signal -> factor publisher -> dashboard -> config server"
if [[ "$EXECUTE" -ne 1 ]]; then
  echo "[DRY-RUN] no process was stopped; pass --execute to proceed"
  exit 0
fi

cd "$BASE_DIR"
bar_interval="$(python3 -c 'import json; print(json.load(open("config/cta_perp.json")).get("bar_interval", "1m"))')"
[[ "$bar_interval" == 1m || "$bar_interval" == 5m || "$bar_interval" == 15m ]] || { echo "[ERROR] invalid bar_interval" >&2; exit 1; }
signal_suffix=""
if [[ "$bar_interval" != 1m ]]; then signal_suffix="_${bar_interval}"; fi
"${BASE_DIR}/scripts/stop_cta_perp${signal_suffix}_signal.sh"
"${BASE_DIR}/scripts/stop_cta_perp_factor_model_${bar_interval}_pub.sh"
"${BASE_DIR}/scripts/stop_cta_perp_dashboard.sh"
"${BASE_DIR}/scripts/stop_cta_perp_config_server.sh"
echo "[INFO] CTA perp stack stopped"
