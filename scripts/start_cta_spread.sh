#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_NAME="$(basename "$BASE_DIR")"
EXECUTE=0
if [[ $# -eq 1 && "$1" == "--execute" ]]; then
  EXECUTE=1
elif [[ $# -ne 0 ]]; then
  echo "Usage: scripts/start_cta_spread.sh [--execute]" >&2
  exit 1
fi
[[ "$ENV_NAME" =~ ^binance-cta-spread-[a-z0-9][a-z0-9_-]*$ ]] || {
  echo "[ERROR] CTA spread env must be named binance-cta-spread-<tag>: $ENV_NAME" >&2
  exit 1
}
[[ -f "${BASE_DIR}/env.sh" ]] || { echo "[ERROR] missing ${BASE_DIR}/env.sh" >&2; exit 1; }
# shellcheck disable=SC1091
source "${BASE_DIR}/env.sh"
"${PYTHON_BIN:-python3}" "${BASE_DIR}/scripts/cta_spread_config_server.py" \
  --config "${BASE_DIR}/config/cta_spread.json" --check >/dev/null

config_enabled="$(python3 -c 'import json,sys; print(str(bool(json.load(open(sys.argv[1]))["enabled"])).lower())' "${BASE_DIR}/config/cta_spread.json" 2>/dev/null || echo invalid)"
echo "[PLAN] env=${ENV_NAME} exchange=binance symbol_scope=${ENV_NAME}:cta_symbol_factors destination=manager live_mutation=start-processes"
echo "[PLAN] trading_enabled=${config_enabled}"
echo "[PLAN] shared factor publisher (when configured) -> config server -> dashboard -> signal; publishes positions through CTA Manager"
if [[ "$EXECUTE" -ne 1 ]]; then
  echo "[DRY-RUN] no process was started; pass --execute to proceed"
  exit 0
fi

cd "$BASE_DIR"
"${PYTHON_BIN:-python3}" "${BASE_DIR}/scripts/cta_spread_factor_config.py" \
  --env-name "$ENV_NAME" --check >/dev/null
if [[ "$config_enabled" == "true" ]]; then
  [[ -n "${CTA_MANAGER_API_URL:-}" ]] || { echo '[ERROR] CTA_MANAGER_API_URL is required'; exit 1; }
fi
"${BASE_DIR}/scripts/start_cta_spread_factor_model_1m_pub.sh"
"${BASE_DIR}/scripts/start_cta_spread_config_server.sh"
"${BASE_DIR}/scripts/start_cta_spread_dashboard.sh"
"${BASE_DIR}/scripts/start_cta_spread_signal.sh"
echo "[INFO] CTA spread stack started; trading_enabled=${config_enabled}"
