#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_NAME="$(basename "$BASE_DIR")"
EXECUTE=0
if [[ $# -eq 1 && "$1" == "--execute" ]]; then
  EXECUTE=1
elif [[ $# -ne 0 ]]; then
  echo "Usage: scripts/start_cta_perp.sh [--execute]" >&2
  exit 1
fi
[[ "$ENV_NAME" =~ ^binance-cta-perp-[a-z0-9][a-z0-9_-]*$ ]] || {
  echo "[ERROR] CTA perp env must be named binance-cta-perp-<tag>: $ENV_NAME" >&2
  exit 1
}
[[ -f "${BASE_DIR}/env.sh" ]] || { echo "[ERROR] missing ${BASE_DIR}/env.sh" >&2; exit 1; }
# shellcheck disable=SC1091
source "${BASE_DIR}/env.sh"
"${PYTHON_BIN:-python3}" "${BASE_DIR}/scripts/cta_perp_config_server.py" \
  --config "${BASE_DIR}/config/cta_perp.json" --check >/dev/null

config_enabled="$(python3 -c 'import json,sys; print(str(bool(json.load(open(sys.argv[1]))["enabled"])).lower())' "${BASE_DIR}/config/cta_perp.json" 2>/dev/null || echo invalid)"
echo "[PLAN] env=${ENV_NAME} exchange=binance symbol_scope=${ENV_NAME}:cta_symbol_factors destination=manager live_mutation=start-processes"
echo "[PLAN] trading_enabled=${config_enabled}"
echo "[PLAN] shared factor publisher (when configured) -> config server -> dashboard -> signal; publishes positions through CTA Manager"
if [[ "$EXECUTE" -ne 1 ]]; then
  echo "[DRY-RUN] no process was started; pass --execute to proceed"
  exit 0
fi

cd "$BASE_DIR"
bar_interval="$(python3 -c 'import json; print(json.load(open("config/cta_perp.json")).get("bar_interval", "1m"))')"
[[ "$bar_interval" == 1m || "$bar_interval" == 5m ]] || { echo "[ERROR] invalid bar_interval" >&2; exit 1; }
signal_suffix=""
[[ "$bar_interval" == 5m ]] && signal_suffix="_5m"
"${PYTHON_BIN:-python3}" "${BASE_DIR}/scripts/cta_perp_factor_config.py" \
  --env-name "$ENV_NAME" --check >/dev/null
if [[ "$config_enabled" == "true" ]]; then
  [[ -n "${CTA_MANAGER_API_URLS:-${CTA_MANAGER_API_URL:-}}" ]] || { echo '[ERROR] CTA_MANAGER_API_URLS (or CTA_MANAGER_API_URL) is required'; exit 1; }
fi
"${BASE_DIR}/scripts/start_cta_perp_factor_model_${bar_interval}_pub.sh"
"${BASE_DIR}/scripts/start_cta_perp_config_server.sh"
"${BASE_DIR}/scripts/start_cta_perp_dashboard.sh"
"${BASE_DIR}/scripts/start_cta_perp${signal_suffix}_signal.sh"
echo "[INFO] CTA perp stack started; trading_enabled=${config_enabled}"
