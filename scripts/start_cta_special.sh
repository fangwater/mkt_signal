#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_NAME="$(basename "$BASE_DIR")"
EXECUTE=0
if [[ $# -eq 1 && "$1" == "--execute" ]]; then
  EXECUTE=1
elif [[ $# -ne 0 ]]; then
  echo "Usage: scripts/start_cta_special.sh [--execute]" >&2
  exit 1
fi
[[ "$ENV_NAME" =~ ^binance[-_]cta[-_]special[-_][a-z0-9][a-z0-9_-]*$ ]] || {
  echo "[ERROR] CTA special env must be named binance-cta-special-<tag>: $ENV_NAME" >&2
  exit 1
}
[[ -f "${BASE_DIR}/env.sh" ]] || { echo "[ERROR] missing ${BASE_DIR}/env.sh" >&2; exit 1; }
# shellcheck disable=SC1091
source "${BASE_DIR}/env.sh"
[[ "${OPEN_VENUE:-}" == "binance-futures" && "${HEDGE_VENUE:-}" == "binance-futures" ]] || {
  echo "[ERROR] OPEN_VENUE and HEDGE_VENUE must both be binance-futures" >&2
  exit 1
}
EXECUTION_BACKEND_LIB="${BASE_DIR}/scripts/execution_backend_lib.sh"
[[ -f "$EXECUTION_BACKEND_LIB" ]] || { echo "[ERROR] missing $EXECUTION_BACKEND_LIB" >&2; exit 1; }
# shellcheck disable=SC1090
source "$EXECUTION_BACKEND_LIB"
[[ "$(execution_backend_for_exchange binance)" == "ltp" ]] || {
  echo "[ERROR] CTA special requires TRADE_ENGINE_EXEC_BACKEND_MAP with binance=ltp" >&2
  exit 1
}
[[ -n "${LTP_PORTFOLIO_ID:-}" ]] || {
  echo "[ERROR] CTA special requires a non-empty LTP_PORTFOLIO_ID" >&2
  exit 1
}
"${PYTHON_BIN:-python3}" "${BASE_DIR}/scripts/cta_special_config_server.py" \
  --config "${BASE_DIR}/config/cta_special.json" --check >/dev/null

config_enabled="$(python3 -c 'import json,sys; print(str(bool(json.load(open(sys.argv[1]))["enabled"])).lower())' "${BASE_DIR}/config/cta_special.json" 2>/dev/null || echo invalid)"
echo "[PLAN] env=${ENV_NAME} exchange=binance symbol_scope=config/cta_special.json backend=ltp live_mutation=start-processes"
echo "[PLAN] trading_enabled=${config_enabled}"
echo "[PLAN] account_monitor -> dedicated BBO (when configured) -> trade_engine -> persist_manager -> pre_trade -> factor publisher -> config server -> dashboard -> signal"
if [[ "$EXECUTE" -ne 1 ]]; then
  echo "[DRY-RUN] no process was started; pass --execute to proceed"
  exit 0
fi

cd "$BASE_DIR"
"${BASE_DIR}/intra_scripts/start_intra_monitors.sh"
"${BASE_DIR}/scripts/start_cta_special_bbo_pub.sh"
"${BASE_DIR}/intra_scripts/start_intra_trade_engine.sh"
"${BASE_DIR}/intra_scripts/start_intra_persist_manager.sh"
"${BASE_DIR}/intra_scripts/start_intra_pre_trade.sh"
"${BASE_DIR}/scripts/start_cta_special_factor_model_1m_pub.sh"
"${BASE_DIR}/scripts/start_cta_special_config_server.sh"
"${BASE_DIR}/scripts/start_cta_special_dashboard.sh"
"${BASE_DIR}/scripts/start_cta_special_signal.sh"
echo "[INFO] CTA special stack started; trading_enabled=${config_enabled}"
