#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_NAME="$(basename "$BASE_DIR")"
EXECUTE=0
if [[ $# -eq 1 && "$1" == "--execute" ]]; then
  EXECUTE=1
elif [[ $# -ne 0 ]]; then
  echo "Usage: scripts/stop_cta_special.sh [--execute]" >&2
  exit 1
fi
[[ "$ENV_NAME" =~ ^binance[-_]cta[-_]special[-_][a-z0-9][a-z0-9_-]*$ ]] || {
  echo "[ERROR] CTA special env must be named binance-cta-special-<tag>: $ENV_NAME" >&2
  exit 1
}
echo "[PLAN] env=${ENV_NAME} exchange=binance symbol_scope=config/cta_special.json live_mutation=stop-processes"
echo "[PLAN] signal -> factor publisher -> pre_trade -> persist_manager -> trade_engine -> account_monitor -> dashboard -> config server"
if [[ "$EXECUTE" -ne 1 ]]; then
  echo "[DRY-RUN] no process was stopped; pass --execute to proceed"
  exit 0
fi

cd "$BASE_DIR"
"${BASE_DIR}/scripts/stop_cta_special_signal.sh"
"${BASE_DIR}/scripts/stop_cta_special_factor_model_1m_pub.sh"
"${BASE_DIR}/intra_scripts/stop_intra_pre_trade.sh"
"${BASE_DIR}/intra_scripts/stop_intra_persist_manager.sh"
"${BASE_DIR}/intra_scripts/stop_intra_trade_engine.sh"
"${BASE_DIR}/intra_scripts/stop_intra_monitors.sh"
"${BASE_DIR}/scripts/stop_cta_special_dashboard.sh"
"${BASE_DIR}/scripts/stop_cta_special_config_server.sh"
echo "[INFO] CTA special stack stopped"
