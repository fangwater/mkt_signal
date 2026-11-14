#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_NAME="$(basename "$BASE_DIR")"
[[ "$ENV_NAME" =~ ^okex-binance-cta-spread-[a-z0-9][a-z0-9_-]*$ ]] || { echo '[ERROR] expected okex-binance-cta-spread-<tag> environment' >&2; exit 1; }
[[ $# == 0 || ( $# == 1 && "$1" == --execute ) ]] || { echo 'Usage: stop_cta_cross_spread.sh [--execute]' >&2; exit 1; }
echo "[PLAN] env=$ENV_NAME exchange=okex+binance symbol_scope=config/cta_cross_spread_symbol_factors.json mutation=stop-processes"
[[ "${1:-}" == --execute ]] || { echo '[DRY-RUN] no process changes'; exit 0; }
cd "$BASE_DIR"
"$SCRIPT_DIR/stop_cta_cross_spread_signal.sh"
"$SCRIPT_DIR/stop_cta_cross_spread_factor_model_1m_pub.sh"
"$SCRIPT_DIR/stop_cta_cross_spread_config_server.sh"
