#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
[[ -f "${BASE_DIR}/env.sh" ]] && source "${BASE_DIR}/env.sh"

VENUE="${EXEC_VENUE:-${VENUE:-}}"
case "$VENUE" in
  binance-futures) EXCHANGE="binance" ;;
  okex-futures) EXCHANGE="okex" ;;
  *) echo "[ERROR] EXEC_VENUE must be binance-futures or okex-futures" >&2; exit 1 ;;
esac

dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
export PMDAEMON_NAME="${PMDAEMON_NAME:-exec_te_${dir_tag}}"
exec "${SCRIPT_DIR}/start_trade_engine.sh" "$EXCHANGE"
