#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

usage() {
  cat <<'EOF'
Usage:
  scripts/stop_fr_pre_trade.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

dir_name="$(basename "${BASE_DIR}")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
case "$dir_name" in
  okex_fr_*|*okex*|*OKEX*) EXCHANGE="okex" ;;
  binance_fr_*|*binance*|*BINANCE*) EXCHANGE="binance" ;;
  bybit_fr_*|*bybit*|*BYBIT*) EXCHANGE="bybit" ;;
  bitget_fr_*|*bitget*|*BITGET*) EXCHANGE="bitget" ;;
  gate_fr_*|*gate*|*GATE*) EXCHANGE="gate" ;;
  *)
    echo "[ERROR] Failed to infer exchange from dir: ${dir_name}" >&2
    exit 1
    ;;
esac

PROC_NAME="${PM2_NAME:-pre_trade_${dir_tag}}"

if npx pm2 describe "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Stopping $PROC_NAME (namespace: $NAMESPACE)"
  npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE"
else
  echo "[WARN] PM2 process not found: $PROC_NAME (namespace: $NAMESPACE)"
fi
