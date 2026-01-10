#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PM2_NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

usage() {
  cat <<'EOF'
Usage:
  scripts/stop_fr_persist_manager.sh [--exchange <binance|okex|bybit|bitget|gate>]
EOF
}

EXCHANGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

dir_name="$(basename "${BASE_DIR}")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
if [[ -z "$EXCHANGE" ]]; then
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
fi

EXCHANGE="$(echo "$EXCHANGE" | tr 'A-Z' 'a-z')"
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi

PROC_NAME="${PM2_NAME:-persist_manager_${dir_tag}}"

if npx pm2 describe "$PROC_NAME" --namespace "$PM2_NAMESPACE" >/dev/null 2>&1; then
  echo "[INFO] Stopping $PROC_NAME (namespace: $PM2_NAMESPACE)"
  npx pm2 delete "$PROC_NAME" --namespace "$PM2_NAMESPACE"
else
  echo "[WARN] PM2 process not found: $PROC_NAME (namespace: $PM2_NAMESPACE)"
fi
