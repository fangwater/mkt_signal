#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# Optional PM2 namespace; defaults to deploy dir name unless PM2_NAMESPACE is set
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

# Determine default venues based on deploy directory name
dir_name="$(basename "${BASE_DIR}")"
case "$dir_name" in
  *okex*|*OKEX*)
    DEFAULT_VENUES=("okex-futures" "okex-margin")
    ;;
  *binance*|*BINANCE*)
    DEFAULT_VENUES=("binance-futures" "binance-margin")
    ;;
  *bybit*|*BYBIT*)
    DEFAULT_VENUES=("bybit-futures" "bybit-margin")
    ;;
  *bitget*|*BITGET*)
    DEFAULT_VENUES=("bitget-futures" "bitget-margin")
    ;;
  *gate*|*GATE*)
    DEFAULT_VENUES=("gate-futures" "gate-margin")
    ;;
  *)
    DEFAULT_VENUES=()
    ;;
esac

# Provided venues override defaults
if [[ $# -gt 0 ]]; then
  VENUES=("$@")
else
  VENUES=("${DEFAULT_VENUES[@]}")
fi

if [[ ${#VENUES[@]} -eq 0 ]]; then
  echo "[ERROR] No venues provided or inferred from directory name ($dir_name)"
  exit 1
fi

stop_one() {
  local venue="$1"
  local name="mkt_pub_${venue}"

  echo "[INFO] Deleting ${name} (namespace: ${NAMESPACE})"
  if npx pm2 delete "$name" --namespace "$NAMESPACE"; then
    echo "[INFO] Deleted ${name}"
  else
    echo "[WARN] ${name} not found in namespace ${NAMESPACE}"
  fi
}

for venue in "${VENUES[@]}"; do
  stop_one "$venue"
  sleep 1
done

echo ""
echo "[INFO] Stopped venues: ${VENUES[*]}"
echo "Namespace: ${NAMESPACE}"
echo "To view remaining processes: npx pm2 status --namespace ${NAMESPACE}"
