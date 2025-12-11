#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# 可选：设置 PM2 namespace（默认使用部署目录名，可用环境变量覆盖）
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"
# Candidate locations: deployed dir first, then repo targets
BIN_CANDIDATES=(
  "${SCRIPT_DIR}/mkt_pub"
  "${SCRIPT_DIR}/target/release/mkt_pub"
  "${SCRIPT_DIR}/../mkt_pub"
  "${SCRIPT_DIR}/../target/release/mkt_pub"
  "${BASE_DIR}/mkt_pub"
  "${BASE_DIR}/target/release/mkt_pub"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] mkt_pub binary not found. Build first with: cargo build --release"
  exit 1
fi

# Determine default venues based on deploy directory name (e.g., okex_fr_trade -> okex-futures,okex-margin)
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

start_one() {
  local venue="$1"
  local name="mkt_pub_${venue}"
  local rust_log="${RUST_LOG:-info}"

  echo "[INFO] Restarting ${name}"
  npx pm2 delete "$name" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

  RUST_LOG="${rust_log}" npx pm2 start "$BIN_PATH" \
    --name "$name" \
    --namespace "$NAMESPACE" \
    -- \
    --venue "$venue"
}

for venue in "${VENUES[@]}"; do
  start_one "$venue"
  sleep 1
done

echo ""
echo "[INFO] Started venues: ${VENUES[*]}"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} mkt_pub_<venue>"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
