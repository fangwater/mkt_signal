#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

if [[ $# -lt 2 ]]; then
  echo "用法: $0 <open-venue> <hedge-venue> [exchange]"
  echo "示例: $0 binance-margin okex-futures"
  exit 1
fi

OPEN_VENUE="$1"
HEDGE_VENUE="$2"
EXCHANGE="${3:-}"

if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="${HEDGE_VENUE%%-*}"
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/fr_signal"
  "${BASE_DIR}/target/release/fr_signal"
  "${BASE_DIR}/scripts/fr_signal"
  "${BASE_DIR}/scripts/target/release/fr_signal"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] fr_signal binary not found. Build first with: cargo build --release --bin fr_signal"
  exit 1
fi

proc_suffix_open="${OPEN_VENUE//-/_}"
proc_suffix_hedge="${HEDGE_VENUE//-/_}"
PROC_NAME="fr_signal_${proc_suffix_open}__${proc_suffix_hedge}"
RUST_LOG="${RUST_LOG:-info}"

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE}) open=${OPEN_VENUE} hedge=${HEDGE_VENUE} exchange=${EXCHANGE}"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  --exchange "$EXCHANGE" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE"

echo ""
echo "[INFO] Started fr_signal"
echo "  exchange=${EXCHANGE}"
echo "  open=${OPEN_VENUE}"
echo "  hedge=${HEDGE_VENUE}"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"

