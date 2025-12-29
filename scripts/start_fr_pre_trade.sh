#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

BIN_CANDIDATES=(
  "${BASE_DIR}/pre_trade"
  "${SCRIPT_DIR}/pre_trade"
  "${BASE_DIR}/target/release/pre_trade"
  "${SCRIPT_DIR}/../target/release/pre_trade"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] pre_trade binary not found. Build first with: cargo build --release --bin pre_trade" >&2
  exit 1
fi

ENV_FILE="$BASE_DIR/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

usage() {
  cat <<'EOF'
Usage:
  scripts/start_fr_pre_trade.sh [--open-venue <venue>] [--hedge-venue <venue>]

Notes:
  - Default: no args, venues inferred from current directory.
EOF
}

OPEN_VENUE=""
HEDGE_VENUE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --open-venue)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      HEDGE_VENUE="${2:-}"
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

if [[ -z "$OPEN_VENUE" && -n "$HEDGE_VENUE" ]] || [[ -n "$OPEN_VENUE" && -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] --open-venue and --hedge-venue must be provided together" >&2
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
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

PROC_NAME="pre_trade_${EXCHANGE}"
RUST_LOG="${RUST_LOG:-info}"
ARGS=()
if [[ -n "$OPEN_VENUE" ]]; then
  ARGS+=(--open-venue "$OPEN_VENUE" --hedge-venue "$HEDGE_VENUE")
fi

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

cd "$BASE_DIR"
if [[ ${#ARGS[@]} -eq 0 ]]; then
  RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
    --name "$PROC_NAME" \
    --namespace "$NAMESPACE"
else
  RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
    --name "$PROC_NAME" \
    --namespace "$NAMESPACE" \
    -- \
    "${ARGS[@]}"
fi

echo "[INFO] Started ${PROC_NAME}"
echo "[INFO] Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
