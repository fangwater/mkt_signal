#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

BIN_CANDIDATES=(
  "${BASE_DIR}/manual_signal"
  "${SCRIPT_DIR}/manual_signal"
  "${BASE_DIR}/target/release/manual_signal"
  "${SCRIPT_DIR}/../target/release/manual_signal"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] manual_signal binary not found. Build first with: cargo build --release --bin manual_signal" >&2
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
  scripts/start_fr_manual_signal.sh [--exchange <binance|okex|bybit|bitget|gate>] [--port <default>] [--bind 0.0.0.0]

Notes:
  - Run under <exchange>_fr_<trade|test> to infer exchange.
  - Default ports: okex=8911, gate=8921, binance=8931, bybit=8941, bitget=8951
EOF
}

default_port_for_exchange() {
  case "$1" in
    okex) echo "8911" ;;
    gate) echo "8921" ;;
    binance) echo "8931" ;;
    bybit) echo "8941" ;;
    bitget) echo "8951" ;;
    *) echo "8911" ;;
  esac
}

EXCHANGE=""
PORT=""
BIND="${BIND:-0.0.0.0}"
OPEN_VENUE=""
HEDGE_VENUE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --port)
      PORT="${2:-}"
      shift 2
      ;;
    --bind)
      BIND="${2:-0.0.0.0}"
      shift 2
      ;;
    --open)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge)
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
case "$EXCHANGE" in
  binance|okex|bybit|bitget|gate)
    ;;
  *)
    echo "[ERROR] Unsupported exchange: $EXCHANGE (allowed: binance/okex/bybit/bitget/gate)" >&2
    exit 1
    ;;
esac

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE is not set; run: source ${ENV_FILE}" >&2
  exit 1
fi

if [[ -z "$PORT" ]]; then
  PORT="$(default_port_for_exchange "$EXCHANGE")"
fi
if [[ ! "$PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --port must be numeric: $PORT" >&2
  exit 1
fi

if [[ -z "$OPEN_VENUE" && -z "$HEDGE_VENUE" ]]; then
  case "$EXCHANGE" in
    binance)
      OPEN_VENUE="binance-margin"
      HEDGE_VENUE="binance-futures"
      ;;
    okex)
      OPEN_VENUE="okex-margin"
      HEDGE_VENUE="okex-futures"
      ;;
    bybit)
      OPEN_VENUE="bybit-margin"
      HEDGE_VENUE="bybit-futures"
      ;;
    bitget)
      OPEN_VENUE="bitget-margin"
      HEDGE_VENUE="bitget-futures"
      ;;
    gate)
      OPEN_VENUE="gate-margin"
      HEDGE_VENUE="gate-futures"
      ;;
  esac
elif [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] --open and --hedge must be provided together" >&2
  exit 1
fi

PROC_NAME="${PM2_NAME:-manual_signal_${dir_tag}}"
RUST_LOG="${RUST_LOG:-info}"

echo "[INFO] Restarting ${PROC_NAME} (exchange=${EXCHANGE}, port=${PORT}, namespace=${NAMESPACE})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

cd "$BASE_DIR"
RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  --open "$OPEN_VENUE" \
  --hedge "$HEDGE_VENUE" \
  --bind "$BIND" \
  --port "$PORT"

echo "[INFO] Started ${PROC_NAME}"
echo "[INFO] Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
