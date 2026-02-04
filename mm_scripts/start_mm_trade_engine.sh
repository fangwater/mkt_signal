#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

BIN_CANDIDATES=(
  "${BASE_DIR}/trade_engine"
  "${SCRIPT_DIR}/trade_engine"
  "${BASE_DIR}/target/release/trade_engine"
  "${SCRIPT_DIR}/../target/release/trade_engine"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] trade_engine binary not found. Build first with: cargo build --release --bin trade_engine" >&2
  exit 1
fi

usage() {
  cat <<'USAGE'
Usage:
  mm_scripts/start_mm_trade_engine.sh [exchange] [--config <path>]

Notes:
  - If exchange not provided, try to read venue from config (default: manual_mm_signal.yaml).
  - Otherwise infer exchange from dir name.
USAGE
}

CONFIG_PATH="${BASE_DIR}/manual_mm_signal.yaml"
EXCHANGE="${1:-}"
shift || true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="${2:-}"
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

normalize_exchange() {
  local ex="$1"
  ex="${ex,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

read_config_exchange() {
  local cfg="$1"
  local v
  if [[ -f "$cfg" ]]; then
    v=$(awk -F: 'tolower($1) ~ /^venue$/ {gsub(/[[:space:]]/, "", $2); gsub(/"/, "", $2); print $2; exit}' "$cfg")
    v="${v,,}"
    v="${v%%-*}"
    v="${v%%_*}"
    if [[ -n "$v" ]]; then
      echo "$v"
    fi
  fi
}

if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$(read_config_exchange "$CONFIG_PATH")"
fi

if [[ -z "$EXCHANGE" ]]; then
  dir_name="$(basename "${BASE_DIR}")"
  dir_lc="${dir_name,,}"
  case "$dir_lc" in
    binance_mm_*|*binance*) EXCHANGE="binance" ;;
    okex_mm_*|*okex*|*okx*) EXCHANGE="okex" ;;
    gate_mm_*|*gate*) EXCHANGE="gate" ;;
    bybit_mm_*|*bybit*) EXCHANGE="bybit" ;;
    bitget_mm_*|*bitget*) EXCHANGE="bitget" ;;
    *) EXCHANGE="" ;;
  esac
fi

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] missing exchange; provide arg or ensure config has venue" >&2
  exit 1
fi

EXCHANGE="$(normalize_exchange "$EXCHANGE")"

PROC_NAME="${PM2_NAME:-mm_trade_engine_$(echo "${BASE_DIR##*/}" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/_/g')}"
RUST_LOG="${RUST_LOG:-info}"

npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  --exchange "$EXCHANGE"

echo "[INFO] Started ${PROC_NAME} (exchange=${EXCHANGE})"
echo "[INFO] Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
