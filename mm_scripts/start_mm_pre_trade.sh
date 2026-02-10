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

usage() {
  cat <<'USAGE'
Usage:
  mm_scripts/start_mm_pre_trade.sh [--venue <binance-futures>] [--config <path>]

Notes:
  - If --venue not provided, try to read venue from config (default: config/manual_mm_signal.yaml).
  - If still missing, infer exchange from dir name and default to <exchange>-futures.
USAGE
}

VENUE=""
CONFIG_PATH="${BASE_DIR}/config/manual_mm_signal.yaml"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --venue)
      VENUE="${2:-}"
      shift 2
      ;;
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

normalize_venue() {
  echo "$1" | tr 'A-Z' 'a-z' | sed 's/_/-/g'
}

read_config_venue() {
  local cfg="$1"
  if [[ -f "$cfg" ]]; then
    awk -F: 'tolower($1) ~ /^venue$/ {gsub(/[[:space:]]/, "", $2); gsub(/"/, "", $2); print $2; exit}' "$cfg"
  fi
}

if [[ -z "$VENUE" ]]; then
  VENUE="$(read_config_venue "$CONFIG_PATH")"
fi

if [[ -z "$VENUE" ]]; then
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
  if [[ -n "$EXCHANGE" ]]; then
    VENUE="${EXCHANGE}-futures"
  fi
fi

if [[ -z "$VENUE" ]]; then
  echo "[ERROR] missing venue; use --venue or provide venue in config" >&2
  exit 1
fi

VENUE="$(normalize_venue "$VENUE")"

PROC_NAME="${PM2_NAME:-mm_pre_trade_$(echo "${BASE_DIR##*/}" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/_/g')}"
RUST_LOG="${RUST_LOG:-info}"
IPC_NS="${IPC_NAMESPACE:-}"
if [[ -z "$IPC_NS" ]]; then
  IPC_NS="$(basename "${BASE_DIR}")"
  echo "[WARN] IPC_NAMESPACE not set; use default: ${IPC_NS}"
fi

npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

IPC_NAMESPACE="$IPC_NS" RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  --open-venue "$VENUE" --hedge-venue "$VENUE"

echo "[INFO] Started ${PROC_NAME} (venue=${VENUE}, ipc_namespace=${IPC_NS})"
echo "[INFO] Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
