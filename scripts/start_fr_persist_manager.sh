#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PM2_NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

BIN_CANDIDATES=(
  "${BASE_DIR}/persist_manager"
  "${SCRIPT_DIR}/persist_manager"
  "${BASE_DIR}/target/release/persist_manager"
  "${SCRIPT_DIR}/../target/release/persist_manager"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] persist_manager binary not found. Build first with: cargo build --release --bin persist_manager" >&2
  exit 1
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[ERROR] env.sh not found: ${ENV_FILE}" >&2
  echo "[ERROR] Run: scripts/deploy_setup_env.sh [trade|test] --exchange <...> (and source env.sh)" >&2
  exit 1
fi

usage() {
  cat <<'EOF'
Usage:
  scripts/start_fr_persist_manager.sh --port <PORT> [--exchange <binance|okex|bybit|bitget|gate>]
  scripts/start_fr_persist_manager.sh [--exchange <binance|okex|bybit|bitget|gate>]

Notes:
  - If --port is omitted, a default port is chosen by exchange:
      okex=9101, gate=9102, binance=9103, bybit=9104, bitget=9105
EOF
}

EXCHANGE=""
PORT="${PORT:-}"
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

default_port_for_exchange() {
  case "$1" in
    okex) echo "9101" ;;
    gate) echo "9102" ;;
    binance) echo "9103" ;;
    bybit) echo "9104" ;;
    bitget) echo "9105" ;;
    *) echo "9101" ;;
  esac
}

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

if [[ -z "$PORT" ]]; then
  PORT="$(default_port_for_exchange "$EXCHANGE")"
fi
if [[ ! "$PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --port must be numeric: $PORT" >&2
  exit 1
fi

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE is not set; run: source ${ENV_FILE}" >&2
  exit 1
fi

PROC_NAME="${PM2_NAME:-persist_manager_${dir_tag}}"
RUST_LOG="${RUST_LOG:-info}"

mkdir -p "${BASE_DIR}/data/persist_manager" >/dev/null 2>&1 || true

echo "[INFO] Restarting ${PROC_NAME} (namespace=${PM2_NAMESPACE} port=${PORT})"
npx pm2 delete "$PROC_NAME" --namespace "$PM2_NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
    --name "$PROC_NAME" \
    --namespace "$PM2_NAMESPACE" \
    -- \
    --port "$PORT"
)

echo ""
echo "[INFO] Started persist_manager"
echo "Namespace: ${PM2_NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${PM2_NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${PM2_NAMESPACE}"
