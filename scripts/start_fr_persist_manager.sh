#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

ensure_pmdaemon() {
  if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
    echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
    echo "[HINT] install with: cargo install pmdaemon" >&2
    exit 1
  fi
}

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
  cat <<'USAGE'
Usage:
  scripts/start_fr_persist_manager.sh [--exchange <binance|okex|bybit|bitget|gate>]
USAGE
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

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE is not set; run: source ${ENV_FILE}" >&2
  exit 1
fi

ensure_pmdaemon

PROC_NAME="${PMDAEMON_NAME:-${PM2_NAME:-persist_manager_${dir_tag}}}"
RUST_LOG="${RUST_LOG:-info}"

mkdir -p "${BASE_DIR}/data/persist_manager" >/dev/null 2>&1 || true

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "$RUST_LOG")"
json_ipc_ns="$(json_escape "$IPC_NAMESPACE")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": [],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}",
        "IPC_NAMESPACE": "${json_ipc_ns}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting ${PROC_NAME}"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo ""
echo "[INFO] Started persist_manager"
echo "Process: ${PROC_NAME}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
