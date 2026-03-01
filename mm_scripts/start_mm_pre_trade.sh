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
  mm_scripts/start_mm_pre_trade.sh [--config <path>]

Notes:
  - venue is always inferred (forced), cannot be overridden by CLI args.
  - First try venue from config (default: config/manual_mm_signal.yaml).
  - If missing, infer exchange from dir name and default to <exchange>-futures.
USAGE
}

CONFIG_PATH="${BASE_DIR}/config/manual_mm_signal.yaml"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --venue)
      echo "[ERROR] --venue is not allowed; venue must be inferred from config/cwd" >&2
      usage >&2
      exit 1
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

VENUE="$(read_config_venue "$CONFIG_PATH")"

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
  echo "[ERROR] missing inferred venue; provide venue in config or use a dir name like <exchange>_mm_<suffix>" >&2
  exit 1
fi

VENUE="$(normalize_venue "$VENUE")"

PROC_NAME="${PMDAEMON_NAME:-${PM2_NAME:-mm_pre_trade_$(echo "${BASE_DIR##*/}" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_-]/_/g')}}"
RUST_LOG="${RUST_LOG:-info}"
IPC_NS="${IPC_NAMESPACE:-}"
if [[ -z "$IPC_NS" ]]; then
  IPC_NS="$(basename "${BASE_DIR}")"
  echo "[WARN] IPC_NAMESPACE not set; use default: ${IPC_NS}"
fi

ensure_pmdaemon

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_venue="$(json_escape "$VENUE")"
json_rust_log="$(json_escape "$RUST_LOG")"
json_ipc_ns="$(json_escape "$IPC_NS")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ["--open-venue", "${json_venue}", "--hedge-venue", "${json_venue}"],
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

echo "[INFO] Started ${PROC_NAME} (venue=${VENUE}, ipc_namespace=${IPC_NS})"
echo "[INFO] Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
