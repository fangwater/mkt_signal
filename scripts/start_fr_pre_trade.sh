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

ENV_FILE="$BASE_DIR/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

usage() {
  cat <<'USAGE'
Usage:
  scripts/start_fr_pre_trade.sh [--open-venue <venue>] [--hedge-venue <venue>]

Notes:
  - Default: no args, venues inferred from current directory.
USAGE
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
dir_lc="${dir_name,,}"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
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

short_exchange() {
  case "${1,,}" in
    binance) echo "bn" ;;
    okex) echo "ok" ;;
    bybit) echo "bb" ;;
    bitget) echo "bg" ;;
    gate) echo "gt" ;;
    *)
      echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2
      ;;
  esac
}

env_tag="fr"
if [[ "$dir_lc" =~ ^[a-z0-9]+[-_]fr[-_](.+)$ ]]; then
  env_tag="$(echo "${BASH_REMATCH[1]}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
fi
if [[ -z "$env_tag" ]]; then
  env_tag="fr"
fi

PROC_NAME="${PMDAEMON_NAME:-fr_pt_$(short_exchange "$EXCHANGE")_${env_tag}}"
LEGACY_PROC_NAME="pre_trade_${dir_tag}"
RUST_LOG="${RUST_LOG:-info}"
ARGS=()
if [[ -n "$OPEN_VENUE" ]]; then
  ARGS+=(--open-venue "$OPEN_VENUE" --hedge-venue "$HEDGE_VENUE")
fi

ensure_pmdaemon

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

build_json_array() {
  local out="["
  local first=1
  local v=""
  for v in "$@"; do
    local esc
    esc="$(json_escape "$v")"
    if [[ $first -eq 0 ]]; then
      out+=", "
    fi
    out+="\"${esc}\""
    first=0
  done
  out+="]"
  printf '%s' "$out"
}

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "$RUST_LOG")"
args_json="$(build_json_array "${ARGS[@]}")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ${args_json},
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting ${PROC_NAME}"
if [[ "$LEGACY_PROC_NAME" != "$PROC_NAME" ]]; then
  "${PMDAEMON[@]}" delete "$LEGACY_PROC_NAME" >/dev/null 2>&1 || true
fi
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started ${PROC_NAME}"
echo "[INFO] Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
