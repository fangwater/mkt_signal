#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/account_monitor"
  "${BASE_DIR}/scripts/account_monitor"
  "${BASE_DIR}/target/release/account_monitor"
  "${SCRIPT_DIR}/account_monitor"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] account_monitor binary not found. Deploy/build first." >&2
  echo "[ERROR] Expected one of:" >&2
  printf '  - %s\n' "${BIN_CANDIDATES[@]}" >&2
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
case "$dir_name" in
  okex_fr_*|*okex*|*OKEX*) EXCHANGE="okex" ;;
  binance_fr_*|*binance*|*BINANCE*) EXCHANGE="binance" ;;
  gate_fr_*|*gate*|*GATE*) EXCHANGE="gate" ;;
  bitget_fr_*|*bitget*|*BITGET*) EXCHANGE="bitget" ;;
  *)
    echo "[ERROR] 无法从部署目录名推断 exchange: ${dir_name} (期望 okex_fr_* / binance_fr_* / gate_fr_* / bitget_fr_*)" >&2
    exit 1
    ;;
esac

PROC_NAME="${PMDAEMON_NAME:-${PM2_NAME:-account_monitor_${dir_tag}}}"
RUST_LOG="${RUST_LOG:-info}"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "$RUST_LOG")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": [],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
JSON

echo "[INFO] 启动 ${PROC_NAME} (exchange=${EXCHANGE})"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] ${PROC_NAME} 已启动"
echo "[INFO] Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
