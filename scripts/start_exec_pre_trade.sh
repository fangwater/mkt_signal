#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi
export enable_ipc_fast_poll=0
export ENABLE_IPC_FAST_POLL=0

VENUE="${EXEC_VENUE:-${VENUE:-}}"
CONFIG_RELOAD_MS="${EXEC_CONFIG_RELOAD_MS:-30000}"
CORE="${EXEC_PRE_TRADE_CORE:-${PRE_TRADE_CORE:-}}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --venue) VENUE="${2:-}"; shift 2 ;;
    --config-reload-ms) CONFIG_RELOAD_MS="${2:-}"; shift 2 ;;
    --core) CORE="${2:-}"; shift 2 ;;
    -h|--help)
      echo "Usage: scripts/start_exec_pre_trade.sh --venue <binance-futures|binance-coin-futures|okex-futures>"
      exit 0
      ;;
    *) echo "[ERROR] Unknown arg: $1" >&2; exit 1 ;;
  esac
done
if [[ "$VENUE" != "binance-futures" && "$VENUE" != "binance-coin-futures" && "$VENUE" != "okex-futures" ]]; then
  echo "[ERROR] unsupported venue: $VENUE" >&2
  exit 1
fi

BIN_PATH=""
for candidate in "${BASE_DIR}/exec-pre-trade" "${BASE_DIR}/target/release/exec-pre-trade"; do
  if [[ -x "$candidate" ]]; then BIN_PATH="$candidate"; break; fi
done
if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] exec-pre-trade binary not found" >&2
  exit 1
fi

case "$VENUE" in
  binance-futures)
    for file in binance_cancel_all_std_um_ws_orders.py binance_cancel_all_unified_open_orders.py binance_local_ip.py sell_margin_spot.py; do
      [[ -f "${SCRIPT_DIR}/${file}" ]] || { echo "[ERROR] missing startup cancel dependency: scripts/${file}" >&2; exit 1; }
    done
    ;;
  binance-coin-futures)
    for file in binance_cancel_all_std_cm_orders.py binance_cancel_all_unified_open_orders.py binance_local_ip.py sell_margin_spot.py; do
      [[ -f "${SCRIPT_DIR}/${file}" ]] || { echo "[ERROR] missing startup cancel dependency: scripts/${file}" >&2; exit 1; }
    done
    ;;
  okex-futures)
    [[ -f "${SCRIPT_DIR}/okx_swap_open_orders.py" ]] || { echo "[ERROR] missing startup cancel dependency: scripts/okx_swap_open_orders.py" >&2; exit 1; }
    ;;
esac

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: ${PMDAEMON_BIN}" >&2; exit 1; }
dir_tag="$(basename "$BASE_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]/_/g')"
PROC_NAME="${PMDAEMON_NAME:-exec_pt_${dir_tag}}"
ARGS=(--venue "$VENUE" --config-reload-ms "$CONFIG_RELOAD_MS")
if [[ -n "$CORE" ]]; then ARGS+=(--core "$CORE"); fi

json_args=""
for value in "${ARGS[@]}"; do
  escaped="$(printf '%s' "$value" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  [[ -n "$json_args" ]] && json_args+=","
  json_args+="\"${escaped}\""
done
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
cat >"$cfg_file" <<JSON
{"apps":[{"name":"${PROC_NAME}","script":"${BIN_PATH}","args":[${json_args}],"cwd":"${BASE_DIR}","env":{"RUST_LOG":"${RUST_LOG:-info}","enable_ipc_fast_poll":"0","ENABLE_IPC_FAST_POLL":"0"}}]}
JSON

PMDAEMON_NAME="$PROC_NAME" "${SCRIPT_DIR}/stop_exec_pre_trade.sh"
echo "[INFO] Starting ${PROC_NAME}; startup will cancel every open ${VENUE} order"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$PROC_NAME"
echo "[INFO] Logs: ${PMDAEMON_BIN} logs ${PROC_NAME} --follow"
