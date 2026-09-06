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

case "$VENUE" in
  binance-futures|binance-coin-futures) EXCHANGE="binance" ;;
  okex-futures) EXCHANGE="okex" ;;
esac

# Keep this parser aligned with runtime_common::execution_backend::ExecBackend.
parse_exec_backend() {
  local raw="$1"
  raw="$(printf '%s' "$raw" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    ''|native|exchange|direct) printf '%s\n' native ;;
    ltp|rapidx|liquidity|liquiditytech) printf '%s\n' ltp ;;
    *) echo "[ERROR] invalid execution backend: $1 (expected native or rapidx)" >&2; return 1 ;;
  esac
}

exec_backend_for_exchange() {
  local exchange="$1"
  local default_backend="${TRADE_ENGINE_EXEC_BACKEND:-}"
  local mapping="${TRADE_ENGINE_EXEC_BACKEND_MAP:-}"
  local parsed_default wildcard='' specific='' entry key value parsed
  local wildcard_set=0 specific_set=0
  local -a entries=()

  if [[ "$mapping" == *$'\n'* || "$mapping" == *$'\r'* ]]; then
    echo "[ERROR] execution backend map must not contain newlines" >&2
    return 1
  fi
  parsed_default="$(parse_exec_backend "$default_backend")" || return 1
  IFS=',' read -r -a entries <<< "$mapping"
  for entry in "${entries[@]}"; do
    entry="$(printf '%s' "$entry" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
    [[ -z "$entry" ]] && continue
    if [[ "$entry" != *=* ]]; then
      echo "[ERROR] invalid execution backend map entry: $entry (expected exchange=backend)" >&2
      return 1
    fi
    key="$(printf '%s' "${entry%%=*}" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | tr '[:upper:]' '[:lower:]')"
    value="${entry#*=}"
    parsed="$(parse_exec_backend "$value")" || return 1
    case "$key" in
      '*')
        if ((wildcard_set)); then
          echo "[ERROR] duplicate wildcard execution backend mapping" >&2
          return 1
        fi
        wildcard="$parsed"
        wildcard_set=1
        ;;
      binance|okex|bybit|bitget|gate|hyperliquid)
        if [[ "$key" == "$exchange" ]]; then
          if ((specific_set)); then
            echo "[ERROR] duplicate $exchange execution backend mapping" >&2
            return 1
          fi
          specific="$parsed"
          specific_set=1
        fi
        ;;
      *)
        echo "[ERROR] unknown exchange in execution backend map: $key" >&2
        return 1
        ;;
    esac
  done

  if ((specific_set)); then
    printf '%s\n' "$specific"
  elif ((wildcard_set)); then
    printf '%s\n' "$wildcard"
  else
    printf '%s\n' "$parsed_default"
  fi
}

EXEC_BACKEND="$(exec_backend_for_exchange "$EXCHANGE")" || exit 1
if [[ "$EXEC_BACKEND" == "ltp" ]]; then
  if [[ "$VENUE" == "binance-coin-futures" ]]; then
    echo "[ERROR] RapidX Exec supports Binance/OKX linear perpetual venues only" >&2
    exit 1
  fi
  for variable in LTP_API_KEY LTP_API_SECRET LTP_PORTFOLIO_ID; do
    if [[ -z "${!variable:-}" ]]; then
      echo "[ERROR] $variable is required for RapidX Exec" >&2
      exit 1
    fi
  done
  if [[ ! "$LTP_PORTFOLIO_ID" =~ ^[0-9]{1,64}$ ]]; then
    echo "[ERROR] LTP_PORTFOLIO_ID must contain 1..64 ASCII digits" >&2
    exit 1
  fi
fi

BIN_PATH=""
for candidate in "${BASE_DIR}/exec-pre-trade" "${BASE_DIR}/target/release/exec-pre-trade"; do
  if [[ -x "$candidate" ]]; then BIN_PATH="$candidate"; break; fi
done
if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] exec-pre-trade binary not found" >&2
  exit 1
fi

if [[ "$EXEC_BACKEND" == "native" ]]; then
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
fi

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
