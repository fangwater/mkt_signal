#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MM_NAME_LIB="${SCRIPT_DIR}/mm_process_name.sh"

if [[ -f "$MM_NAME_LIB" ]]; then
  # shellcheck disable=SC1090
  source "$MM_NAME_LIB"
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
MODE=""
EXCHANGE=""
ENV_TAG=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]fr([_-](.+))?$ ]]; then
  MODE="fr"
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="$(echo "${BASH_REMATCH[3]:-fr}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
elif [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]exec([_-](.+))?$ ]]; then
  MODE="exec"
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="$(echo "${BASH_REMATCH[3]:-exec}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
elif type mm_parse_deploy_dir >/dev/null 2>&1 && read -r EXCHANGE ENV_TAG < <(mm_parse_deploy_dir "$dir_lc"); then
  MODE="mm"
else
  echo "[ERROR] 无法从部署目录名推断 account_monitor 环境: ${dir_name}" >&2
  echo "[ERROR] 期望如 okex_fr_trade / binance_exec_trade / binance_mm_alpha" >&2
  exit 1
fi

if type mm_normalize_exchange >/dev/null 2>&1; then
  EXCHANGE="$(mm_normalize_exchange "$EXCHANGE")"
fi
if [[ -z "$ENV_TAG" ]]; then
  ENV_TAG="$MODE"
fi

EXEC_BACKEND="native"
ACCOUNT_MONITOR_ARGS=()
if [[ "$MODE" == "exec" ]]; then
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
    case "$EXCHANGE" in
      binance|okex) ;;
      *) echo "[ERROR] RapidX account monitor supports Binance/OKX Exec only" >&2; exit 1 ;;
    esac
    ACCOUNT_MONITOR_ARGS=(--exchange "$EXCHANGE")
    BIN_CANDIDATES=(
      "${BASE_DIR}/rapidx_account_monitor"
      "${BASE_DIR}/target/release/rapidx_account_monitor"
    )
  fi
fi

if [[ "$EXEC_BACKEND" == "native" ]]; then
  BIN_CANDIDATES=(
    "${BASE_DIR}/account_monitor"
    "${BASE_DIR}/scripts/account_monitor"
    "${BASE_DIR}/target/release/account_monitor"
    "${SCRIPT_DIR}/account_monitor"
  )
fi

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] account monitor binary not found for backend=${EXEC_BACKEND}. Deploy/build first." >&2
  echo "[ERROR] Expected one of:" >&2
  printf '  - %s\n' "${BIN_CANDIDATES[@]}" >&2
  exit 1
fi

if [[ "$MODE" == "mm" ]]; then
  DEFAULT_PROC_NAME="mm_am_${EXCHANGE}_${ENV_TAG}"
else
  short_exchange() {
    if type mm_short_exchange >/dev/null 2>&1; then
      mm_short_exchange "$1"
      return
    fi
    case "${1,,}" in
      binance) echo "bn" ;;
      okex) echo "ok" ;;
      bybit) echo "bb" ;;
      bitget) echo "bg" ;;
      gate) echo "gt" ;;
      hyperliquid) echo "hl" ;;
      *)
        echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2
        ;;
    esac
  }
  if [[ "$MODE" == "exec" ]]; then
    DEFAULT_PROC_NAME="exec_am_$(short_exchange "$EXCHANGE")_${ENV_TAG}"
  else
    DEFAULT_PROC_NAME="fr_am_$(short_exchange "$EXCHANGE")_${ENV_TAG}"
  fi
fi

PROC_NAME="${PMDAEMON_NAME:-$DEFAULT_PROC_NAME}"
LEGACY_PROC_NAME="account_monitor_${dir_tag}"
LEGACY_FR_PROC_NAME=""
if [[ "$MODE" == "fr" ]]; then
  LEGACY_FR_PROC_NAME="am_$(short_exchange "$EXCHANGE")_${ENV_TAG}"
fi
BUGGY_MM_PROC_NAME=""
if [[ "$MODE" == "mm" ]]; then
  if type mm_short_exchange >/dev/null 2>&1; then
    BUGGY_MM_PROC_NAME="am_$(mm_short_exchange "$EXCHANGE")_fr"
  else
    BUGGY_MM_PROC_NAME="am_$(printf '%s' "${EXCHANGE,,}" | cut -c1-2)_fr"
  fi
fi
RUST_LOG="${RUST_LOG:-info}"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

shell_quote() {
  printf '%q' "$1"
}

core_args=()
if [[ -n "${ACCOUNT_MONITOR_CORE:-}" ]]; then
  if [[ ! "$ACCOUNT_MONITOR_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] ACCOUNT_MONITOR_CORE 必须为单个整数 (got: $ACCOUNT_MONITOR_CORE)" >&2
    exit 1
  fi
  core_args=(--core "$ACCOUNT_MONITOR_CORE")
  echo "[INFO] core bind ${ACCOUNT_MONITOR_CORE} (from $ENV_FILE:ACCOUNT_MONITOR_CORE)"
fi

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_name="$(json_escape "$PROC_NAME")"
json_shell="$(json_escape "/bin/bash")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "$RUST_LOG")"
cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN_PATH")"
for arg in "${ACCOUNT_MONITOR_ARGS[@]}" "${core_args[@]}"; do
  cmd+=" $(shell_quote "$arg")"
done
json_cmd="$(json_escape "$cmd")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_shell}",
      "args": ["-lc", "${json_cmd}"],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
JSON

echo "[INFO] 启动 ${PROC_NAME} (exchange=${EXCHANGE})"
STOP_SCRIPT="${SCRIPT_DIR}/stop_account_monitor.sh"
if [[ ! -x "$STOP_SCRIPT" ]]; then
  echo "[ERROR] stop script not found or not executable: $STOP_SCRIPT" >&2
  exit 1
fi
"$STOP_SCRIPT"
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] ${PROC_NAME} 已启动"
echo "[INFO] Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
