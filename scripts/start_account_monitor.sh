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
dir_lc="${dir_name,,}"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
MODE=""
EXCHANGE=""
ENV_TAG=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]fr([_-](.+))?$ ]]; then
  MODE="fr"
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="$(echo "${BASH_REMATCH[3]:-fr}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
elif type mm_parse_deploy_dir >/dev/null 2>&1 && read -r EXCHANGE ENV_TAG < <(mm_parse_deploy_dir "$dir_lc"); then
  MODE="mm"
else
  echo "[ERROR] 无法从部署目录名推断 account_monitor 环境: ${dir_name}" >&2
  echo "[ERROR] 期望如 okex_fr_trade / binance_mm_alpha" >&2
  exit 1
fi

if type mm_normalize_exchange >/dev/null 2>&1; then
  EXCHANGE="$(mm_normalize_exchange "$EXCHANGE")"
fi
if [[ -z "$ENV_TAG" ]]; then
  ENV_TAG="$MODE"
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
      *)
        echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2
        ;;
    esac
  }
  DEFAULT_PROC_NAME="fr_am_$(short_exchange "$EXCHANGE")_${ENV_TAG}"
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

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_name="$(json_escape "$PROC_NAME")"
json_shell="$(json_escape "/bin/bash")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "$RUST_LOG")"
cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN_PATH")"
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
"${PMDAEMON[@]}" delete "$LEGACY_PROC_NAME" >/dev/null 2>&1 || true
if [[ -n "$LEGACY_FR_PROC_NAME" && "$LEGACY_FR_PROC_NAME" != "$PROC_NAME" ]]; then
  "${PMDAEMON[@]}" delete "$LEGACY_FR_PROC_NAME" >/dev/null 2>&1 || true
fi
if [[ -n "$BUGGY_MM_PROC_NAME" ]]; then
  "${PMDAEMON[@]}" delete "$BUGGY_MM_PROC_NAME" >/dev/null 2>&1 || true
fi
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] ${PROC_NAME} 已启动"
echo "[INFO] Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
