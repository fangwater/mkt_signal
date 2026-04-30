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

usage() {
  cat <<'USAGE'
用法: intra_scripts/start_intra_monitors.sh

说明:
  - 同所期现：启动单个 account_monitor 对应当前 exchange
  - 进程名: intra_am_<exchange>_<env>
  - 需要 env.sh 提供 IPC_NAMESPACE
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

ensure_pmdaemon

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[WARN] 未找到 env.sh：${ENV_FILE}"
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

EXCHANGE=""
ENV_TAG="intra"
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="${BASH_REMATCH[2]//-/_}"
elif [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
fi
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi

# env.sh 中的 OPEN_VENUE 推断更可靠
if [[ -z "$EXCHANGE" && -n "${OPEN_VENUE:-}" ]]; then
  EXCHANGE="${OPEN_VENUE%%-*}"
fi

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 无法确定 exchange (dir=$dir_name)，期望 <exchange>-intra-<tag>"
  exit 1
fi

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE 未设置（请 source env.sh）"
  exit 1
fi

bin_for_exchange() {
  local ex="$1"
  case "$ex" in
    okex|binance|gate|bybit|bitget) ;;
    *) return 1 ;;
  esac
  local candidates=(
    "${BASE_DIR}/account_monitor_${ex}"
    "${SCRIPT_DIR}/account_monitor_${ex}"
    "${BASE_DIR}/${ex}_account_monitor"
    "${SCRIPT_DIR}/${ex}_account_monitor"
  )
  for cand in "${candidates[@]}"; do
    if [[ -x "$cand" ]]; then echo "$cand"; return 0; fi
  done
  return 1
}

if ! BIN="$(bin_for_exchange "$EXCHANGE")"; then
  echo "[ERROR] 未找到 account monitor 二进制 for exchange=${EXCHANGE}"
  echo "[ERROR] 请先部署: scripts/deploy_intra_monitors.sh --env-name $(basename "$BASE_DIR") --exchange ${EXCHANGE}"
  exit 1
fi

PROC_NAME="intra_am_${EXCHANGE}_${ENV_TAG}"

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }
shell_quote() { printf '%q' "$1"; }

json_name="$(json_escape "$PROC_NAME")"
json_shell="$(json_escape "/bin/bash")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "${RUST_LOG:-info}")"
json_ipc_ns="$(json_escape "$IPC_NAMESPACE")"
cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN")"
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
        "RUST_LOG": "${json_rust_log}",
        "IPC_NAMESPACE": "${json_ipc_ns}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting $PROC_NAME (exchange=$EXCHANGE namespace=$IPC_NAMESPACE)"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started: $PROC_NAME"
echo "[INFO] Logs: ${PMDAEMON[*]} logs $PROC_NAME --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
