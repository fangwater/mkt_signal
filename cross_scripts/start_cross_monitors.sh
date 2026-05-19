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
用法: cross_scripts/start_cross_monitors.sh

说明:
  - 启动 cross 所需的两侧账户 monitor（支持 binance/okex/bybit/bitget/gate）。
  - 会优先从 env.sh 读取 OPEN_VENUE/HEDGE_VENUE；
    若没有，则从部署目录名推断：<open>-<hedge>-cross-...
  - 跨所会启动两个 pmdaemon 进程：
      cross_am_<open>_<hedge>_<env>_open   -> account_monitor_<open_exchange>
      cross_am_<open>_<hedge>_<env>_hedge  -> account_monitor_<hedge_exchange>
  - 同所只启动一个 pmdaemon 进程：
      cross_am_<exchange>_<env> -> account_monitor_<exchange>

前置:
  - 必须设置 IPC_NAMESPACE（建议在部署目录生成 env.sh）：
      scripts/deploy_setup_env_cross.sh --env-name <open>-<hedge>-cross-... --open-venue ... --hedge-venue ...
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ensure_pmdaemon

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[WARN] 未找到 env.sh：${ENV_FILE}"
fi

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

infer_pair_from_dir() {
  local name="${1,,}"
  local open_ex=""
  local hedge_ex=""
  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?([_-](open|hedge))?$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
  fi
  if [[ -n "$open_ex" && -n "$hedge_ex" ]]; then
    echo "$(normalize_exchange "$open_ex"),$(normalize_exchange "$hedge_ex")"
  fi
}

infer_env_tag_from_dir() {
  local name="${1,,}"
  if [[ "$name" =~ ^[a-z0-9]+[-_][a-z0-9]+[-_]cross[-_]([a-z0-9][a-z0-9_-]*)[-_](open|hedge)$ ]]; then
    echo "${BASH_REMATCH[1]//-/_}"
    return
  fi
  if [[ "$name" =~ ^[a-z0-9]+[-_][a-z0-9]+[-_]cross[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
    echo "${BASH_REMATCH[1]//-/_}"
    return
  fi
  echo "cross"
}

infer_side_from_dir() {
  local name="${1,,}"
  if [[ "$name" =~ ^[a-z0-9]+[-_][a-z0-9]+[-_]cross[-_][a-z0-9][a-z0-9_-]*[-_](open|hedge)$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return
  fi
  echo ""
}

OPEN_EXCHANGE=""
HEDGE_EXCHANGE=""
if [[ -n "${OPEN_VENUE:-}" ]]; then
  OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
fi
if [[ -n "${HEDGE_VENUE:-}" ]]; then
  HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
fi

OPEN_EXCHANGE="$(normalize_exchange "${OPEN_EXCHANGE:-}")"
HEDGE_EXCHANGE="$(normalize_exchange "${HEDGE_EXCHANGE:-}")"

if [[ -z "$OPEN_EXCHANGE" || -z "$HEDGE_EXCHANGE" ]]; then
  if inferred="$(infer_pair_from_dir "$(basename "$BASE_DIR")")" && [[ -n "$inferred" ]]; then
    OPEN_EXCHANGE="${inferred%%,*}"
    HEDGE_EXCHANGE="${inferred##*,}"
  fi
fi

if [[ -z "$OPEN_EXCHANGE" || -z "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] 无法确定 cross open/hedge exchange（需 env.sh 或目录名 <open>-<hedge>-cross-...）"
  usage
  exit 1
fi

ENV_TAG="$(infer_env_tag_from_dir "$(basename "$BASE_DIR")")"
SIDE_TAG="${CROSS_SIDE:-$(infer_side_from_dir "$(basename "$BASE_DIR")")}"

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE 未设置（请 source env.sh）"
  echo "[ERROR] 建议: scripts/deploy_setup_env_cross.sh --env-name $(basename "$BASE_DIR") --open-venue ${OPEN_EXCHANGE}-futures --hedge-venue ${HEDGE_EXCHANGE}-futures"
  exit 1
fi

bin_for_exchange() {
  local ex="$1"
  case "$ex" in
    binance|okex|bybit|bitget|gate) ;;
    *)
      return 1
      ;;
  esac

  local candidates=(
    "${BASE_DIR}/account_monitor_${ex}"
    "${SCRIPT_DIR}/account_monitor_${ex}"
    "${BASE_DIR}/${ex}_account_monitor"
    "${SCRIPT_DIR}/${ex}_account_monitor"
  )
  for cand in "${candidates[@]}"; do
    if [[ -x "$cand" ]]; then
      echo "$cand"
      return 0
    fi
  done
  return 1
}

TMP_CFGS=()
cleanup_tmp_cfgs() {
  if [[ ${#TMP_CFGS[@]} -gt 0 ]]; then
    rm -f "${TMP_CFGS[@]}" >/dev/null 2>&1 || true
  fi
}
trap cleanup_tmp_cfgs EXIT

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

shell_quote() {
  printf '%q' "$1"
}

proc_base_name() {
  if [[ "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
    echo "cross_am_${OPEN_EXCHANGE}_${ENV_TAG}"
  else
    echo "cross_am_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_${ENV_TAG}"
  fi
}

proc_name_for_side() {
  local side="$1"
  local base
  base="$(proc_base_name)"
  if [[ "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
    echo "$base"
  else
    echo "${base}_${side}"
  fi
}

start_one() {
  local side="$1"
  local ex="$2"
  local proc_name
  proc_name="$(proc_name_for_side "$side")"

  local bin
  if ! bin="$(bin_for_exchange "$ex")"; then
    echo "[ERROR] 未找到 account monitor 二进制 for exchange=${ex}"
    echo "[ERROR] 期望存在: ${BASE_DIR}/account_monitor_${ex} 或 ${BASE_DIR}/${ex}_account_monitor"
    echo "[ERROR] 请先部署: scripts/deploy_cross_monitors.sh --env-name $(basename "$BASE_DIR") --open-venue ${OPEN_EXCHANGE}-futures --hedge-venue ${HEDGE_EXCHANGE}-futures"
    exit 1
  fi

  local cfg_file
  cfg_file="$(mktemp)"
  TMP_CFGS+=("$cfg_file")

  local json_name json_base json_rust_log json_ipc_ns json_shell json_cmd cmd
  json_name="$(json_escape "$proc_name")"
  json_shell="$(json_escape "/bin/bash")"
  json_base="$(json_escape "$BASE_DIR")"
  json_rust_log="$(json_escape "${RUST_LOG:-info}")"
  json_ipc_ns="$(json_escape "$IPC_NAMESPACE")"
  cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$bin")"
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

  echo "[INFO] Restarting $proc_name (exchange=$ex namespace=$IPC_NAMESPACE)"
  "${PMDAEMON[@]}" delete "$proc_name" >/dev/null 2>&1 || true
  "${PMDAEMON[@]}" --config "$cfg_file" start --name "$proc_name"
}

if [[ "$SIDE_TAG" == "hedge" ]]; then
  start_one "hedge" "$HEDGE_EXCHANGE"
elif [[ "$SIDE_TAG" == "open" ]]; then
  start_one "open" "$OPEN_EXCHANGE"
else
  start_one "open" "$OPEN_EXCHANGE"
fi
if [[ -z "$SIDE_TAG" && "$HEDGE_EXCHANGE" != "$OPEN_EXCHANGE" ]]; then
  sleep 0.5
  start_one "hedge" "$HEDGE_EXCHANGE"
fi

echo "[INFO] Started:"
if [[ "$SIDE_TAG" == "hedge" ]]; then
  echo "  - $(proc_name_for_side hedge)"
elif [[ "$SIDE_TAG" == "open" ]]; then
  echo "  - $(proc_name_for_side open)"
else
  echo "  - $(proc_name_for_side open)"
fi
if [[ -z "$SIDE_TAG" && "$HEDGE_EXCHANGE" != "$OPEN_EXCHANGE" ]]; then
  echo "  - $(proc_name_for_side hedge)"
fi
if [[ "$SIDE_TAG" == "hedge" ]]; then
  echo "[INFO] Logs: ${PMDAEMON[*]} logs $(proc_name_for_side hedge) --follow"
else
  echo "[INFO] Logs: ${PMDAEMON[*]} logs $(proc_name_for_side open) --follow"
fi
echo "[INFO] Status: ${PMDAEMON[*]} list"
