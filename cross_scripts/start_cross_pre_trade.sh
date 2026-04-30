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
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] pre_trade binary not found. Build/deploy first."
  exit 1
fi

usage() {
  cat <<'USAGE'
用法: cross_scripts/start_cross_pre_trade.sh [--resample-suffix <suffix>] [--open-venue <okex-futures>] [--hedge-venue <binance-futures>]

说明:
  - 默认从 env.sh 读取 OPEN_VENUE/HEDGE_VENUE/IPC_NAMESPACE；若未提供则尝试从目录名推断 open/hedge。
  - 将以 pmdaemon 启动 1 个进程：
      跨所: cross_pt_<open>_<hedge>_<env> -> pre_trade --open-venue ... --hedge-venue ...
      同所: cross_pt_<exchange>_<env>     -> pre_trade --open-venue ... --hedge-venue ...
  - 建议先生成并配置 env.sh（包含 IPC_NAMESPACE/凭证等）：
      scripts/deploy_setup_env_cross.sh --env-name <open>-<hedge>-cross-... --open-venue ... --hedge-venue ...
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

RESAMPLE_SUFFIX=""
CLI_OPEN_VENUE=""
CLI_HEDGE_VENUE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --resample-suffix)
      RESAMPLE_SUFFIX="${2:-}"
      shift 2
      ;;
    --open-venue)
      CLI_OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      CLI_HEDGE_VENUE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

ensure_pmdaemon

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[WARN] 未找到 env.sh：${ENV_FILE}"
  echo "[WARN] 若 IPC_NAMESPACE 未设置，pre_trade 会直接 panic；建议先生成并配置 env.sh。"
fi

normalize_venue() {
  echo "${1,,}"
}

ensure_cross_venue() {
  local v
  v="$(normalize_venue "$1")"
  if [[ -z "$v" || ! "$v" =~ ^[a-z0-9]+-(margin|futures|spot|swap|perp|perpetual)$ ]]; then
    echo "[ERROR] 非法 cross venue: $1"
    exit 1
  fi
  echo "$v"
}

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

infer_pair_from_dir() {
  local name="$1"
  local open_ex=""
  local hedge_ex=""
  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
  fi
  if [[ "$open_ex" == "okx" ]]; then
    open_ex="okex"
  fi
  if [[ "$hedge_ex" == "okx" ]]; then
    hedge_ex="okex"
  fi
  if [[ -n "$open_ex" && -n "$hedge_ex" ]]; then
    echo "${open_ex},${hedge_ex}"
  fi
}

infer_env_tag_from_dir() {
  local name="${1,,}"
  if [[ "$name" =~ ^[a-z0-9]+[-_][a-z0-9]+[-_]cross[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
    echo "${BASH_REMATCH[1]//-/_}"
    return
  fi
  echo "cross"
}

OPEN_VENUE="${CLI_OPEN_VENUE:-${OPEN_VENUE:-}}"
HEDGE_VENUE="${CLI_HEDGE_VENUE:-${HEDGE_VENUE:-}}"

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  if inferred="$(infer_pair_from_dir "$dir_lc")" && [[ -n "$inferred" ]]; then
    if [[ "${inferred%%,*}" == "${inferred##*,}" ]]; then
      OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-margin}"
      HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
    else
      OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-futures}"
      HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
    fi
  fi
fi

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 缺少 open/hedge venue，且无法从目录名推断 (dir=$dir_name)"
  usage
  exit 1
fi

OPEN_VENUE="$(ensure_cross_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_cross_venue "$HEDGE_VENUE")"
if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] cross open/hedge venue 不能完全相同：open=$OPEN_VENUE hedge=$HEDGE_VENUE"
  exit 1
fi

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE 未设置（env.sh 缺失或未 source）。"
  echo "[ERROR] 请先执行：scripts/deploy_setup_env_cross.sh --env-name ${dir_name} --open-venue ${OPEN_VENUE} --hedge-venue ${HEDGE_VENUE}"
  exit 1
fi

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
ENV_TAG="$(infer_env_tag_from_dir "$dir_lc")"
if [[ "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
  DEFAULT_PROC_NAME="cross_pt_${OPEN_EXCHANGE}_${ENV_TAG}"
else
  DEFAULT_PROC_NAME="cross_pt_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_${ENV_TAG}"
fi
PROC_NAME="${PMDAEMON_NAME:-${PM2_NAME:-$DEFAULT_PROC_NAME}}"

args=(--open-venue "$OPEN_VENUE" --hedge-venue "$HEDGE_VENUE")
if [[ -n "$RESAMPLE_SUFFIX" ]]; then
  args+=(--resample-suffix "$RESAMPLE_SUFFIX")
fi

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
json_rust_log="$(json_escape "${RUST_LOG:-info}")"
json_ipc_ns="$(json_escape "$IPC_NAMESPACE")"
cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN_PATH")"
for arg in "${args[@]}"; do
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
        "RUST_LOG": "${json_rust_log}",
        "IPC_NAMESPACE": "${json_ipc_ns}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting $PROC_NAME (open=$OPEN_VENUE hedge=$HEDGE_VENUE namespace=$IPC_NAMESPACE)"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started $PROC_NAME"
echo "[INFO] Logs: ${PMDAEMON[*]} logs $PROC_NAME --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
