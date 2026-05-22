#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

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
用法: intra_scripts/start_intra_pre_trade.sh [--resample-suffix <suffix>]

说明:
  - 同所期现：从 env.sh 读取 OPEN_VENUE/HEDGE_VENUE/IPC_NAMESPACE
  - 启动 1 个 pmdaemon 进程：intra_pt_<exchange>_<env>
  - 建议先生成 env: scripts/deploy_setup_env_intra.sh --env-name <exchange>-intra-<tag> --exchange <exchange>
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

RESAMPLE_SUFFIX=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --resample-suffix)
      RESAMPLE_SUFFIX="${2:-}"
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

# 同所期现目录约定：<exchange>-intra-<tag>
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

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 exchange (dir=$dir_name)，期望 <exchange>-intra-<tag>"
  exit 1
fi

# OPEN_VENUE / HEDGE_VENUE 优先来自 env.sh，否则按 exchange 默认
OPEN_VENUE="${OPEN_VENUE:-${EXCHANGE}-margin}"
HEDGE_VENUE="${HEDGE_VENUE:-${EXCHANGE}-futures}"

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE 未设置（env.sh 缺失或未 source）。"
  echo "[ERROR] 请先执行：scripts/deploy_setup_env_intra.sh --env-name ${dir_name} --exchange ${EXCHANGE}"
  exit 1
fi

DEFAULT_PROC_NAME="intra_pt_${EXCHANGE}_${ENV_TAG}"
PROC_NAME="${PMDAEMON_NAME:-${PM2_NAME:-$DEFAULT_PROC_NAME}}"

# CPU core binding lookup (optional, per-host table at ~/.mkt_signal_cores.sh).
# 不存在或没匹配条目 → 不传 --core，binary 自然跳过绑核。
CORE_BIND_TABLE="${MKT_CORE_BIND_TABLE:-$HOME/.mkt_signal_cores.sh}"
if [[ -f "$CORE_BIND_TABLE" ]]; then
  # shellcheck disable=SC1090
  source "$CORE_BIND_TABLE"
fi
core_args=()
if declare -p MKT_CORE_BINDINGS >/dev/null 2>&1; then
  _bind_key="${dir_name}:pre_trade"
  if [[ -n "${MKT_CORE_BINDINGS[$_bind_key]:-}" ]]; then
    core_args=(--core "${MKT_CORE_BINDINGS[$_bind_key]}")
    echo "[INFO] core bind ${MKT_CORE_BINDINGS[$_bind_key]} (table=$CORE_BIND_TABLE key=$_bind_key)"
  fi
fi

args=(--open-venue "$OPEN_VENUE" --hedge-venue "$HEDGE_VENUE")
if [[ -n "$RESAMPLE_SUFFIX" ]]; then
  args+=(--resample-suffix "$RESAMPLE_SUFFIX")
fi
if [[ ${#core_args[@]} -gt 0 ]]; then
  args+=("${core_args[@]}")
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

echo "[INFO] Restarting $PROC_NAME (exchange=$EXCHANGE open=$OPEN_VENUE hedge=$HEDGE_VENUE namespace=$IPC_NAMESPACE)"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started $PROC_NAME"
echo "[INFO] Logs: ${PMDAEMON[*]} logs $PROC_NAME --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
