#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_intra_binance_std.sh --env-suffix <suffix> [--bin]
  scripts/deploy_intra_binance_std.sh <suffix>

说明:
  - 默认只部署到本机 $HOME/binance-intra-<suffix>/（不启动进程）。
  - 部署 Binance std 同所期现 intra 环境：
      open=binance-margin
      hedge=binance-futures
      BINANCE_ACCOUNT_MODE=STANDARD
  - 环境目录固定: $HOME/binance-intra-<suffix>
  - 仅部署，不启动任何进程
  - 支持 suffix: arb01、arb02、arb03
  - 固定端口（binance intra）:
      arb01 -> 19171
      arb02 -> 19172
      arb03 -> 19173
  - --bin: 跳过 env/config_server，仅更新主要进程部署产物
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

ENV_SUFFIX=""
BIN_MODE="0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix) ENV_SUFFIX="${2:-}"; shift 2 ;;
    --bin)        BIN_MODE="1"; shift ;;
    --remote)
      echo "[ERROR] --remote 已移除；binance intra 入口现在只部署本机" >&2
      exit 1 ;;
    -h|--help)    usage; exit 0 ;;
    *)
      if [[ -z "$ENV_SUFFIX" ]]; then
        ENV_SUFFIX="$1"; shift
      else
        echo "[ERROR] 未知参数: $1" >&2; usage; exit 1
      fi
      ;;
  esac
done

if [[ -z "$ENV_SUFFIX" ]]; then
  echo "[ERROR] 需要传入 env suffix（arb01|arb02|arb03）" >&2
  usage; exit 1
fi

ENV_SUFFIX="$(echo "$ENV_SUFFIX" | tr 'A-Z' 'a-z')"
case "$ENV_SUFFIX" in
  arb01) CONFIG_PORT="19171" ;;
  arb02) CONFIG_PORT="19172" ;;
  arb03) CONFIG_PORT="19173" ;;
  *)
    echo "[ERROR] 仅支持 suffix: arb01|arb02|arb03（收到: ${ENV_SUFFIX}）" >&2
    exit 1 ;;
esac

EXCHANGE="binance"
ENV_NAME="${EXCHANGE}-intra-${ENV_SUFFIX}"
INTRA_ENV_SUFFIX="intra-${ENV_SUFFIX}"
ENV_FILE="$HOME/${ENV_NAME}/env.sh"
TARGET_DIR="$HOME/${ENV_NAME}"

if [[ "$BIN_MODE" == "1" && ! -d "$TARGET_DIR" ]]; then
  echo "[ERROR] --bin 模式要求本地环境目录已存在: $HOME/$ENV_NAME" >&2
  exit 1
fi

run_deploy() {
  local cmd=("$@")
  echo "[RUN] ${cmd[*]}"
  local output status
  set +e
  output="$("${cmd[@]}" 2>&1)"
  status=$?
  set -e
  [[ -n "$output" ]] && echo "$output"
  if [[ "$status" -eq 0 ]]; then return 0; fi
  if echo "$output" | grep -Eiq "text file busy|text busy|etxtbsy"; then
    echo "[WARN] 检测到 Text file busy，跳过并继续"; return 0
  fi
  echo "[ERROR] 命令失败，停止部署: ${cmd[*]}" >&2
  return "$status"
}

ensure_binance_standard_mode() {
  local env_file="$1"
  if [[ ! -f "$env_file" ]]; then
    echo "[ERROR] env.sh 不存在: $env_file" >&2; exit 1
  fi
  local target='export BINANCE_ACCOUNT_MODE="STANDARD"'
  if grep -Fqx "$target" "$env_file"; then return 0; fi
  if grep -Eq '^export BINANCE_ACCOUNT_MODE=' "$env_file"; then
    python3 - "$env_file" <<'PY'
import pathlib, re, sys
path = pathlib.Path(sys.argv[1])
text = path.read_text()
text = re.sub(
    r'^export BINANCE_ACCOUNT_MODE=.*$',
    'export BINANCE_ACCOUNT_MODE="STANDARD"',
    text, count=1, flags=re.M,
)
path.write_text(text)
PY
  else
    printf '\n# Binance same-exchange (intra) account mode\n%s\n' "$target" >> "$env_file"
  fi
}

configure_binance_arb01_core_layout() {
  if [[ "$ENV_NAME" != "binance-intra-arb01" ]]; then
    return 0
  fi
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "[WARN] $ENV_FILE 不存在，跳过 binance-intra-arb01 core layout 写入" >&2
    return 0
  fi
  intra_upsert_env_exports_block \
    "$ENV_FILE" \
    "managed binance-intra-arb01 core layout" \
    "Local binance intra arb01 core layout: hot path on cores 16-20; persist_manager helper on overflow core 26." \
    "ACCOUNT_MONITOR_CORE='16'" \
    "TRADE_SIGNAL_CORE='17'" \
    "PRE_TRADE_CORE='18'" \
    "TRADE_ENGINE_CORE='19'" \
    "TRADE_ENGINE_IPC_CORE='20'" \
    "PERSIST_MANAGER_CORE='26'"
  echo "[INFO] binance-intra-arb01 core layout written to $ENV_FILE"
}

echo "[INFO] Binance std intra deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] exchange=${EXCHANGE} (open=${EXCHANGE}-margin, hedge=${EXCHANGE}-futures)"
echo "[INFO] config_port=${CONFIG_PORT}"
echo "[INFO] 不会执行 start 命令"
[[ "$BIN_MODE" == "1" ]] && echo "[INFO] mode=bin"
echo "[INFO] target=local ${TARGET_DIR}"

cd "$ROOT_DIR"

if [[ "$BIN_MODE" != "1" ]]; then
  run_deploy bash scripts/deploy_setup_env_intra.sh \
    --env-name "$ENV_NAME" \
    --env-suffix "$INTRA_ENV_SUFFIX" \
    --exchange "$EXCHANGE"

  ensure_binance_standard_mode "$ENV_FILE"
fi

configure_binance_arb01_core_layout

if [[ "$BIN_MODE" != "1" ]]; then
  run_deploy bash scripts/deploy_intra_config_server.sh \
    --env-name "$ENV_NAME" \
    --exchange "$EXCHANGE" \
    --port "$CONFIG_PORT"
fi

run_deploy bash scripts/deploy_intra_monitors.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$INTRA_ENV_SUFFIX" \
  --exchange "$EXCHANGE"

run_deploy bash scripts/deploy_intra_trade_engine.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$INTRA_ENV_SUFFIX" \
  --exchange "$EXCHANGE"

run_deploy bash scripts/deploy_intra_viz_server.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$INTRA_ENV_SUFFIX" \
  --exchange "$EXCHANGE"

run_deploy bash scripts/deploy_intra_persist_manager.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$INTRA_ENV_SUFFIX" \
  --exchange "$EXCHANGE"

run_deploy bash scripts/deploy_intra_pre_trade.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$INTRA_ENV_SUFFIX" \
  --exchange "$EXCHANGE" \
  --sync-scripts

run_deploy bash scripts/deploy_intra_trade_signal.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$INTRA_ENV_SUFFIX" \
  --exchange "$EXCHANGE" \
  --sync-scripts

if [[ "$BIN_MODE" != "1" ]]; then
  ensure_binance_standard_mode "$ENV_FILE"
  configure_binance_arb01_core_layout
fi

echo "[INFO] Binance std intra 部署完成（未启动进程）"
echo "[INFO] 本机环境目录: ${TARGET_DIR}"
