#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_xarb_binance_std.sh --env-suffix <suffix> [--bin]
  scripts/deploy_xarb_binance_std.sh <suffix>

说明:
  - 部署 Binance std 期现 xarb 环境：
      open=binance-margin
      hedge=binance-futures
      BINANCE_ACCOUNT_MODE=STANDARD
  - 环境目录固定为：$HOME/binance-binance-xarb-<suffix>
  - 只部署，不启动任何进程。
  - 支持 suffix: trade01、trade02、trade03
  - --bin: 跳过 env/config_server，仅更新主要进程部署产物
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX=""
BIN_MODE="0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix)
      ENV_SUFFIX="${2:-}"
      shift 2
      ;;
    --bin)
      BIN_MODE="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "$ENV_SUFFIX" ]]; then
        ENV_SUFFIX="$1"
        shift
      else
        echo "[ERROR] 未知参数: $1" >&2
        usage
        exit 1
      fi
      ;;
  esac
done

if [[ -z "$ENV_SUFFIX" ]]; then
  echo "[ERROR] 需要传入 env suffix（trade01|trade02|trade03）" >&2
  usage
  exit 1
fi

ENV_SUFFIX="$(echo "$ENV_SUFFIX" | tr 'A-Z' 'a-z')"
case "$ENV_SUFFIX" in
  trade01)
    CONFIG_PORT="18151"
    ;;
  trade02)
    CONFIG_PORT="18152"
    ;;
  trade03)
    CONFIG_PORT="18153"
    ;;
  *)
    echo "[ERROR] Binance std xarb 仅支持 suffix: trade01|trade02|trade03（收到: ${ENV_SUFFIX}）" >&2
    exit 1
    ;;
esac

OPEN_VENUE="binance-margin"
HEDGE_VENUE="binance-futures"
ENV_NAME="binance-binance-xarb-${ENV_SUFFIX}"
XARB_ENV_SUFFIX="xarb-${ENV_SUFFIX}"
ENV_FILE="$HOME/${ENV_NAME}/env.sh"

if [[ "$BIN_MODE" == "1" && ! -d "$HOME/$ENV_NAME" ]]; then
  echo "[ERROR] --bin 模式要求环境目录已存在: $HOME/$ENV_NAME" >&2
  exit 1
fi

run_deploy() {
  local cmd=("$@")
  echo "[RUN] ${cmd[*]}"
  local output
  local status
  set +e
  output="$("${cmd[@]}" 2>&1)"
  status=$?
  set -e

  if [[ -n "$output" ]]; then
    echo "$output"
  fi

  if [[ "$status" -eq 0 ]]; then
    return 0
  fi

  if echo "$output" | grep -Eiq "text file busy|text busy|etxtbsy"; then
    echo "[WARN] 检测到 Text file busy，跳过该步骤并继续后续部署"
    return 0
  fi

  echo "[ERROR] 命令失败，停止部署: ${cmd[*]}" >&2
  return "$status"
}

ensure_binance_standard_mode() {
  local env_file="$1"
  if [[ ! -f "$env_file" ]]; then
    echo "[ERROR] env.sh 不存在: $env_file" >&2
    exit 1
  fi

  local target='export BINANCE_ACCOUNT_MODE="STANDARD"'
  if grep -Fqx "$target" "$env_file"; then
    return 0
  fi

  if grep -Eq '^export BINANCE_ACCOUNT_MODE=' "$env_file"; then
    python3 - "$env_file" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
text = re.sub(
    r'^export BINANCE_ACCOUNT_MODE=.*$',
    'export BINANCE_ACCOUNT_MODE="STANDARD"',
    text,
    count=1,
    flags=re.M,
)
path.write_text(text)
PY
  else
    printf '\n# Binance same-exchange xarb account mode\n%s\n' "$target" >> "$env_file"
  fi
}

echo "[INFO] Binance std xarb deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] open=${OPEN_VENUE}, hedge=${HEDGE_VENUE}"
echo "[INFO] config_port=${CONFIG_PORT}"
echo "[INFO] 不会执行 start 命令"
if [[ "$BIN_MODE" == "1" ]]; then
  echo "[INFO] mode=bin"
fi

cd "$ROOT_DIR"

if [[ "$BIN_MODE" != "1" ]]; then
  run_deploy bash scripts/deploy_setup_env_xarb.sh \
    --env-name "$ENV_NAME" \
    --env-suffix "$XARB_ENV_SUFFIX" \
    --open-venue "$OPEN_VENUE" \
    --hedge-venue "$HEDGE_VENUE"

  ensure_binance_standard_mode "$ENV_FILE"

  run_deploy bash scripts/deploy_xarb_config_server.sh \
    --env-name "$ENV_NAME" \
    --open-venue "$OPEN_VENUE" \
    --hedge-venue "$HEDGE_VENUE" \
    --port "$CONFIG_PORT" \
    --apply-nginx
fi

run_deploy bash scripts/deploy_xarb_monitors.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$XARB_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE"

run_deploy bash scripts/deploy_xarb_trade_engine.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$XARB_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE"

run_deploy bash scripts/deploy_xarb_viz_server.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$XARB_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE" \
  --apply-nginx

run_deploy bash scripts/deploy_xarb_persist_manager.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$XARB_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE"

run_deploy bash scripts/deploy_xarb_pre_trade.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$XARB_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE" \
  --sync-scripts

run_deploy bash scripts/deploy_xarb_trade_signal.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$XARB_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE" \
  --sync-scripts

if [[ "$BIN_MODE" != "1" ]]; then
  ensure_binance_standard_mode "$ENV_FILE"
fi

echo "[INFO] Binance std xarb 部署完成（仅 deploy，不含 start）"
echo "[INFO] 环境目录: $HOME/$ENV_NAME"
