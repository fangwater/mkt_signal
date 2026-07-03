#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_intra_okex.sh --env-suffix <suffix> [--bin] [--skip-nginx-apply]
  scripts/deploy_intra_okex.sh <suffix>

说明:
  - 部署 OKEx 同所期现 intra 环境：
      open=okex-margin
      hedge=okex-futures
  - 环境目录固定: $HOME/okex-intra-<suffix>
  - 仅部署，不启动任何进程
  - 支持 suffix: arb01、arb02、arb03
  - 固定端口（okex intra）:
      arb01 -> 19181
      arb02 -> 19182
      arb03 -> 19183
  - --bin: 跳过 env/config_server，仅更新主要进程部署产物
  - --skip-nginx-apply: 只更新 nginx mapping 文件，不执行 sudo nginx reload
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

ENV_SUFFIX=""
BIN_MODE="0"
APPLY_NGINX="1"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix) ENV_SUFFIX="${2:-}"; shift 2 ;;
    --bin)        BIN_MODE="1"; shift ;;
    --skip-nginx-apply) APPLY_NGINX="0"; shift ;;
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
  arb01) CONFIG_PORT="19181" ;;
  arb02) CONFIG_PORT="19182" ;;
  arb03) CONFIG_PORT="19183" ;;
  *)
    echo "[ERROR] 仅支持 suffix: arb01|arb02|arb03（收到: ${ENV_SUFFIX}）" >&2
    exit 1 ;;
esac

EXCHANGE="okex"
ENV_NAME="${EXCHANGE}-intra-${ENV_SUFFIX}"
INTRA_ENV_SUFFIX="intra-${ENV_SUFFIX}"
TARGET_DIR="$HOME/$ENV_NAME"

if [[ "$BIN_MODE" == "1" && ! -d "$HOME/$ENV_NAME" ]]; then
  echo "[ERROR] --bin 模式要求环境目录已存在: $HOME/$ENV_NAME" >&2
  exit 1
fi


configure_hk_okex_arb01_core_layout() {
  if [[ "$ENV_NAME" != "okex-intra-arb01" ]]; then
    return 0
  fi
  if [[ ! -f "$TARGET_DIR/env.sh" ]]; then
    echo "[WARN] $TARGET_DIR/env.sh 不存在，跳过 HK okex-intra-arb01 core layout 写入" >&2
    return 0
  fi
  intra_upsert_env_exports_block \
    "$TARGET_DIR/env.sh" \
    "managed HK isolated-core layout" \
    "HK el-cc-okx-srv01: housekeeping=0-3, okex intra hot cores=4,6,8,10, OKEX spread_pbs cores=12,14; keep sibling CPUs 5,7,9,11,13,15 unused/offline." \
    "OKEX_INTRA_HOT_CORES='4,6,8,10'" \
    "TRADE_ENGINE_CORE='4'" \
    "TRADE_ENGINE_IPC_CORE='6'" \
    "PRE_TRADE_CORE='8'" \
    "TRADE_SIGNAL_CORE='10'"
  echo "[INFO] HK okex-intra-arb01 core layout written to $TARGET_DIR/env.sh"
}

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

echo "[INFO] OKEx intra deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] exchange=${EXCHANGE} (open=${EXCHANGE}-margin, hedge=${EXCHANGE}-futures)"
echo "[INFO] config_port=${CONFIG_PORT}"
echo "[INFO] 不会执行 start 命令"
[[ "$BIN_MODE" == "1" ]] && echo "[INFO] mode=bin"

cd "$ROOT_DIR"

if [[ "$BIN_MODE" != "1" ]]; then
  run_deploy bash scripts/deploy_setup_env_intra.sh \
    --env-name "$ENV_NAME" \
    --env-suffix "$INTRA_ENV_SUFFIX" \
    --exchange "$EXCHANGE"

  config_server_args=(
    bash scripts/deploy_intra_config_server.sh
    --env-name "$ENV_NAME"
    --exchange "$EXCHANGE"
    --port "$CONFIG_PORT"
  )
  if [[ "$APPLY_NGINX" == "1" ]]; then
    config_server_args+=(--apply-nginx)
  fi
  run_deploy "${config_server_args[@]}"
fi

configure_hk_okex_arb01_core_layout

run_deploy bash scripts/deploy_intra_monitors.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$INTRA_ENV_SUFFIX" \
  --exchange "$EXCHANGE"

run_deploy bash scripts/deploy_intra_trade_engine.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$INTRA_ENV_SUFFIX" \
  --exchange "$EXCHANGE"

viz_server_args=(
  bash scripts/deploy_intra_viz_server.sh
  --env-name "$ENV_NAME"
  --env-suffix "$INTRA_ENV_SUFFIX"
  --exchange "$EXCHANGE"
)
if [[ "$APPLY_NGINX" == "1" ]]; then
  viz_server_args+=(--apply-nginx)
fi
run_deploy "${viz_server_args[@]}"

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

echo "[INFO] OKEx intra 部署完成（仅 deploy，不含 start）"
echo "[INFO] 环境目录: $HOME/$ENV_NAME"
