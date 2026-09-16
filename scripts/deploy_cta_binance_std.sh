#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_cta_binance_std.sh --env-suffix <suffix> [--bin]
  scripts/deploy_cta_binance_std.sh <suffix>

说明:
  - 默认只部署到本机 $HOME/binance-cta-<suffix>/（不启动进程）。
  - 部署 Binance 同所期现 cta 环境：
      open=binance-margin
      hedge=binance-futures
      执行后端固定为 ltp（RapidX portfolio credentials）
  - 环境目录固定: $HOME/binance-cta-<suffix>
  - 仅部署，不启动任何进程
  - 支持 suffix: rx01
  - 固定端口（binance cta）:
      rx01 -> 19174 (config server), 10186 (viz server)
  - --bin: 跳过 env/config_server，仅更新主要进程部署产物
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

ENV_SUFFIX=""
BIN_MODE="0"
EXEC_BACKEND="ltp"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix) ENV_SUFFIX="${2:-}"; shift 2 ;;
    --bin)        BIN_MODE="1"; shift ;;
    --exec-backend)
      case "${2:-}" in
        ltp|rapidx) EXEC_BACKEND="ltp" ;;
        *) echo "[ERROR] cta 当前仅支持 ltp/rapidx 执行后端（收到: ${2:-}）" >&2; exit 1 ;;
      esac
      shift 2 ;;
    --remote)
      echo "[ERROR] --remote 已移除；binance cta 入口现在只部署本机" >&2
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
  echo "[ERROR] 需要传入 env suffix（rx01）" >&2
  usage; exit 1
fi

ENV_SUFFIX="$(echo "$ENV_SUFFIX" | tr 'A-Z' 'a-z')"
case "$ENV_SUFFIX" in
  rx01) CONFIG_PORT="19174" ;;
  *)
    echo "[ERROR] 仅支持 suffix: rx01（收到: ${ENV_SUFFIX}）" >&2
    exit 1 ;;
esac

EXCHANGE="binance"
ENV_NAME="${EXCHANGE}-cta-${ENV_SUFFIX}"
CTA_ENV_SUFFIX="cta-${ENV_SUFFIX}"
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

configure_binance_core_layout() {
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "[WARN] $ENV_FILE 不存在，跳过 ${ENV_NAME} core layout 写入" >&2
    return 0
  fi
  local persist_core
  case "$ENV_NAME" in
    binance-cta-rx01)
      persist_core=15
      ;;
    *) return 0 ;;
  esac
  intra_upsert_env_exports_block \
    "$ENV_FILE" \
    "managed ${ENV_NAME} core layout" \
    "Local ${ENV_NAME} core layout; only persist_manager is pinned." \
    "PERSIST_MANAGER_CORE='${persist_core}'"
  echo "[INFO] ${ENV_NAME} core layout written to $ENV_FILE"
}

echo "[INFO] Binance std cta deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] exchange=${EXCHANGE} (open=${EXCHANGE}-margin, hedge=${EXCHANGE}-futures)"
echo "[INFO] config_port=${CONFIG_PORT}"
echo "[INFO] exec_backend=${EXEC_BACKEND}"
echo "[INFO] 不会执行 start 命令"
[[ "$BIN_MODE" == "1" ]] && echo "[INFO] mode=bin"
echo "[INFO] target=local ${TARGET_DIR}"

cd "$ROOT_DIR"

if [[ "$BIN_MODE" != "1" ]]; then
  run_deploy bash scripts/deploy_setup_env_intra.sh \
    --env-name "$ENV_NAME" \
    --env-suffix "$CTA_ENV_SUFFIX" \
    --exchange "$EXCHANGE" \
    --exec-backend "$EXEC_BACKEND"
fi

configure_binance_core_layout

if [[ "$BIN_MODE" != "1" ]]; then
  run_deploy bash scripts/deploy_intra_config_server.sh \
    --env-name "$ENV_NAME" \
    --exchange "$EXCHANGE" \
    --port "$CONFIG_PORT"
fi

run_deploy bash scripts/deploy_intra_monitors.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CTA_ENV_SUFFIX" \
  --exchange "$EXCHANGE" \
  --exec-backend "$EXEC_BACKEND"

run_deploy bash scripts/deploy_intra_trade_engine.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CTA_ENV_SUFFIX" \
  --exchange "$EXCHANGE"

run_deploy bash scripts/deploy_intra_viz_server.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CTA_ENV_SUFFIX" \
  --exchange "$EXCHANGE"

run_deploy bash scripts/deploy_intra_persist_manager.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CTA_ENV_SUFFIX" \
  --exchange "$EXCHANGE"

run_deploy bash scripts/deploy_intra_pre_trade.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CTA_ENV_SUFFIX" \
  --exchange "$EXCHANGE" \
  --sync-scripts

run_deploy bash scripts/deploy_intra_trade_signal.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CTA_ENV_SUFFIX" \
  --exchange "$EXCHANGE" \
  --sync-scripts

if [[ "$BIN_MODE" != "1" ]]; then
  configure_binance_core_layout
fi

echo "[INFO] Binance std cta 部署完成（未启动进程）"
echo "[INFO] 本机环境目录: ${TARGET_DIR}"
