#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 强制路由到 SG (bybit + binance-futures 数据都已本地化在 SG).
export FR_DEPLOY_HOST="${FR_DEPLOY_HOST:-ubuntu@47.131.162.78}"
export FR_DEPLOY_KEY="${FR_DEPLOY_KEY:-$ROOT_DIR/aws-sg.pem}"
export FR_REMOTE_HOME="${FR_REMOTE_HOME:-/home/ubuntu}"

# shellcheck source=lib/fr_remote_deploy.sh
source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_cross_bybit_binance.sh --env-suffix <suffix> [--bin]
  scripts/deploy_cross_bybit_binance.sh <suffix>

说明:
  - 部署到 SG (ubuntu@47.131.162.78)，不启动进程。
  - 子脚本仍在本地生成 $HOME/$ENV_NAME/，随后 rsync 到远端。
  - 部署 Bybit/Binance 跨所合约 cross 环境：
      open=bybit-futures
      hedge=binance-futures (STANDARD 模式，由 deploy_setup_env_cross.sh 默认设置)
  - 环境目录固定: $HOME/bybit-binance-cross-<suffix>
  - 仅部署，不启动任何进程
  - 支持 suffix: trade、arb01、arb02、arb03
  - 固定端口（bybit-binance cross config，按 18200+open_rank*10+hedge_rank=3*10+1=31）:
      trade -> 18231
      arb01 -> 19231
      arb02 -> 19232
      arb03 -> 19233
  - viz_server 端口沿用 deploy_cross_viz_server.sh 的 cross 后缀映射:
      trade -> 10211
      arb01 -> 10251
      arb02 -> 10252
      arb03 -> 10253
  - --bin: 跳过 env/config_server/nginx，仅更新主要进程部署产物
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
  echo "[ERROR] 需要传入 env suffix（trade|arb01|arb02|arb03）" >&2
  usage; exit 1
fi

ENV_SUFFIX="$(echo "$ENV_SUFFIX" | tr 'A-Z' 'a-z')"
case "$ENV_SUFFIX" in
  trade) CONFIG_PORT="18231" ;;
  arb01) CONFIG_PORT="19231" ;;
  arb02) CONFIG_PORT="19232" ;;
  arb03) CONFIG_PORT="19233" ;;
  *)
    echo "[ERROR] 仅支持 suffix: trade|arb01|arb02|arb03（收到: ${ENV_SUFFIX}）" >&2
    exit 1 ;;
esac

OPEN_VENUE="bybit-futures"
HEDGE_VENUE="binance-futures"
CROSS_ENV_SUFFIX="cross-${ENV_SUFFIX}"
ENV_NAME="bybit-binance-${CROSS_ENV_SUFFIX}"

if [[ "$BIN_MODE" == "1" && ! -d "$HOME/$ENV_NAME" ]]; then
  echo "[ERROR] --bin 模式要求本地环境目录已存在: $HOME/$ENV_NAME" >&2
  exit 1
fi

fr_remote_init "$ROOT_DIR" "$ENV_NAME"
if [[ "$BIN_MODE" != "1" ]]; then
  fr_remote_fetch_nginx_mapping "$ROOT_DIR"
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

echo "[INFO] Bybit/Binance cross deploy-only (target: ${FR_DEPLOY_HOST})"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] open=${OPEN_VENUE}, hedge=${HEDGE_VENUE} (binance STANDARD)"
echo "[INFO] config_port=${CONFIG_PORT}"
echo "[INFO] 不会执行 start 命令"
[[ "$BIN_MODE" == "1" ]] && echo "[INFO] mode=bin"

cd "$ROOT_DIR"

if [[ "$BIN_MODE" != "1" ]]; then
  run_deploy bash scripts/deploy_setup_env_cross.sh \
    --env-name "$ENV_NAME" \
    --env-suffix "$CROSS_ENV_SUFFIX" \
    --open-venue "$OPEN_VENUE" \
    --hedge-venue "$HEDGE_VENUE"

  run_deploy bash scripts/deploy_cross_config_server.sh \
    --env-name "$ENV_NAME" \
    --open-venue "$OPEN_VENUE" \
    --hedge-venue "$HEDGE_VENUE" \
    --port "$CONFIG_PORT" \
    --nginx-mapping-file "$FR_NGINX_STAGING"
fi

run_deploy bash scripts/deploy_cross_monitors.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CROSS_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE"

run_deploy bash scripts/deploy_cross_trade_engine.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CROSS_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE"

run_deploy bash scripts/deploy_cross_viz_server.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CROSS_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE" \
  --nginx-mapping-file "$FR_NGINX_STAGING"

run_deploy bash scripts/deploy_cross_persist_manager.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CROSS_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE"

if [[ "$BIN_MODE" == "1" ]]; then
  PRE_TRADE_SCRIPT_MODE=(--bin-only)
  TRADE_SIGNAL_SCRIPT_MODE=(--bin-only)
else
  PRE_TRADE_SCRIPT_MODE=(--sync-scripts)
  TRADE_SIGNAL_SCRIPT_MODE=(--sync-scripts)
fi

run_deploy bash scripts/deploy_cross_pre_trade.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CROSS_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE" \
  "${PRE_TRADE_SCRIPT_MODE[@]}"

run_deploy bash scripts/deploy_cross_trade_signal.sh \
  --env-name "$ENV_NAME" \
  --env-suffix "$CROSS_ENV_SUFFIX" \
  --open-venue "$OPEN_VENUE" \
  --hedge-venue "$HEDGE_VENUE" \
  "${TRADE_SIGNAL_SCRIPT_MODE[@]}"

if [[ "$BIN_MODE" == "1" ]]; then
  fr_remote_sync_binaries "$ENV_NAME"
else
  fr_remote_sync_env_dir "$ENV_NAME"
  fr_remote_apply_nginx "$ENV_NAME"
fi

echo "[INFO] Bybit/Binance cross 部署完成（远端 ${FR_DEPLOY_HOST}，未启动进程）"
echo "[INFO] 远端环境目录: ${FR_REMOTE_HOME}/${ENV_NAME}"
echo "[INFO] 注意: env.sh 不参与 rsync——首次部署需要手动在远端写入"
echo "[INFO]   BYBIT_API_KEY / BYBIT_API_SECRET"
echo "[INFO]   BINANCE_API_KEY / BINANCE_API_SECRET   (account mode 已固定 STANDARD)"
