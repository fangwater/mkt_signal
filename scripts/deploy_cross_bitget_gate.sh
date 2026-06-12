#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_cross_bitget_gate.sh --env-suffix <suffix> [--bin]
  scripts/deploy_cross_bitget_gate.sh <suffix>

说明:
  - 默认只部署到本机 $HOME/$ENV_NAME/（不启动进程）。
  - 部署 Bitget/Gate 跨所合约 cross 环境：
      open=bitget-futures
      hedge=gate-futures
  - 环境目录固定: $HOME/bitget-gate-cross-<suffix>
  - 仅部署，不启动任何进程
  - 支持 suffix: trade、arb01、arb02、arb03
  - 固定端口（bitget-gate cross config）:
      trade -> 18245
      arb01 -> 19245
      arb02 -> 19246
      arb03 -> 19247
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
    --remote)
      echo "[ERROR] --remote 已移除；bitget-gate cross 入口现在只部署本机" >&2
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
  echo "[ERROR] 需要传入 env suffix（trade|arb01|arb02|arb03）" >&2
  usage; exit 1
fi

ENV_SUFFIX="$(echo "$ENV_SUFFIX" | tr 'A-Z' 'a-z')"
case "$ENV_SUFFIX" in
  trade) CONFIG_PORT="18245" ;;
  arb01) CONFIG_PORT="19245" ;;
  arb02) CONFIG_PORT="19246" ;;
  arb03) CONFIG_PORT="19247" ;;
  *)
    echo "[ERROR] 仅支持 suffix: trade|arb01|arb02|arb03（收到: ${ENV_SUFFIX}）" >&2
    exit 1 ;;
esac

OPEN_VENUE="bitget-futures"
HEDGE_VENUE="gate-futures"
CROSS_ENV_SUFFIX="cross-${ENV_SUFFIX}"
ENV_NAME="bitget-gate-${CROSS_ENV_SUFFIX}"
TARGET_DIR="$HOME/$ENV_NAME"

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

echo "[INFO] Bitget/Gate cross deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] open=${OPEN_VENUE}, hedge=${HEDGE_VENUE}"
echo "[INFO] config_port=${CONFIG_PORT}"
echo "[INFO] 不会执行 start 命令"
[[ "$BIN_MODE" == "1" ]] && echo "[INFO] mode=bin"
echo "[INFO] target=local ${TARGET_DIR}"

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
    --port "$CONFIG_PORT"
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
  --hedge-venue "$HEDGE_VENUE"

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

echo "[INFO] Bitget/Gate cross 部署完成（未启动进程）"
echo "[INFO] 本机环境目录: ${TARGET_DIR}"
