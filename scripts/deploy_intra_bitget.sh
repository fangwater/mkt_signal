#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_intra_bitget.sh --env-suffix <suffix> [--bin]
  scripts/deploy_intra_bitget.sh <suffix>

说明:
  - 部署 Bitget 同所期现 intra 环境：
      open=bitget-margin
      hedge=bitget-futures
  - 环境目录固定: $HOME/bitget-intra-<suffix>
  - 仅部署，不启动任何进程
  - 支持 suffix: trade、arb01、arb02、arb03
  - 固定端口（bitget intra）:
      trade -> 18134
      arb01 -> 19211
      arb02 -> 19212
      arb03 -> 19213
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
  trade) CONFIG_PORT="18134" ;;
  arb01) CONFIG_PORT="19211" ;;
  arb02) CONFIG_PORT="19212" ;;
  arb03) CONFIG_PORT="19213" ;;
  *)
    echo "[ERROR] 仅支持 suffix: trade|arb01|arb02|arb03（收到: ${ENV_SUFFIX}）" >&2
    exit 1 ;;
esac

EXCHANGE="bitget"
ENV_NAME="${EXCHANGE}-intra-${ENV_SUFFIX}"
INTRA_ENV_SUFFIX="intra-${ENV_SUFFIX}"

if [[ "$BIN_MODE" == "1" && ! -d "$HOME/$ENV_NAME" ]]; then
  echo "[ERROR] --bin 模式要求环境目录已存在: $HOME/$ENV_NAME" >&2
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

echo "[INFO] Bitget intra deploy-only"
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

  run_deploy bash scripts/deploy_intra_config_server.sh \
    --env-name "$ENV_NAME" \
    --exchange "$EXCHANGE" \
    --port "$CONFIG_PORT" \
    --apply-nginx
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
  --exchange "$EXCHANGE" \
  --apply-nginx

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

echo "[INFO] Bitget intra 部署完成（仅 deploy，不含 start）"
echo "[INFO] 环境目录: $HOME/$ENV_NAME"
