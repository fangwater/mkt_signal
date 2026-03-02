#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_fr_binance.sh --env-suffix <suffix> [--bin]
  scripts/deploy_fr_binance.sh <suffix>

说明:
  - 只部署，不启动任何进程。
  - 支持两组固定 suffix：
      1) 老组：trade、hf01、hf02
      2) 新组：trade01、trade02、trade03（独立环境）
  - 端口按 suffix 明文写死，不允许外部覆盖。
  - --bin: 仅替换二进制（不改脚本/配置/nginx）。
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
  echo "[ERROR] 需要传入 env suffix（trade|hf01|hf02|trade01|trade02|trade03）" >&2
  usage
  exit 1
fi

ENV_SUFFIX="$(echo "$ENV_SUFFIX" | tr 'A-Z' 'a-z')"

case "$ENV_SUFFIX" in
  trade)
    CONFIG_PORT="18031"
    VIZ_PORT="10031"
    ;;
  hf01)
    CONFIG_PORT="18041"
    VIZ_PORT="10041"
    ;;
  hf02)
    CONFIG_PORT="18042"
    VIZ_PORT="10042"
    ;;
  trade01)
    CONFIG_PORT="18051"
    VIZ_PORT="10051"
    ;;
  trade02)
    CONFIG_PORT="18052"
    VIZ_PORT="10052"
    ;;
  trade03)
    CONFIG_PORT="18053"
    VIZ_PORT="10053"
    ;;
  *)
    echo "[ERROR] binance FR 仅支持 suffix: trade|hf01|hf02|trade01|trade02|trade03（收到: ${ENV_SUFFIX}）" >&2
    exit 1
    ;;
esac

ENV_NAME="binance_fr_${ENV_SUFFIX}"

echo "[INFO] Binance FR deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] config_port=${CONFIG_PORT}, viz_port=${VIZ_PORT}"
echo "[INFO] 不会执行 start 命令"
if [[ "$BIN_MODE" == "1" ]]; then
  echo "[INFO] mode=bin (仅替换二进制)"
fi

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

cd "$ROOT_DIR"

if [[ "$BIN_MODE" == "1" ]]; then
  run_deploy bash scripts/deploy_account_monitor.sh \
    --exchange binance \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_fr_trade_engine.sh \
    --exchange binance \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_fr_viz_server.sh \
    --env-name "$ENV_NAME" \
    --exchange binance \
    --port "$VIZ_PORT" \
    --bin-only

  run_deploy bash scripts/deploy_fr_persist_manager.sh \
    --exchange binance \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_fr_pre_trade.sh \
    --exchange binance \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_fr_signal.sh \
    --exchange binance \
    --env-name "$ENV_NAME" \
    --bin-only
else
  run_deploy bash scripts/deploy_fr_config_server.sh \
    --env-name "$ENV_NAME" \
    --exchange binance \
    --port "$CONFIG_PORT" \
    --apply-nginx

  run_deploy bash scripts/deploy_account_monitor.sh \
    --exchange binance \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_fr_trade_engine.sh \
    --exchange binance \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_fr_viz_server.sh \
    --env-name "$ENV_NAME" \
    --exchange binance \
    --port "$VIZ_PORT" \
    --apply-nginx

  run_deploy bash scripts/deploy_fr_persist_manager.sh \
    --exchange binance \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_fr_pre_trade.sh \
    --exchange binance \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_fr_signal.sh \
    --exchange binance \
    --env-name "$ENV_NAME"
fi

echo "[INFO] Binance FR 部署完成（仅 deploy，不含 start）"
