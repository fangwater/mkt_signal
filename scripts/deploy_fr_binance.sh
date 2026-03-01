#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_fr_binance.sh --env-suffix <suffix>
  scripts/deploy_fr_binance.sh <suffix>

说明:
  - 只部署，不启动任何进程。
  - 只支持固定 suffix：trade、hf01、hf02。
  - 端口按 suffix 明文写死，不允许外部覆盖。
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix)
      ENV_SUFFIX="${2:-}"
      shift 2
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
  echo "[ERROR] 需要传入 env suffix（trade|hf01|hf02）" >&2
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
  *)
    echo "[ERROR] binance FR 只支持固定 suffix: trade|hf01|hf02（收到: ${ENV_SUFFIX}）" >&2
    exit 1
    ;;
esac

ENV_NAME="binance_fr_${ENV_SUFFIX}"

echo "[INFO] Binance FR deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] config_port=${CONFIG_PORT}, viz_port=${VIZ_PORT}"
echo "[INFO] 不会执行 start 命令"

run_deploy() {
  local cmd=("$@")
  echo "[RUN] ${cmd[*]}"
  "${cmd[@]}"
}

cd "$ROOT_DIR"

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

echo "[INFO] Binance FR 部署完成（仅 deploy，不含 start）"
