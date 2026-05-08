#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/fr_remote_deploy.sh
source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_fr_binance.sh --env-suffix <suffix> [--bin]
  scripts/deploy_fr_binance.sh <suffix>

说明:
  - 部署到远端 ${FR_DEPLOY_HOST:-ubuntu@54.64.147.69}（不启动进程）。
  - 子脚本仍在本地生成 $HOME/$ENV_NAME/，随后 rsync 到远端。
  - 支持三组固定 suffix：
      1) 老组：trade、hf01、hf02
      2) 新组：trade01、trade02、trade03（独立环境）
      3) arb 组：arb01..arb05（独立环境，2xxxx 端口段）
  - 端口按 suffix 明文写死，不允许外部覆盖：
      trade   -> CONFIG 18031 / VIZ 10031
      hf01    -> CONFIG 18041 / VIZ 10041
      hf02    -> CONFIG 18042 / VIZ 10042
      trade01 -> CONFIG 18051 / VIZ 10051
      trade02 -> CONFIG 18052 / VIZ 10052
      trade03 -> CONFIG 18053 / VIZ 10053
      arb01   -> CONFIG 20031 / VIZ 20131
      arb02   -> CONFIG 20032 / VIZ 20132
      arb03   -> CONFIG 20033 / VIZ 20133
      arb04   -> CONFIG 20034 / VIZ 20134
      arb05   -> CONFIG 20035 / VIZ 20135
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
  echo "[ERROR] 需要传入 env suffix（trade|hf01|hf02|trade01|trade02|trade03|arb01|arb02|arb03|arb04|arb05）" >&2
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
  arb01)
    CONFIG_PORT="20031"
    VIZ_PORT="20131"
    ;;
  arb02)
    CONFIG_PORT="20032"
    VIZ_PORT="20132"
    ;;
  arb03)
    CONFIG_PORT="20033"
    VIZ_PORT="20133"
    ;;
  arb04)
    CONFIG_PORT="20034"
    VIZ_PORT="20134"
    ;;
  arb05)
    CONFIG_PORT="20035"
    VIZ_PORT="20135"
    ;;
  *)
    echo "[ERROR] binance FR 仅支持 suffix: trade|hf01|hf02|trade01|trade02|trade03|arb01|arb02|arb03|arb04|arb05（收到: ${ENV_SUFFIX}）" >&2
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
    --nginx-mapping-file "$FR_NGINX_STAGING"

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
    --nginx-mapping-file "$FR_NGINX_STAGING"

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

if [[ "$BIN_MODE" == "1" ]]; then
  fr_remote_sync_binaries "$ENV_NAME"
else
  fr_remote_sync_env_dir "$ENV_NAME"
  fr_remote_apply_nginx "$ENV_NAME"
fi

echo "[INFO] Binance FR 部署完成（远端 ${FR_DEPLOY_HOST}，未启动进程）"
