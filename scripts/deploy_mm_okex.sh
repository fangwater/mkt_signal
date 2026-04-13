#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_mm_okex.sh --env-suffix <suffix> [--bin|--runtime-only]
  scripts/deploy_mm_okex.sh <suffix>

说明:
  - 只部署，不启动任何进程。
  - 目前支持固定 suffix:
      1) beta: 使用基础端口
      2) alpha: 在基础端口上 +1
  - 固定端口（okex mm）:
      config_server = 18111
      viz_server = 10211
      trade_signal = deploy only, no dedicated HTTP port
    alpha 环境分别使用 18112 / 10212。
  - --bin: 仅替换二进制（不改脚本/配置/nginx/env.sh）。
  - --runtime-only: 仅替换二进制和脚本（不改配置/nginx/env.sh）。
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX=""
BIN_MODE="0"
RUNTIME_ONLY="0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix)
      ENV_SUFFIX="${2:-}"
      shift 2
      ;;
    --bin)
      if [[ "$RUNTIME_ONLY" == "1" ]]; then
        echo "[ERROR] --bin 与 --runtime-only 互斥" >&2
        exit 1
      fi
      BIN_MODE="1"
      shift
      ;;
    --runtime-only)
      if [[ "$BIN_MODE" == "1" ]]; then
        echo "[ERROR] --bin 与 --runtime-only 互斥" >&2
        exit 1
      fi
      RUNTIME_ONLY="1"
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
  echo "[ERROR] 需要传入 env suffix（alpha|beta）" >&2
  usage
  exit 1
fi

ENV_SUFFIX="$(echo "$ENV_SUFFIX" | tr 'A-Z' 'a-z')"

case "$ENV_SUFFIX" in
  beta)
    CONFIG_PORT="18111"
    VIZ_PORT="10211"
    ;;
  alpha)
    CONFIG_PORT="18112"
    VIZ_PORT="10212"
    ;;
  *)
    echo "[ERROR] okex MM 仅支持 suffix: alpha|beta（收到: ${ENV_SUFFIX}）" >&2
    exit 1
    ;;
esac

ENV_NAME="okex_mm_${ENV_SUFFIX}"

echo "[INFO] OKEX MM deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] config_port=${CONFIG_PORT}, viz_port=${VIZ_PORT}"
echo "[INFO] 不会执行 start 命令"
if [[ "$BIN_MODE" == "1" ]]; then
  echo "[INFO] mode=bin (仅替换二进制)"
elif [[ "$RUNTIME_ONLY" == "1" ]]; then
  echo "[INFO] mode=runtime-only (仅替换二进制和脚本)"
fi

if [[ ( "$BIN_MODE" == "1" || "$RUNTIME_ONLY" == "1" ) && ! -d "$HOME/$ENV_NAME" ]]; then
  echo "[ERROR] 仅替换模式要求环境目录已存在: $HOME/$ENV_NAME" >&2
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
  run_deploy bash scripts/deploy_mm_account_monitor.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX" \
    --bin-only

  run_deploy bash scripts/deploy_mm_trade_engine.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX" \
    --bin-only

  run_deploy bash scripts/deploy_mm_signal.sh \
    --exchange okex \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_mm_viz_server.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX" \
    --port "$VIZ_PORT" \
    --bin-only

  run_deploy bash scripts/deploy_mm_persist_manager.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX" \
    --bin-only

  run_deploy bash scripts/deploy_mm_pre_trade.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX" \
    --bin-only
elif [[ "$RUNTIME_ONLY" == "1" ]]; then
  run_deploy bash scripts/deploy_mm_config_server.sh \
    --env-name "$ENV_NAME" \
    --exchange okex \
    --scripts-only

  run_deploy bash scripts/deploy_mm_account_monitor.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_trade_engine.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX" \
    --runtime-only

  run_deploy bash scripts/deploy_mm_signal.sh \
    --exchange okex \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_mm_viz_server.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX" \
    --port "$VIZ_PORT" \
    --runtime-only

  run_deploy bash scripts/deploy_mm_persist_manager.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_pre_trade.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX"
else
  run_deploy bash scripts/deploy_mm_config_server.sh \
    --env-name "$ENV_NAME" \
    --exchange okex \
    --port "$CONFIG_PORT" \
    --apply-nginx

  run_deploy bash scripts/deploy_mm_account_monitor.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_trade_engine.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_signal.sh \
    --exchange okex \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_mm_viz_server.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX" \
    --port "$VIZ_PORT" \
    --apply-nginx

  run_deploy bash scripts/deploy_mm_persist_manager.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_pre_trade.sh \
    --exchange okex \
    --env-suffix "$ENV_SUFFIX"
fi

echo "[INFO] OKEX MM 部署完成（仅 deploy，不含 start）"
