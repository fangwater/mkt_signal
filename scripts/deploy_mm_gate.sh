#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=lib/fr_remote_deploy.sh
source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_mm_gate.sh --env-suffix <suffix> [--bin|--runtime-only]
  scripts/deploy_mm_gate.sh <suffix>

说明:
  - 部署到远端 ${FR_DEPLOY_HOST:-ubuntu@54.64.147.69}（不启动进程）。
  - 子脚本仍在本地生成 $HOME/$ENV_NAME/，随后 rsync 到远端。
  - 目前支持固定 suffix:
      1) beta: 使用基础端口
      2) alpha: 在基础端口上 +1
  - 固定端口（gate mm）:
      config_server = 18121
      viz_server = 10221
      trade_signal = deploy only, no dedicated HTTP port
    alpha 环境分别使用 18122 / 10222。
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
    CONFIG_PORT="18121"
    VIZ_PORT="10221"
    ;;
  alpha)
    CONFIG_PORT="18122"
    VIZ_PORT="10222"
    ;;
  *)
    echo "[ERROR] gate MM 仅支持 suffix: alpha|beta（收到: ${ENV_SUFFIX}）" >&2
    exit 1
    ;;
esac

ENV_NAME="gate_mm_${ENV_SUFFIX}"

echo "[INFO] GATE MM deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] config_port=${CONFIG_PORT}, viz_port=${VIZ_PORT}"
echo "[INFO] 不会执行 start 命令"
if [[ "$BIN_MODE" == "1" ]]; then
  echo "[INFO] mode=bin (仅替换二进制)"
elif [[ "$RUNTIME_ONLY" == "1" ]]; then
  echo "[INFO] mode=runtime-only (仅替换二进制和脚本)"
fi

if [[ ( "$BIN_MODE" == "1" || "$RUNTIME_ONLY" == "1" ) && ! -d "$HOME/$ENV_NAME" ]]; then
  echo "[ERROR] 仅替换模式要求本地环境目录已存在: $HOME/$ENV_NAME" >&2
  exit 1
fi

fr_remote_init "$ROOT_DIR" "$ENV_NAME"
if [[ "$BIN_MODE" != "1" && "$RUNTIME_ONLY" != "1" ]]; then
  fr_remote_fetch_nginx_mapping "$ROOT_DIR"
fi

TARGET_DIR="$HOME/$ENV_NAME"

write_env_template_if_missing() {
  local target_dir="$1"
  local env_name="$2"
  local env_file="${target_dir}/env.sh"
  if [[ -f "$env_file" ]]; then
    echo "[INFO] env template exists, keep as-is: $env_file"
    return 0
  fi

  cat >"$env_file" <<EOF
#!/usr/bin/env bash

export IPC_NAMESPACE="${env_name}"

export GATE_API_KEY=""
export GATE_API_SECRET=""

export RUST_LOG="info"
EOF
  chmod 600 "$env_file"
  echo "[INFO] wrote env template: $env_file"
}

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
    --exchange gate \
    --env-suffix "$ENV_SUFFIX" \
    --bin-only

  run_deploy bash scripts/deploy_mm_trade_engine.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX" \
    --bin-only

  run_deploy bash scripts/deploy_mm_signal.sh \
    --exchange gate \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_mm_viz_server.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX" \
    --port "$VIZ_PORT" \
    --bin-only

  run_deploy bash scripts/deploy_mm_persist_manager.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX" \
    --bin-only

  run_deploy bash scripts/deploy_mm_pre_trade.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX" \
    --bin-only
elif [[ "$RUNTIME_ONLY" == "1" ]]; then
  run_deploy bash scripts/deploy_mm_config_server.sh \
    --env-name "$ENV_NAME" \
    --exchange gate \
    --scripts-only

  run_deploy bash scripts/deploy_mm_account_monitor.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_trade_engine.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX" \
    --runtime-only

  run_deploy bash scripts/deploy_mm_signal.sh \
    --exchange gate \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_mm_viz_server.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX" \
    --port "$VIZ_PORT" \
    --runtime-only

  run_deploy bash scripts/deploy_mm_persist_manager.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_pre_trade.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX"
else
  run_deploy bash scripts/deploy_mm_config_server.sh \
    --env-name "$ENV_NAME" \
    --exchange gate \
    --port "$CONFIG_PORT" \
    --nginx-mapping-file "$FR_NGINX_STAGING" \
    --skip-nginx-apply

  run_deploy bash scripts/deploy_mm_account_monitor.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_trade_engine.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_signal.sh \
    --exchange gate \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_mm_viz_server.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX" \
    --port "$VIZ_PORT" \
    --nginx-mapping-file "$FR_NGINX_STAGING"

  run_deploy bash scripts/deploy_mm_persist_manager.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX"

  run_deploy bash scripts/deploy_mm_pre_trade.sh \
    --exchange gate \
    --env-suffix "$ENV_SUFFIX"
fi

if [[ "$BIN_MODE" != "1" && "$RUNTIME_ONLY" != "1" ]]; then
  write_env_template_if_missing "$TARGET_DIR" "$ENV_NAME"
fi

if [[ "$BIN_MODE" == "1" ]]; then
  fr_remote_sync_binaries "$ENV_NAME"
elif [[ "$RUNTIME_ONLY" == "1" ]]; then
  fr_remote_sync_env_dir "$ENV_NAME"
else
  fr_remote_sync_env_dir "$ENV_NAME"
  fr_remote_apply_nginx "$ENV_NAME"
fi

echo "[INFO] GATE MM 部署完成（远端 ${FR_DEPLOY_HOST}，未启动进程）"
echo "[INFO] 远端环境目录: ${FR_REMOTE_HOME}/${ENV_NAME}"
if [[ "$BIN_MODE" != "1" && "$RUNTIME_ONLY" != "1" ]]; then
  echo "[INFO] 注意: env.sh 不参与 rsync——首次部署需要手动在远端写入 GATE_API_KEY/SECRET"
fi
