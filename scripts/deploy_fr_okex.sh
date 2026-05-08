#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_fr_okex.sh --env-suffix <suffix> [--bin]
  scripts/deploy_fr_okex.sh <suffix>

说明:
  - 只部署，不启动任何进程。
  - 支持 suffix: trade、arb01、arb02、arb03、arb04、arb05。
  - 端口按 suffix 明文写死，不允许外部传入覆盖：
      trade -> CONFIG 18011 / VIZ 10011
      arb01 -> CONFIG 20011 / VIZ 20111
      arb02 -> CONFIG 20012 / VIZ 20112
      arb03 -> CONFIG 20013 / VIZ 20113
      arb04 -> CONFIG 20014 / VIZ 20114
      arb05 -> CONFIG 20015 / VIZ 20115
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
  echo "[ERROR] 需要传入 env suffix（例如 trade）" >&2
  usage
  exit 1
fi

ENV_SUFFIX="$(echo "$ENV_SUFFIX" | tr 'A-Z' 'a-z')"

case "$ENV_SUFFIX" in
  trade)
    CONFIG_PORT="18011"
    VIZ_PORT="10011"
    ;;
  arb01)
    CONFIG_PORT="20011"
    VIZ_PORT="20111"
    ;;
  arb02)
    CONFIG_PORT="20012"
    VIZ_PORT="20112"
    ;;
  arb03)
    CONFIG_PORT="20013"
    VIZ_PORT="20113"
    ;;
  arb04)
    CONFIG_PORT="20014"
    VIZ_PORT="20114"
    ;;
  arb05)
    CONFIG_PORT="20015"
    VIZ_PORT="20115"
    ;;
  *)
    echo "[ERROR] okex FR 仅支持 suffix: trade|arb01|arb02|arb03|arb04|arb05（收到: ${ENV_SUFFIX}）" >&2
    exit 1
    ;;
esac

ENV_NAME="okex_fr_${ENV_SUFFIX}"
TARGET_DIR="$HOME/$ENV_NAME"

echo "[INFO] OKEx FR deploy-only"
echo "[INFO] env_name=${ENV_NAME}"
echo "[INFO] config_port=${CONFIG_PORT}, viz_port=${VIZ_PORT}"
echo "[INFO] 不会执行 start 命令"
if [[ "$BIN_MODE" == "1" ]]; then
  echo "[INFO] mode=bin (仅替换二进制)"
fi

if [[ "$BIN_MODE" == "1" && ! -d "$TARGET_DIR" ]]; then
  echo "[ERROR] --bin 模式要求环境目录已存在: $TARGET_DIR" >&2
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

sync_okx_rate_cache_assets() {
  local src_server="$ROOT_DIR/scripts/okx_rate_cache_server.py"
  local src_start="$ROOT_DIR/scripts/start_okx_rate_cache.sh"
  local src_stop="$ROOT_DIR/scripts/stop_okx_rate_cache.sh"
  local dst_dir="$TARGET_DIR/scripts"

  mkdir -p "$dst_dir"

  if [[ ! -f "$src_server" ]]; then
    echo "[WARN] 未找到 OKX loan rate cache 脚本: $src_server"
  else
    rsync -a "$src_server" "$dst_dir/"
    chmod +x "$dst_dir/okx_rate_cache_server.py"
    echo "[INFO] 已同步 okx_rate_cache_server.py 到 $dst_dir/"
  fi

  if [[ ! -f "$src_start" || ! -f "$src_stop" ]]; then
    echo "[WARN] 未找到固定端口 start/stop 脚本（start_okx_rate_cache.sh / stop_okx_rate_cache.sh）"
  else
    rsync -a "$src_start" "$dst_dir/"
    rsync -a "$src_stop" "$dst_dir/"
    chmod +x "$dst_dir/start_okx_rate_cache.sh" "$dst_dir/stop_okx_rate_cache.sh"
    echo "[INFO] 已同步固定端口脚本到 $dst_dir/ (127.0.0.1:28901)"
  fi
}

cd "$ROOT_DIR"

if [[ "$BIN_MODE" == "1" ]]; then
  run_deploy bash scripts/deploy_account_monitor.sh \
    --exchange okex \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_fr_trade_engine.sh \
    --exchange okex \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_fr_viz_server.sh \
    --env-name "$ENV_NAME" \
    --exchange okex \
    --port "$VIZ_PORT" \
    --bin-only

  run_deploy bash scripts/deploy_fr_persist_manager.sh \
    --exchange okex \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_fr_pre_trade.sh \
    --exchange okex \
    --env-name "$ENV_NAME" \
    --bin-only

  run_deploy bash scripts/deploy_fr_signal.sh \
    --exchange okex \
    --env-name "$ENV_NAME" \
    --bin-only
else
  run_deploy bash scripts/deploy_fr_config_server.sh \
    --env-name "$ENV_NAME" \
    --exchange okex \
    --port "$CONFIG_PORT" \
    --apply-nginx

  run_deploy bash scripts/deploy_account_monitor.sh \
    --exchange okex \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_fr_trade_engine.sh \
    --exchange okex \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_fr_viz_server.sh \
    --env-name "$ENV_NAME" \
    --exchange okex \
    --port "$VIZ_PORT" \
    --apply-nginx

  run_deploy bash scripts/deploy_fr_persist_manager.sh \
    --exchange okex \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_fr_pre_trade.sh \
    --exchange okex \
    --env-name "$ENV_NAME"

  run_deploy bash scripts/deploy_fr_signal.sh \
    --exchange okex \
    --env-name "$ENV_NAME"

  sync_okx_rate_cache_assets
fi

echo "[INFO] OKEx FR 部署完成（仅 deploy，不含 start）"
echo "[INFO] 请确保 $TARGET_DIR/env.sh 含以下变量:"
echo "[INFO]   IPC_NAMESPACE=${ENV_NAME}"
echo "[INFO]   OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE"
echo "[INFO]   OKEX_LOAN_RATE_URL=http://127.0.0.1:28901/rates"
echo "[INFO] loan rate cache 固定端口启动命令："
echo "[INFO]   cd $TARGET_DIR && ./scripts/start_okx_rate_cache.sh"
echo "[INFO] loan rate cache 停止命令："
echo "[INFO]   cd $TARGET_DIR && ./scripts/stop_okx_rate_cache.sh"
