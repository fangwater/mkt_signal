#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法: scripts/deploy_fr_config_server.sh --exchange <exchange> [--env-suffix fr_trade] [--target <path>]

说明:
  - 通过 --exchange 指定交易所（必须），脚本会同步 fr_config_server 相关脚本
  - 目标目录默认: $HOME/<exchange>_<env-suffix>/scripts（可用 --target 自定义根目录）
  - 不自动启动，可在目标目录执行 ./scripts/start_fr_config_server.sh

示例:
  scripts/deploy_fr_config_server.sh --exchange okex               # 部署到 ~/okex_fr_trade
  scripts/deploy_fr_config_server.sh --exchange binance            # 部署到 ~/binance_fr_trade
  scripts/deploy_fr_config_server.sh --exchange okex --target /opt/fr  # 自定义路径
EOF
}

EXCHANGE=""
ENV_SUFFIX="fr_trade"
TARGET_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-$EXCHANGE}"
      shift 2
      ;;
    --env-suffix)
      ENV_SUFFIX="${2:-$ENV_SUFFIX}"
      shift 2
      ;;
    --target)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 需要使用 --exchange 指定交易所（例如 okex / binance / bybit ...）"
  usage
  exit 1
fi

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME/${EXCHANGE}_${ENV_SUFFIX}"
fi

DEST_SCRIPT_DIR="$TARGET_DIR/scripts"
mkdir -p "$DEST_SCRIPT_DIR"

FILES=(
  "scripts/fr_config_server.py"
  "scripts/start_fr_config_server.sh"
  "scripts/stop_fr_config_server.sh"
)

for file in "${FILES[@]}"; do
  src="$ROOT_DIR/$file"
  if [[ ! -f "$src" ]]; then
    echo "[WARN] 跳过缺失文件: $src"
    continue
  fi
  rsync -a "$src" "$DEST_SCRIPT_DIR/"
done

for path in "$DEST_SCRIPT_DIR"/fr_config_server.py "$DEST_SCRIPT_DIR"/start_fr_config_server.sh "$DEST_SCRIPT_DIR"/stop_fr_config_server.sh; do
  [[ -f "$path" ]] && chmod +x "$path"
done

echo "[INFO] 已部署 fr_config_server 脚本到 $DEST_SCRIPT_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && ./scripts/start_fr_config_server.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./scripts/stop_fr_config_server.sh"
