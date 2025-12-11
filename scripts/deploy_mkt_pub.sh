#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="mkt_pub"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

# 参数解析
ENV_TYPE="trade"
EXCHANGE="binance"
while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      ENV_TYPE="$1"
      shift
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange 需要一个值"
        exit 1
      fi
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      echo "用法: $0 [trade|test] [--exchange binance]"
      exit 1
      ;;
  esac
done

TARGET_DIR="$HOME/${EXCHANGE}_fr_${ENV_TYPE}"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

# 同步启动/停止脚本到 scripts/
SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=("start_mkt_pub.sh" "stop_mkt_pub.sh")
mkdir -p "$TARGET_DIR/scripts"
for script in "${SCRIPTS_TO_SYNC[@]}"; do
  if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
    rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

# 仅同步 mkt_cfg.yaml
mkdir -p "$TARGET_DIR/config"
if [[ -f "$ROOT_DIR/config/mkt_cfg.yaml" ]]; then
  rsync -a "$ROOT_DIR/config/mkt_cfg.yaml" "$TARGET_DIR/config/"
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./scripts/start_mkt_pub.sh"
