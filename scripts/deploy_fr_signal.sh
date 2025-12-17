#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_signal"
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

# 同步启动/停止脚本及相关工具
SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=(
  "start_trade_signal.sh"
  "stop_trade_signal.sh"
  "start_fr_signal.sh"
  "stop_fr_signal.sh"
  "sync_funding_rate_thresholds.py"
  "print_funding_rate_thresholds.py"
  "sync_fr_strategy_params.py"
  "print_fr_strategy_params.py"
  "sync_fr_symbol_lists.py"
  "print_fr_symbol_lists.py"
  "sync_fr_spread_thresholds.py"
  "print_spread_thresholds.py"
)
mkdir -p "$TARGET_DIR/scripts"
for script in "${SCRIPTS_TO_SYNC[@]}"; do
  if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
    rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./scripts/start_trade_signal.sh"
