#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="pre_trade"
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
      echo "用法: $0 [trade|test] [--exchange binance|okex]"
      exit 1
      ;;
  esac
done

# 规范化为小写
EXCHANGE="$(echo "$EXCHANGE" | tr 'A-Z' 'a-z')"
case "$EXCHANGE" in
  binance|okex)
    ;;
  *)
    echo "[ERROR] 不支持的 exchange: $EXCHANGE (目前仅支持 binance/okex)"
    exit 1
    ;;
esac

TARGET_DIR="$HOME/${EXCHANGE}_fr_${ENV_TYPE}"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=(
  "sync_fr_risk_params.py"
  "print_fr_risk_params.py"
)
mkdir -p "$TARGET_DIR/scripts"
for script in "${SCRIPTS_TO_SYNC[@]}"; do
  if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
    rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 风控参数同步: cd $TARGET_DIR && ./scripts/sync_fr_risk_params.py  # 会按目录推断 open/hedge"
