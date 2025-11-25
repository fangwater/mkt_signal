#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="rolling_metrics"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

# 解析参数
ENV_TYPE="${1:-trade}"
case "$ENV_TYPE" in
    trade)
        TARGET_DIR="$HOME/fr_trade"
        ;;
    test)
        TARGET_DIR="$HOME/fr_test"
        ;;
    *)
        echo "[ERROR] 未知环境类型: $ENV_TYPE"
        echo "用法: $0 [trade|test]"
        exit 1
        ;;
esac

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

EXTRA_FILES=(
  "scripts/print_rolling_metrics_thresholds.py"
  "scripts/sync_rolling_metrics_params.py"
  "docs/rolling_metrics.md"
)

for file in "${EXTRA_FILES[@]}"; do
  SRC_PATH="$ROOT_DIR/$file"
  if [[ -f "$SRC_PATH" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC_PATH" "$DEST_DIR/"
  fi
done

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
