#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_engine"
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
mkdir -p "$TARGET_DIR/config"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

# 部署配置文件
echo "[INFO] 部署配置文件到 $TARGET_DIR/config"
cp "$ROOT_DIR/config/trade_engine.toml" "$TARGET_DIR/config/"

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
