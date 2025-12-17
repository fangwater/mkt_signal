#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="manual_signal"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_fr_manual_signal.sh [trade|test] --exchange <binance|okex|bybit|bitget|gate>

说明:
  - 仅构建并复制二进制 manual_signal 到目标目录（不自动启动）。
  - FR 目标目录:  $HOME/<exchange>_fr_<trade|test>/
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

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
      usage
      exit 1
      ;;
  esac
done

# 规范化为小写
EXCHANGE="$(echo "$EXCHANGE" | tr 'A-Z' 'a-z')"
case "$EXCHANGE" in
  binance|okex|bybit|bitget|gate)
    ;;
  *)
    echo "[ERROR] 不支持的 exchange: $EXCHANGE (支持: binance/okex/bybit/bitget/gate)"
    exit 1
    ;;
esac

TARGET_DIR="$HOME/${EXCHANGE}_fr_${ENV_TYPE}"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR (仅复制二进制)"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
