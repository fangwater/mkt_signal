#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_engine"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

# 参数解析
ENV_TYPE="trade"
EXCHANGE="binance"
ENV_NAME=""
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
    --env-name)
      ENV_NAME="${2:-}"
      if [[ -z "$ENV_NAME" ]]; then
        echo "[ERROR] --env-name 需要一个值"
        exit 1
      fi
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      echo "用法: $0 [trade|test] [--exchange binance|okex|bybit|bitget|gate] [--env-name <exchange>_fr_<suffix>]"
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

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
}

require_fr_env_name() {
  local exchange="$1"
  local name="$2"
  if [[ ! "$name" =~ ^${exchange}_fr(_[a-z0-9][a-z0-9_-]*)?$ ]]; then
    echo "[ERROR] env-name must match ${exchange}_fr_<suffix> (got: ${name})" >&2
    exit 1
  fi
}

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${EXCHANGE}_fr_${ENV_TYPE}"
fi
ENV_NAME="$(normalize_env_name "$ENV_NAME")"
require_fr_env_name "$EXCHANGE" "$ENV_NAME"

TARGET_DIR="$HOME/${ENV_NAME}"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
mkdir -p "$TARGET_DIR/config"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

# 同步启动/停止脚本
SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=("start_trade_engine.sh" "stop_trade_engine.sh")
mkdir -p "$TARGET_DIR/scripts"
for script in "${SCRIPTS_TO_SYNC[@]}"; do
  if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
    rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

# trade_engine 读取 /home/<user>/config/mkt_cfg.yaml 的本地 IP；无需部署额外配置文件

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./scripts/start_trade_engine.sh"
