#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="model_pub"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'USAGE'
Usage:
  deploy_model_pub.sh [--target <model_name>]

Behavior:
  - 不传 --target 时，使用 config/model_pub.toml 中默认的 model_name 推断
  - 部署目录: ~/model_pub/<model_name>/
  - toml 中的 {model_name} 会被替换为实际 model_name

Examples:
  bash scripts/deploy_model_pub.sh --target binance_futures_direction_model
  bash scripts/deploy_model_pub.sh
USAGE
}

MODEL_NAME=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      MODEL_NAME="${2:-}"
      if [[ -z "$MODEL_NAME" ]]; then
        echo "[ERROR] --target 需要一个值" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$MODEL_NAME" ]]; then
  MODEL_NAME="binance_futures_direction_model"
  echo "[INFO] 未指定 --target，使用默认 model_name: $MODEL_NAME"
fi

TARGET_DIR="$HOME/model_pub/${MODEL_NAME}"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR (model_name=$MODEL_NAME)"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"
chmod +x "$TARGET_DIR/$BIN_NAME"

mkdir -p "$TARGET_DIR/scripts"
for script in start_model_pub.sh stop_model_pub.sh; do
  if [[ -f "$ROOT_DIR/scripts/$script" ]]; then
    rsync -a "$ROOT_DIR/scripts/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

mkdir -p "$TARGET_DIR/config"
if [[ -f "$ROOT_DIR/config/model_pub.toml" ]]; then
  rsync -a "$ROOT_DIR/config/model_pub.toml" "$TARGET_DIR/config/"
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] target_dir: $TARGET_DIR"
echo "[INFO] model_name: $MODEL_NAME"
echo "[INFO] 启动: cd $TARGET_DIR && ./scripts/start_model_pub.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./scripts/stop_model_pub.sh"
