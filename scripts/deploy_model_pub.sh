#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="model_pub"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'USAGE'
Usage:
  deploy_model_pub.sh [--target <dir>]

Defaults:
  部署根目录 -> $HOME/model_pub（可用 --target 覆盖）

Examples:
  bash scripts/deploy_model_pub.sh
  bash scripts/deploy_model_pub.sh --target /opt/model_pub
USAGE
}

TARGET_DIR="$HOME/model_pub"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET_DIR="${2:-}"
      if [[ -z "$TARGET_DIR" ]]; then
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

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
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
echo "[INFO] 启动示例: cd $TARGET_DIR && ./scripts/start_model_pub.sh --model <model_name>"
