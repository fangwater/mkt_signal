#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="depth_pub"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
Usage:
  deploy_depth_pub.sh [--dir <path>]

Defaults:
  --dir not provided -> $HOME/depth_pub

Examples:
  bash scripts/deploy_depth_pub.sh
  bash scripts/deploy_depth_pub.sh --dir "$HOME/depth_pub"

Notes:
  - Venue is selected at runtime via start_depth_pub.sh --venue <venue>
EOF
}

# 参数解析
TARGET_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)
      TARGET_DIR="${2:-}"
      if [[ -z "$TARGET_DIR" ]]; then
        echo "[ERROR] --dir 需要一个路径" >&2
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
      echo "[ERROR] 未知参数: $1"
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME/depth_pub"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

# 同步启动/停止脚本到 scripts/
SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=("start_depth_pub.sh" "stop_depth_pub.sh")
mkdir -p "$TARGET_DIR/scripts"
for script in "${SCRIPTS_TO_SYNC[@]}"; do
  if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
    rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

# 同步 depth_cfg.yaml + iceoryx2.toml
mkdir -p "$TARGET_DIR/config"
if [[ -f "$ROOT_DIR/config/depth_cfg.yaml" ]]; then
  rsync -a "$ROOT_DIR/config/depth_cfg.yaml" "$TARGET_DIR/config/"
fi
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "$TARGET_DIR/config/"
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 启动示例: cd $TARGET_DIR && ./scripts/start_depth_pub.sh --venue binance-futures"
