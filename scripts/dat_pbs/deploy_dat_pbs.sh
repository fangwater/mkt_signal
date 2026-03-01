#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_NAME="dat_pbs"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
Usage:
  deploy_dat_pbs.sh [--dir <path>]

Defaults:
  --dir not set -> $HOME/dat_pbs

Examples:
  bash scripts/dat_pbs/deploy_dat_pbs.sh
  bash scripts/dat_pbs/deploy_dat_pbs.sh --dir "$HOME/dat_pbs"

Notes:
  - Exchange is selected at runtime via scripts/start_dat_pbs.sh --exchange <exchange>
  - This deploy variant is for dat_pbs processes managed by pmdaemon.
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
  TARGET_DIR="$HOME/dat_pbs"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
# 原子替换二进制，避免直接覆盖运行中文件触发 "Text file busy"
tmp_bin="$(mktemp "$TARGET_DIR/${BIN_NAME}.new.XXXXXX")"
cp "$BIN_PATH" "$tmp_bin"
chmod +x "$tmp_bin"
mv -f "$tmp_bin" "$TARGET_DIR/$BIN_NAME"

# 同步启动/停止脚本到 scripts/（与 depth_pub 一致）
SCRIPT_DIR_SRC="$ROOT_DIR/scripts/dat_pbs"
SCRIPTS_TO_SYNC=("start_dat_pbs.sh" "stop_dat_pbs.sh" "setup_pmdaemon_logrotate.sh")
mkdir -p "$TARGET_DIR/scripts"
for script in "${SCRIPTS_TO_SYNC[@]}"; do
  if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
    rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

# 同步 mkt_cfg.yaml + iceoryx2.toml
mkdir -p "$TARGET_DIR/config"
if [[ -f "$ROOT_DIR/config/mkt_cfg.yaml" ]]; then
  rsync -a "$ROOT_DIR/config/mkt_cfg.yaml" "$TARGET_DIR/config/"
fi
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "$TARGET_DIR/config/"
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 启动示例: cd $TARGET_DIR && ./scripts/start_dat_pbs.sh --exchange binance"
