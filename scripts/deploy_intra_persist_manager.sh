#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="persist_manager"

# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_intra_persist_manager.sh --exchange <binance> [--env-suffix intra-trade] [--env-name binance-intra-trade] [--jobs <n>] [--cargo-target-dir <path>]

说明:
  - 同所期货现货套利专用，单一 exchange
  - 构建 persist_manager 并拷贝到 $HOME/<exchange>-<env_suffix>/
  - 同步辅助脚本: intra_scripts/{start,stop}_intra_persist_manager.sh

示例:
  scripts/deploy_intra_persist_manager.sh --exchange binance
  scripts/deploy_intra_persist_manager.sh --env-name binance-intra-trade --exchange binance
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

ENV_SUFFIX="intra-trade"
ENV_NAME=""
EXCHANGE=""
CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix) ENV_SUFFIX="${2:-intra-trade}"; shift 2 ;;
    --env-name)   ENV_NAME="${2:-}"; shift 2 ;;
    --exchange)   EXCHANGE="${2:-}"; shift 2 ;;
    --jobs)       BUILD_JOBS="${2:-}"; shift 2 ;;
    --cargo-target-dir) CARGO_TARGET_DIR_OVERRIDE="${2:-}"; shift 2 ;;
    *)
      echo "[ERROR] 未知参数: $1"; usage; exit 1 ;;
  esac
done

if [[ -n "$ENV_NAME" && -z "$EXCHANGE" ]]; then
  if [[ "${ENV_NAME,,}" =~ ^([a-z0-9]+)[-_]intra[-_][a-z0-9][a-z0-9_-]*$ ]]; then
    EXCHANGE="${BASH_REMATCH[1]}"
  fi
fi

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 需要 --exchange，或使用 --env-name <exchange>-intra-<tag>"
  usage; exit 1
fi

EXCHANGE="$(intra_ensure_exchange "$EXCHANGE")"

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${EXCHANGE}-${ENV_SUFFIX}"
fi
TARGET_DIR="$HOME/${ENV_NAME}"

echo "[INFO] 构建 $BIN_NAME (release)"
CARGO_TARGET_DIR_EFFECTIVE="$(intra_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"
(
  cd "$ROOT_DIR"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
    cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
)
BIN_PATH="$(intra_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$BIN_NAME")"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR" "$TARGET_DIR/data/persist_manager"

EXTRA_FILES=(
  "intra_scripts/start_intra_persist_manager.sh"
  "intra_scripts/stop_intra_persist_manager.sh"
  "intra_scripts/configure_intra_persist_sync_source.sh"
  "scripts/setup_nginx_stream_4190.sh"
)
echo "[INFO] 同步 intra_scripts 到 $TARGET_DIR"
for file in "${EXTRA_FILES[@]}"; do
  SRC_PATH="$ROOT_DIR/$file"
  if [[ -f "$SRC_PATH" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC_PATH" "$DEST_DIR/"
    chmod +x "$DEST_DIR/$(basename "$file")" 2>/dev/null || true
  fi
done

if ! intra_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  exit 2
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 配置 gRPC 同步源: cd $TARGET_DIR && ./intra_scripts/configure_intra_persist_sync_source.sh"
echo "[INFO] 启动: cd $TARGET_DIR && ./intra_scripts/start_intra_persist_manager.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./intra_scripts/stop_intra_persist_manager.sh"
