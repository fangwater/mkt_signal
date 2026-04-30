#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="manual_signal"

# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_intra_manual_signal.sh --exchange <binance> [--env-suffix intra-trade] [--env-name binance-intra-trade] [--jobs <n>] [--cargo-target-dir <path>]

说明:
  - 同所期现：构建 manual_signal 并拷贝到 $HOME/<exchange>-<env_suffix>/
  - 目录名约定: <exchange>-intra-trade（例如 binance-intra-trade）

示例:
  scripts/deploy_intra_manual_signal.sh --exchange binance
  scripts/deploy_intra_manual_signal.sh --env-name binance-intra-trade --exchange binance
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

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR (仅复制二进制)"
mkdir -p "$TARGET_DIR"
if ! intra_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  exit 2
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
