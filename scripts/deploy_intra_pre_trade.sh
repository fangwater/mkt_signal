#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="pre_trade"

# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_intra_pre_trade.sh --exchange <binance> [--env-suffix intra-trade] [--env-name binance-intra-trade] [--jobs <n>] [--cargo-target-dir <path>] [--sync-scripts|--bin-only]

说明:
  - 同所期货现货套利专用，只接受单一 exchange（margin 与 futures 自动配对）
  - 构建 pre_trade 并拷贝到 $HOME/<exchange>-<env_suffix>/（默认 env_suffix=intra-trade）
  - 部署目录约定: <exchange>-intra-trade（例如 binance-intra-trade）
  - 同步辅助脚本: intra_scripts/{start,stop}_intra_pre_trade.sh, sync/print_intra_risk_params.py
  - 不自动启动；部署后在目标目录:
      ./intra_scripts/start_intra_pre_trade.sh
      ./intra_scripts/stop_intra_pre_trade.sh
  - 默认只更新二进制；如需同步脚本请添加 --sync-scripts

提示:
  - 建议先生成 env: scripts/deploy_setup_env_intra.sh --exchange <ex>

示例:
  scripts/deploy_intra_pre_trade.sh --exchange binance
  scripts/deploy_intra_pre_trade.sh --env-name binance-intra-trade --exchange binance
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX="intra-trade"
ENV_NAME=""
EXCHANGE=""
CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""
SYNC_SCRIPTS="0"
BIN_ONLY="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix)
      ENV_SUFFIX="${2:-intra-trade}"
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --jobs)
      BUILD_JOBS="${2:-}"
      shift 2
      ;;
    --cargo-target-dir)
      CARGO_TARGET_DIR_OVERRIDE="${2:-}"
      shift 2
      ;;
    --sync-scripts)
      if [[ "$BIN_ONLY" == "1" ]]; then
        echo "[ERROR] --sync-scripts 与 --bin-only 互斥"
        exit 1
      fi
      SYNC_SCRIPTS="1"
      shift
      ;;
    --bin-only)
      if [[ "$SYNC_SCRIPTS" == "1" ]]; then
        echo "[ERROR] --sync-scripts 与 --bin-only 互斥"
        exit 1
      fi
      BIN_ONLY="1"
      shift
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

# 由 env_name 反推 exchange（格式: <exchange>-intra-<tag>）
if [[ -n "$ENV_NAME" && -z "$EXCHANGE" ]]; then
  if [[ "${ENV_NAME,,}" =~ ^([a-z0-9]+)[-_]intra[-_][a-z0-9][a-z0-9_-]*$ ]]; then
    EXCHANGE="${BASH_REMATCH[1]}"
  fi
fi

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 需要 --exchange，或使用 --env-name <exchange>-intra-<tag>"
  usage
  exit 1
fi

EXCHANGE="$(intra_ensure_exchange "$EXCHANGE")"
read -r OPEN_VENUE HEDGE_VENUE <<<"$(intra_venues_for_exchange "$EXCHANGE")"

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${EXCHANGE}-${ENV_SUFFIX}"
fi

TARGET_DIR="$HOME/${ENV_NAME}"
echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"

EXTRA_FILES=(
  "intra_scripts/start_intra_pre_trade.sh"
  "intra_scripts/stop_intra_pre_trade.sh"
  "intra_scripts/sync_intra_risk_params.py"
  "intra_scripts/print_intra_risk_params.py"
  "intra_scripts/start_intra_persist_manager.sh"
  "intra_scripts/stop_intra_persist_manager.sh"
)

if [[ "$SYNC_SCRIPTS" == "1" ]]; then
  echo "[INFO] 同步 intra_scripts/ 到 $TARGET_DIR"
  for file in "${EXTRA_FILES[@]}"; do
    SRC_PATH="$ROOT_DIR/$file"
    if [[ -f "$SRC_PATH" ]]; then
      DEST_DIR="$TARGET_DIR/$(dirname "$file")"
      mkdir -p "$DEST_DIR"
      rsync -a "$SRC_PATH" "$DEST_DIR/"
      chmod +x "$DEST_DIR/$(basename "$file")" 2>/dev/null || true
    fi
  done
else
  echo "[INFO] 跳过脚本同步（如需，请添加 --sync-scripts）"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
CARGO_TARGET_DIR_EFFECTIVE="$(intra_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"
(
  cd "$ROOT_DIR"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
    cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
)

BIN_PATH="$(intra_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$BIN_NAME")"
if ! intra_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  if [[ "$SYNC_SCRIPTS" == "1" ]]; then
    echo "[WARN] 二进制更新失败但脚本已同步: $TARGET_DIR/intra_scripts"
  fi
  echo "[WARN] 请稍后重试或先停止进程: $TARGET_DIR/$BIN_NAME"
  exit 2
fi

echo "[INFO] 部署完成: $TARGET_DIR"
echo "[INFO] exchange=$EXCHANGE open=$OPEN_VENUE hedge=$HEDGE_VENUE"
echo "[INFO] 建议先配置 env: scripts/deploy_setup_env_intra.sh --env-name ${ENV_NAME} --exchange ${EXCHANGE}"
echo "[INFO] 启动: cd $TARGET_DIR && ./intra_scripts/start_intra_pre_trade.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./intra_scripts/stop_intra_pre_trade.sh"
