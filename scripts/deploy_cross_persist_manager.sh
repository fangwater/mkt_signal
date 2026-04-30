#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="persist_manager"

# shellcheck source=scripts/deploy_cross_lib.sh
source "$ROOT_DIR/scripts/deploy_cross_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_cross_persist_manager.sh --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-suffix cross-trade] [--env-name okex-binance-cross-trade] [--jobs <n>] [--cargo-target-dir <path>]

说明:
  - 跨所合约对套利专用：open/hedge 必须都是 -futures/-swap/-perp，且不相同。
  - 构建 persist_manager 并拷贝到 $HOME/<open>-<hedge>-<env_suffix>/（默认 env_suffix=cross-trade）。
  - persist_manager 依赖 IPC_NAMESPACE（由 env.sh 提供）来订阅 pre_trade 的持久化通道:
      <IPC_NAMESPACE>/persist_pubs/pre_trade_signal_record
      <IPC_NAMESPACE>/persist_pubs/trade_update_record
      <IPC_NAMESPACE>/persist_pubs/order_update_record
  - 同步辅助脚本到目标目录:
      cross_scripts/start_cross_persist_manager.sh
      cross_scripts/stop_cross_persist_manager.sh

示例:
  scripts/deploy_cross_persist_manager.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_cross_persist_manager.sh --env-name okex-binance-cross-trade --open-venue okex-futures --hedge-venue binance-futures
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX="cross-trade"
ENV_NAME=""
OPEN_VENUE=""
HEDGE_VENUE=""
CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix)
      ENV_SUFFIX="${2:-cross-trade}"
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --open-venue)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      HEDGE_VENUE="${2:-}"
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
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -n "$ENV_NAME" && ( -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ) ]]; then
  if inferred="$(cross_infer_pair_from_name "$ENV_NAME")" && [[ -n "$inferred" ]]; then
    OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-futures}"
    HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
  fi
fi

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue，或使用 --env-name <open>-<hedge>-cross-<tag>"
  usage
  exit 1
fi

PAIR_RESULT="$(cross_ensure_venue_pair "$OPEN_VENUE" "$HEDGE_VENUE")"
if [[ -z "$PAIR_RESULT" ]]; then
  exit 1
fi
read -r OPEN_VENUE HEDGE_VENUE <<<"$PAIR_RESULT"

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-${ENV_SUFFIX}"
fi
TARGET_DIR="$HOME/${ENV_NAME}"

echo "[INFO] 构建 $BIN_NAME (release)"
CARGO_TARGET_DIR_EFFECTIVE="$(cross_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"
(
  cd "$ROOT_DIR"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
    cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
)
BIN_PATH="$(cross_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$BIN_NAME")"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
mkdir -p "$TARGET_DIR/data/persist_manager" >/dev/null 2>&1 || true

EXTRA_FILES=(
  "cross_scripts/start_cross_persist_manager.sh"
  "cross_scripts/stop_cross_persist_manager.sh"
)

echo "[INFO] 同步 cross_scripts 到 $TARGET_DIR"
for file in "${EXTRA_FILES[@]}"; do
  SRC_PATH="$ROOT_DIR/$file"
  if [[ -f "$SRC_PATH" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC_PATH" "$DEST_DIR/"
    chmod +x "$DEST_DIR/$(basename "$file")" 2>/dev/null || true
  fi
done

if ! cross_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  exit 2
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && ./cross_scripts/start_cross_persist_manager.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./cross_scripts/stop_cross_persist_manager.sh"
