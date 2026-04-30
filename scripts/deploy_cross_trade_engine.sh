#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_engine"

# shellcheck source=scripts/deploy_cross_lib.sh
source "$ROOT_DIR/scripts/deploy_cross_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_cross_trade_engine.sh [--open-venue <okex-futures>] [--hedge-venue <binance-futures>] [--env-suffix cross-trade] [--env-name okex-binance-cross-trade] [--jobs <n>] [--cargo-target-dir <path>]

说明:
  - 跨所合约对套利专用：open/hedge 必须都是 -futures/-swap/-perp，且不相同。
  - 构建 trade_engine 并拷贝到 $HOME/<open>-<hedge>-<env_suffix>/ 目录（默认 env_suffix=cross-trade）。
  - 跨所目录约定: <open>-<hedge>-cross-trade（例如 okex-binance-cross-trade）。
  - 同步辅助脚本:
      cross_scripts/start_cross_trade_engine.sh
      cross_scripts/stop_cross_trade_engine.sh
  - 不自动启动；部署后在目标目录执行：
      ./cross_scripts/start_cross_trade_engine.sh
      ./cross_scripts/stop_cross_trade_engine.sh

提示:
  - 建议先生成 env: scripts/deploy_setup_env_cross.sh --open-venue ... --hedge-venue ...

示例:
  scripts/deploy_cross_trade_engine.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_cross_trade_engine.sh --env-name okex-binance-cross-trade
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

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"

EXTRA_FILES=(
  "cross_scripts/start_cross_trade_engine.sh"
  "cross_scripts/stop_cross_trade_engine.sh"
)

echo "[INFO] 同步 cross_scripts 到 $TARGET_DIR（先更新脚本，避免二进制 busy 影响脚本更新）"
for file in "${EXTRA_FILES[@]}"; do
  SRC_PATH="$ROOT_DIR/$file"
  if [[ -f "$SRC_PATH" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC_PATH" "$DEST_DIR/"
    chmod +x "$DEST_DIR/$(basename "$file")" 2>/dev/null || true
  fi
done

echo "[INFO] 构建 $BIN_NAME (release)"
CARGO_TARGET_DIR_EFFECTIVE="$(cross_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"
(
  cd "$ROOT_DIR"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
    cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
)

BIN_PATH="$(cross_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$BIN_NAME")"
if ! cross_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  echo "[WARN] 二进制更新失败但脚本已同步: $TARGET_DIR/cross_scripts"
  exit 2
fi

echo "[INFO] 部署完成: $TARGET_DIR"
echo "[INFO] venues: open=${OPEN_VENUE} hedge=${HEDGE_VENUE}"
echo "[INFO] 建议先配置 env: scripts/deploy_setup_env_cross.sh --env-name ${ENV_NAME} --open-venue ${OPEN_VENUE} --hedge-venue ${HEDGE_VENUE}"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./cross_scripts/start_cross_trade_engine.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./cross_scripts/stop_cross_trade_engine.sh"
