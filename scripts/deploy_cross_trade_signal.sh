#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_signal"

# shellcheck source=scripts/deploy_cross_lib.sh
source "$ROOT_DIR/scripts/deploy_cross_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_cross_trade_signal.sh --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-suffix cross-trade] [--env-name okex-binance-cross-trade] [--jobs <n>] [--cargo-target-dir <path>] [--sync-scripts|--bin-only]

说明:
  - 跨所合约对套利专用：open/hedge 必须都是 -futures/-swap/-perp，且不相同。
  - 构建 trade_signal 并拷贝到 $HOME/<openEx>-<hedgeEx>-<env_suffix>/（默认 env_suffix=cross-trade）。
  - trade_signal 启动时依赖 CWD 目录名推断分支与 Redis 前缀:
      <open>-<hedge>-cross-trade / <open>-<hedge>-cross-arb01..03
  - 默认只更新二进制；如需同步脚本请添加 --sync-scripts（或显式使用 --bin-only）

示例:
  scripts/deploy_cross_trade_signal.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_cross_trade_signal.sh --env-name okex-binance-cross-trade --open-venue okex-futures --hedge-venue binance-futures
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
SYNC_SCRIPTS="0"
BIN_ONLY="0"

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

if [[ "$SYNC_SCRIPTS" == "1" ]]; then
  SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
  CROSS_SCRIPT_DIR_SRC="$ROOT_DIR/cross_scripts"
  CROSS_SCRIPTS_TO_SYNC=(
    "start_cross_trade_signal.sh"
    "stop_cross_trade_signal.sh"
  )
  CROSS_TOOLS_TO_SYNC=(
    "sync_cross_symbol_lists.py"
    "print_cross_symbol_lists.py"
    "set_um_leverage.py"
    "set_cross_futures_leverage.py"
    "set_cross_cancel_all.py"
    "set_cross_align.py"
    "verify_cross_account_modes.py"
    "sync_cross_strategy_params.py"
    "print_cross_strategy_params.py"
    "sync_cross_spread_thresholds.py"
    "print_cross_spread_thresholds.py"
    "sync_cross_funding_thresholds.py"
    "print_cross_funding_thresholds.py"
  )
  mkdir -p "$TARGET_DIR/cross_scripts"

  for script in "${CROSS_SCRIPTS_TO_SYNC[@]}"; do
    if [[ -f "$CROSS_SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$CROSS_SCRIPT_DIR_SRC/$script" "$TARGET_DIR/cross_scripts/"
      chmod +x "$TARGET_DIR/cross_scripts/$script"
    fi
  done

  for tool in "${CROSS_TOOLS_TO_SYNC[@]}"; do
    if [[ -f "$CROSS_SCRIPT_DIR_SRC/$tool" ]]; then
      rsync -a "$CROSS_SCRIPT_DIR_SRC/$tool" "$TARGET_DIR/cross_scripts/"
      chmod +x "$TARGET_DIR/cross_scripts/$tool"
    elif [[ -f "$SCRIPT_DIR_SRC/$tool" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$tool" "$TARGET_DIR/cross_scripts/"
      chmod +x "$TARGET_DIR/cross_scripts/$tool"
    fi
  done
  cross_sync_contract_ops_scripts "$ROOT_DIR" "$TARGET_DIR"
else
  echo "[INFO] 跳过脚本同步（如需同步脚本，请添加 --sync-scripts）"
fi

if ! cross_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  exit 2
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./cross_scripts/start_cross_trade_signal.sh"
if [[ "$SYNC_SCRIPTS" == "1" ]]; then
  echo "[INFO] cross 的 Redis 参数/交易对同步脚本在: $TARGET_DIR/cross_scripts"
  cat <<EOF
[INFO] 常用命令（在目标目录执行，可自动从 CWD 推断 open/hedge pair）:
  cd "$TARGET_DIR"
  ./cross_scripts/sync_cross_symbol_lists.py
  ./cross_scripts/print_cross_symbol_lists.py
  ./cross_scripts/set_um_leverage.py --leverage 6
  ./cross_scripts/set_cross_futures_leverage.py --leverage 5
  ./cross_scripts/set_cross_cancel_all.py
  ./cross_scripts/set_cross_cancel_all.py --execute
  ./cross_scripts/set_cross_align.py
  ./cross_scripts/set_cross_align.py --execute
  ./cross_scripts/sync_cross_strategy_params.py
  ./cross_scripts/print_cross_strategy_params.py
  ./cross_scripts/cross_contract_ops.py cancel both
EOF
else
  echo "[INFO] 未同步脚本（保留目标目录已有 cross_scripts，需更新请加 --sync-scripts）"
fi
