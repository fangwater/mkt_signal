#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_signal"

# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_intra_trade_signal.sh --exchange <binance> [--env-suffix intra-trade] [--env-name binance-intra-trade] [--jobs <n>] [--cargo-target-dir <path>] [--sync-scripts|--bin-only]

说明:
  - 同所期货现货套利专用，单一 exchange
  - 构建 trade_signal 并拷贝到 $HOME/<exchange>-<env_suffix>/（默认 env_suffix=intra-trade）
  - trade_signal 启动时按 CWD 目录名推断分支：<exchange>-intra-<env>
  - 默认只更新二进制；如需同步脚本请添加 --sync-scripts

示例:
  scripts/deploy_intra_trade_signal.sh --exchange binance
  scripts/deploy_intra_trade_signal.sh --env-name binance-intra-trade --exchange binance
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
    --env-suffix) ENV_SUFFIX="${2:-intra-trade}"; shift 2 ;;
    --env-name)   ENV_NAME="${2:-}"; shift 2 ;;
    --exchange)   EXCHANGE="${2:-}"; shift 2 ;;
    --jobs)       BUILD_JOBS="${2:-}"; shift 2 ;;
    --cargo-target-dir) CARGO_TARGET_DIR_OVERRIDE="${2:-}"; shift 2 ;;
    --sync-scripts)
      [[ "$BIN_ONLY" == "1" ]] && { echo "[ERROR] --sync-scripts 与 --bin-only 互斥"; exit 1; }
      SYNC_SCRIPTS="1"; shift ;;
    --bin-only)
      [[ "$SYNC_SCRIPTS" == "1" ]] && { echo "[ERROR] --sync-scripts 与 --bin-only 互斥"; exit 1; }
      BIN_ONLY="1"; shift ;;
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
  usage
  exit 1
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
mkdir -p "$TARGET_DIR"

if [[ "$SYNC_SCRIPTS" == "1" ]]; then
  INTRA_SCRIPT_DIR_SRC="$ROOT_DIR/intra_scripts"
  TO_SYNC=(
    "start_intra_trade_signal.sh"
    "stop_intra_trade_signal.sh"
    "sync_intra_symbol_lists.py"
    "print_intra_symbol_lists.py"
    "sync_intra_strategy_params.py"
    "print_intra_strategy_params.py"
    "sync_intra_spread_thresholds.py"
    "print_intra_spread_thresholds.py"
    "sync_intra_funding_thresholds.py"
    "print_intra_funding_thresholds.py"
    "sync_intra_amount_u.py"
    "print_intra_amount_u.py"
    "sync_intra_max_pos_u.py"
    "print_intra_max_pos_u.py"
    "print_intra_hedge_offset_limits.py"
  )
  mkdir -p "$TARGET_DIR/intra_scripts"
  for f in "${TO_SYNC[@]}"; do
    SRC="$INTRA_SCRIPT_DIR_SRC/$f"
    if [[ -f "$SRC" ]]; then
      rsync -a "$SRC" "$TARGET_DIR/intra_scripts/"
      chmod +x "$TARGET_DIR/intra_scripts/$f"
    fi
  done
else
  echo "[INFO] 跳过脚本同步（如需同步，请添加 --sync-scripts）"
fi

if ! intra_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  exit 2
fi

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./intra_scripts/start_intra_trade_signal.sh"
if [[ "$SYNC_SCRIPTS" == "1" ]]; then
  echo "[INFO] Redis 参数/交易对同步脚本: $TARGET_DIR/intra_scripts"
  cat <<EOF
[INFO] 常用命令（在目标目录执行）:
  cd "$TARGET_DIR"
  ./intra_scripts/sync_intra_symbol_lists.py
  ./intra_scripts/print_intra_symbol_lists.py
  ./intra_scripts/sync_intra_strategy_params.py
  ./intra_scripts/print_intra_strategy_params.py
EOF
fi
