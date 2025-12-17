#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="trade_signal"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法: scripts/deploy_xarb_trade_signal.sh [trade|test] --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-name okex-binance-xarb-trade]

说明:
  - 构建 trade_signal 并拷贝到 $HOME/<openEx>-<hedgeEx>-xarb-<trade|test>/（或 --env-name 指定）。
  - xarb 固定 futures 资产类型：open/hedge 都必须为 *-futures。
  - trade_signal 启动时依赖 CWD 目录名推断分支与 Redis 前缀：
      - <exchange>_fr_trade / <exchange>_fr_test
      - <open>-<hedge>-xarb-trade / <open>-<hedge>-xarb-test

示例:
  scripts/deploy_xarb_trade_signal.sh trade --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_xarb_trade_signal.sh --env-name okex-binance-xarb-trade --open-venue okex-futures --hedge-venue binance-futures
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_TYPE="trade"
ENV_NAME=""
OPEN_VENUE=""
HEDGE_VENUE=""

normalize_venue() {
  echo "${1,,}"
}

ensure_futures_venue() {
  local v
  v="$(normalize_venue "$1")"
  if [[ -z "$v" || "$v" != *-futures ]]; then
    echo "[ERROR] xarb 只支持 futures：venue 必须以 -futures 结尾: $1"
    exit 1
  fi
  echo "$v"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      ENV_TYPE="$1"
      shift
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
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue"
  usage
  exit 1
fi

OPEN_VENUE="$(ensure_futures_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_futures_venue "$HEDGE_VENUE")"
if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] xarb 需要跨所：open=$OPEN_VENUE hedge=$HEDGE_VENUE"
  exit 1
fi

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
if [[ "$OPEN_EXCHANGE" == "okx" ]]; then
  OPEN_EXCHANGE="okex"
fi
if [[ "$HEDGE_EXCHANGE" == "okx" ]]; then
  HEDGE_EXCHANGE="okex"
fi

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-xarb-${ENV_TYPE}"
fi
TARGET_DIR="$HOME/${ENV_NAME}"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=(
  "start_trade_signal.sh"
  "stop_trade_signal.sh"
  "sync_xarb_symbol_lists.py"
  "print_xarb_symbol_lists.py"
  "sync_xarb_strategy_params.py"
  "print_xarb_strategy_params.py"
  "sync_xarb_spread_thresholds.py"
  "print_xarb_spread_thresholds.py"
  "sync_xarb_funding_rate_thresholds.py"
  "print_xarb_funding_rate_thresholds.py"
)
mkdir -p "$TARGET_DIR/scripts"
for script in "${SCRIPTS_TO_SYNC[@]}"; do
  if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
    rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 手动启动: cd $TARGET_DIR && ./scripts/start_trade_signal.sh"
