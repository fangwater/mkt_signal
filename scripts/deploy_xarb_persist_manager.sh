#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="persist_manager"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
用法: scripts/deploy_xarb_persist_manager.sh [trade|test] --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-name okex-binance-xarb-trade]

说明:
  - 构建 persist_manager 并拷贝到 $HOME/<open>-<hedge>-xarb-<trade|test>/（或 --env-name 指定）。
  - xarb 固定 futures 资产类型：open/hedge 都必须为 *-futures。
  - persist_manager 依赖 IPC_NAMESPACE（由 env.sh 提供）来订阅 pre_trade 的持久化通道：
      <IPC_NAMESPACE>/persist_pubs/pre_trade_signal_record
      <IPC_NAMESPACE>/persist_pubs/trade_update_record
      <IPC_NAMESPACE>/persist_pubs/order_update_record
  - 同步辅助脚本到目标目录:
      xarb_scripts/start_xarb_persist_manager.sh
      xarb_scripts/stop_xarb_persist_manager.sh

示例:
  scripts/deploy_xarb_persist_manager.sh trade --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_xarb_persist_manager.sh --env-name okex-binance-xarb-trade --open-venue okex-futures --hedge-venue binance-futures
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
(
  cd "$ROOT_DIR"
  cargo build --release --bin "$BIN_NAME"
)

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"
mkdir -p "$TARGET_DIR/data/persist_manager" >/dev/null 2>&1 || true

EXTRA_FILES=(
  "xarb_scripts/start_xarb_persist_manager.sh"
  "xarb_scripts/stop_xarb_persist_manager.sh"
)

echo "[INFO] 同步 xarb_scripts 到 $TARGET_DIR"
for file in "${EXTRA_FILES[@]}"; do
  SRC_PATH="$ROOT_DIR/$file"
  if [[ -f "$SRC_PATH" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC_PATH" "$DEST_DIR/"
    chmod +x "$DEST_DIR/$(basename "$file")" 2>/dev/null || true
  fi
done

echo "[INFO] $BIN_NAME 部署完成到 $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && ./xarb_scripts/start_xarb_persist_manager.sh --port 8088"
echo "[INFO] 停止: cd $TARGET_DIR && ./xarb_scripts/stop_xarb_persist_manager.sh"

