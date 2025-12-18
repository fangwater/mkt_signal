#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
用法: scripts/deploy_xarb_monitors.sh [trade|test] --open-venue <okex-futures> --hedge-venue <binance-futures> [--env-name okex-binance-xarb-trade]

说明:
  - 构建并部署 xarb 所需的两侧账户 monitor（二进制为 okex_account_monitor / binance_account_monitor）。
  - 输出到 $HOME/<open>-<hedge>-xarb-<trade|test>/（或 --env-name 指定）：
      account_monitor_okex
      account_monitor_binance
      xarb_scripts/start_xarb_monitors.sh
      xarb_scripts/stop_xarb_monitors.sh

示例:
  scripts/deploy_xarb_monitors.sh trade --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_xarb_monitors.sh --env-name okex-binance-xarb-trade --open-venue okex-futures --hedge-venue binance-futures
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

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
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

OPEN_EXCHANGE="$(normalize_exchange "${OPEN_VENUE%%-*}")"
HEDGE_EXCHANGE="$(normalize_exchange "${HEDGE_VENUE%%-*}")"
if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-xarb-${ENV_TYPE}"
fi

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR/xarb_scripts"

deploy_one() {
  local exchange="$1"
  local bin_name=""
  local out_name=""
  case "$exchange" in
    okex)
      bin_name="okex_account_monitor"
      out_name="account_monitor_okex"
      ;;
    binance)
      bin_name="binance_account_monitor"
      out_name="account_monitor_binance"
      ;;
    *)
      echo "[ERROR] xarb monitors 当前仅支持 okex/binance：got exchange=$exchange"
      exit 1
      ;;
  esac

  echo "[INFO] 构建 $bin_name (release)"
  (
    cd "$ROOT_DIR"
    cargo build --release --bin "$bin_name"
  )

  echo "[INFO] 部署 $bin_name -> $TARGET_DIR/$out_name"
  cp "$ROOT_DIR/target/release/$bin_name" "$TARGET_DIR/$out_name"
  chmod +x "$TARGET_DIR/$out_name"
}

deploy_one "$OPEN_EXCHANGE"
if [[ "$HEDGE_EXCHANGE" != "$OPEN_EXCHANGE" ]]; then
  deploy_one "$HEDGE_EXCHANGE"
fi

# 同步 xarb monitor start/stop 脚本
SCRIPTS_TO_SYNC=(
  "xarb_scripts/start_xarb_monitors.sh"
  "xarb_scripts/stop_xarb_monitors.sh"
)
for file in "${SCRIPTS_TO_SYNC[@]}"; do
  SRC="$ROOT_DIR/$file"
  if [[ -f "$SRC" ]]; then
    rsync -a "$SRC" "$TARGET_DIR/$(dirname "$file")/"
    chmod +x "$TARGET_DIR/$file" 2>/dev/null || true
  fi
done

echo "[INFO] xarb monitors 部署完成: $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && source ./env.sh && ./xarb_scripts/start_xarb_monitors.sh"
