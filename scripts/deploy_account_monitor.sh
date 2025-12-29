#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  deploy_account_monitor.sh [trade|test] --exchange okex|binance|gate [--scripts-only|--bin-only]

Examples:
  bash scripts/deploy_account_monitor.sh trade --exchange okex
  bash scripts/deploy_account_monitor.sh test  --exchange binance
  bash scripts/deploy_account_monitor.sh trade --exchange gate

Notes:
  - This script builds the existing per-exchange binaries:
      okex   -> okex_account_monitor
      binance-> binance_account_monitor
      gate   -> gate_account_monitor
  - Deploy dir:
      $HOME/<exchange>_fr_<env>   (e.g. $HOME/okex_fr_trade, $HOME/binance_fr_trade)
EOF
}

ENV_TYPE="trade"
EXCHANGE=""
DO_BUILD=1
DO_SCRIPTS=1
ONLY_MODE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      ENV_TYPE="$1"
      shift
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --scripts-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only 与 --bin-only 互斥" >&2
        exit 1
      fi
      ONLY_MODE="scripts"
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only 与 --bin-only 互斥" >&2
        exit 1
      fi
      ONLY_MODE="bin"
      DO_BUILD=1
      DO_SCRIPTS=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] --exchange must be one of: okex, binance, gate" >&2
  usage >&2
  exit 1
fi

# 规范化为小写
EXCHANGE="$(echo "$EXCHANGE" | tr 'A-Z' 'a-z')"

case "$EXCHANGE" in
  okex|binance|gate) ;;
  *)
    echo "[ERROR] --exchange must be one of: okex, binance, gate" >&2
    usage >&2
    exit 1
    ;;
esac

TARGET_DIR="$HOME/${EXCHANGE}_fr_${ENV_TYPE}"

case "$EXCHANGE" in
  okex) BIN_NAME="okex_account_monitor" ;;
  binance) BIN_NAME="binance_account_monitor" ;;
  gate) BIN_NAME="gate_account_monitor" ;;
esac

BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] Build $BIN_NAME (release)"
  cargo build --release --bin "$BIN_NAME"
fi

mkdir -p "$TARGET_DIR"
if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] Deploy $BIN_NAME -> $TARGET_DIR"
  cp "$BIN_PATH" "$TARGET_DIR/account_monitor"
  chmod +x "$TARGET_DIR/account_monitor"
fi

# 同步启动/停止脚本到 scripts/
SCRIPT_DIR_SRC="$ROOT_DIR/scripts"
SCRIPTS_TO_SYNC=("start_account_monitor.sh" "stop_account_monitor.sh")
if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/scripts"
  for script in "${SCRIPTS_TO_SYNC[@]}"; do
    if [[ -f "$SCRIPT_DIR_SRC/$script" ]]; then
      rsync -a "$SCRIPT_DIR_SRC/$script" "$TARGET_DIR/scripts/"
      chmod +x "$TARGET_DIR/scripts/$script"
    fi
  done
fi

echo "[INFO] Done: $TARGET_DIR/account_monitor"
echo "[INFO] Start: cd $TARGET_DIR && ./scripts/start_account_monitor.sh"
