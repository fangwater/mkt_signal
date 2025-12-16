#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  deploy_account_monitor.sh [trade|test] --exchange okex|binance [--dir TARGET_DIR]

Examples:
  bash scripts/deploy_account_monitor.sh trade --exchange okex
  bash scripts/deploy_account_monitor.sh test  --exchange binance --dir "$HOME/fr_test"

Notes:
  - This script builds the existing per-exchange binaries:
      okex   -> okex_account_monitor
      binance-> binance_account_monitor
EOF
}

ENV_TYPE="trade"
EXCHANGE=""
TARGET_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      ENV_TYPE="$1"
      shift
      ;;
    --exchange|-e)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --dir|-d)
      TARGET_DIR="${2:-}"
      shift 2
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

if [[ -z "$TARGET_DIR" ]]; then
  case "$ENV_TYPE" in
    trade) TARGET_DIR="$HOME/fr_trade" ;;
    test) TARGET_DIR="$HOME/fr_test" ;;
    *)
      echo "[ERROR] Unknown env type: $ENV_TYPE (expected trade|test)" >&2
      exit 1
      ;;
  esac
fi

case "$EXCHANGE" in
  okex) BIN_NAME="okex_account_monitor" ;;
  binance) BIN_NAME="binance_account_monitor" ;;
  *)
    echo "[ERROR] --exchange must be one of: okex, binance" >&2
    usage >&2
    exit 1
    ;;
esac

BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

echo "[INFO] Build $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] Deploy $BIN_NAME -> $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/"
chmod +x "$TARGET_DIR/$BIN_NAME"

echo "[INFO] Done: $TARGET_DIR/$BIN_NAME"

