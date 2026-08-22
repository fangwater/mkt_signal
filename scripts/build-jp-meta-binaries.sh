#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXCHANGE=""

usage() {
  cat <<'USAGE'
Usage: scripts/build-jp-meta-binaries.sh --exchange <name>

Options:
  --exchange <name>   Exchange (binance, gate, okex, or okx)
  -h, --help          Show this help

Builds every release binary used by the JP Meta FR/MM base stack. Artifacts
are always written to target/release so the publish scripts consume exactly
the binaries produced here.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

EXCHANGE="${EXCHANGE,,}"
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi
case "$EXCHANGE" in
  binance|gate|okex) ;;
  *)
    echo "[ERROR] exchange must be binance, gate, okex, or okx: $EXCHANGE" >&2
    exit 2
    ;;
esac

if ! command -v cargo >/dev/null 2>&1; then
  echo "[ERROR] required command not found: cargo" >&2
  exit 1
fi

ACCOUNT_MONITOR_BIN="${EXCHANGE}_account_monitor"
TARGET_DIR="$ROOT_DIR/target"
RELEASE_DIR="$TARGET_DIR/release"

echo "[INFO] building JP Meta release binaries for exchange=$EXCHANGE"
(
  cd "$ROOT_DIR"
  cargo build --release --target-dir "$TARGET_DIR" \
    -p mkt_signal \
    --bin "$ACCOUNT_MONITOR_BIN" \
    --bin pre_trade \
    --bin trade_engine
  cargo build --release --target-dir "$TARGET_DIR" \
    -p trade_signal \
    --bin trade_signal
  cargo build --release --target-dir "$TARGET_DIR" \
    -p viz_server \
    --bin viz_server
  cargo build --release --target-dir "$TARGET_DIR" \
    -p persist_manager \
    --features runtime \
    --bin persist_manager
)

required_binaries=(
  "$RELEASE_DIR/$ACCOUNT_MONITOR_BIN"
  "$RELEASE_DIR/pre_trade"
  "$RELEASE_DIR/trade_engine"
  "$RELEASE_DIR/trade_signal"
  "$RELEASE_DIR/viz_server"
  "$RELEASE_DIR/persist_manager"
)
for binary in "${required_binaries[@]}"; do
  if [[ ! -x "$binary" ]]; then
    echo "[ERROR] expected release binary is missing or not executable: $binary" >&2
    exit 1
  fi
done

echo "[INFO] JP Meta release build complete; binaries=${#required_binaries[@]} persist_manager=included"
