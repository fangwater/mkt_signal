#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="$ROOT_DIR/target"
RELEASE_DIR="$TARGET_DIR/release"

usage() {
  cat <<'USAGE'
Usage: scripts/build-intra-binaries.sh

Builds every release binary used by the supported Bybit, OKX, and Binance
Intra stacks. Artifacts are written to target/release so publish-intra.sh
consumes exactly this build.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi
if [[ $# -ne 0 ]]; then
  echo "[ERROR] this script does not accept arguments: $*" >&2
  usage >&2
  exit 2
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo "[ERROR] required command not found: cargo" >&2
  exit 1
fi

echo "[INFO] building Intra release binaries"
(
  cd "$ROOT_DIR"
  cargo build --release --target-dir "$TARGET_DIR" \
    -p mkt_signal \
    --bin bybit_account_monitor \
    --bin okex_account_monitor \
    --bin binance_account_monitor \
    --bin hyperliquid_account_monitor \
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
  "$RELEASE_DIR/bybit_account_monitor"
  "$RELEASE_DIR/okex_account_monitor"
  "$RELEASE_DIR/binance_account_monitor"
  "$RELEASE_DIR/hyperliquid_account_monitor"
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

echo "[INFO] Intra release build complete; binaries=${#required_binaries[@]} persist_manager=included"
