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
for command_name in cargo git sha256sum awk mktemp mv; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "[ERROR] required command not found: $command_name" >&2
    exit 1
  fi
done

echo "[INFO] building Intra release binaries"
(
  cd "$ROOT_DIR"
  cargo build --release --target-dir "$TARGET_DIR" \
    -p mkt_signal \
    --bin bybit_account_monitor \
    --bin okex_account_monitor \
    --bin binance_account_monitor \
    --bin rapidx_account_monitor \
    --bin rapidx_open_orders \
    --bin rapidx_order_smoke \
    --bin rapidx_intra_signal_smoke \
    --bin rapidx_loan \
    --bin rapidx_query_smoke \
    --bin rapidx_transfer \
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
  "$RELEASE_DIR/rapidx_account_monitor"
  "$RELEASE_DIR/rapidx_open_orders"
  "$RELEASE_DIR/rapidx_order_smoke"
  "$RELEASE_DIR/rapidx_intra_signal_smoke"
  "$RELEASE_DIR/rapidx_loan"
  "$RELEASE_DIR/rapidx_query_smoke"
  "$RELEASE_DIR/rapidx_transfer"
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

manifest_path="$RELEASE_DIR/intra-release.manifest"
manifest_body="$(mktemp "$RELEASE_DIR/.intra-release-body.XXXXXX")"
manifest_tmp="$(mktemp "$RELEASE_DIR/.intra-release-manifest.XXXXXX")"
cleanup_manifest_tmp() {
  rm -f "$manifest_body" "$manifest_tmp" >/dev/null 2>&1 || true
}
trap cleanup_manifest_tmp EXIT

git_commit="$(git -C "$ROOT_DIR" rev-parse HEAD)"
for binary in "${required_binaries[@]}"; do
  binary_name="$(basename "$binary")"
  binary_hash="$(sha256sum "$binary" | awk '{print $1}')"
  printf 'binary %s %s\n' "$binary_name" "$binary_hash" >>"$manifest_body"
done
release_id="$(sha256sum "$manifest_body" | awk '{print $1}')"
{
  printf 'release_id %s\n' "$release_id"
  printf 'git_commit %s\n' "$git_commit"
  cat "$manifest_body"
} >"$manifest_tmp"
chmod 644 "$manifest_tmp"
mv -f "$manifest_tmp" "$manifest_path"
rm -f "$manifest_body"
trap - EXIT

echo "[INFO] Intra release build complete; binaries=${#required_binaries[@]} persist_manager=included release_id=$release_id"
