#!/usr/bin/env bash

set -Eeuo pipefail

readonly PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly CONFIG_PATH="${USSTOCK_MBP_REPLAY_CONFIG:-$PROJECT_DIR/config.toml}"
readonly CSV_ROOT="${USSTOCK_MBP_CSV_ROOT:-/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/usstock_mbp_csv}"
readonly RUST_ROOT="${USSTOCK_MBP_RUST_ROOT:-/mnt/nvme-raid0-28t/fanghaizhou/opt}"
readonly CARGO_BIN="$RUST_ROOT/cargo/bin/cargo"
readonly TARGET_DIR="${USSTOCK_MBP_TARGET_DIR:-$PROJECT_DIR/target}"
readonly EXPECTED_PERIODS=6

export CARGO_HOME="$RUST_ROOT/cargo"
export RUSTUP_HOME="$RUST_ROOT/rustup"
export PATH="$CARGO_HOME/bin:/usr/local/bin:/usr/bin:/bin"

log() {
  printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

cd "$PROJECT_DIR"
log "building release binary"
"$CARGO_BIN" build --release --locked --target-dir "$TARGET_DIR"

while true; do
  completed=$(find "$CSV_ROOT" -mindepth 2 -maxdepth 2 -type f -name decompress.complete | wc -l)
  if [[ "$completed" -eq "$EXPECTED_PERIODS" ]]; then
    break
  fi
  log "waiting for decompression completed=$completed expected=$EXPECTED_PERIODS"
  sleep 60
done

readonly REPLAY_BIN="$TARGET_DIR/release/usstock_lseg_mbp_replay"
log "running preflight"
"$REPLAY_BIN" --config "$CONFIG_PATH" --preflight
log "starting replay"
"$REPLAY_BIN" --config "$CONFIG_PATH"
log "verifying published databases"
"$REPLAY_BIN" --config "$CONFIG_PATH" --verify
log "all MBP periods replayed and verified"
