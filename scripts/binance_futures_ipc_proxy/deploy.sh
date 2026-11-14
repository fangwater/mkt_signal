#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOST="${1:-jp-meta-elvpn}"
REMOTE="/home/ubuntu/binance_futures_ipc_proxy"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BIN="${REPO_DIR}/target/release/binance_futures_ipc_proxy"

[[ -x "$BIN" ]] || { echo "[ERROR] build the release binary first: $BIN" >&2; exit 1; }
ssh "$HOST" "test \"\$(id -un)\" = ubuntu && mkdir -p '$REMOTE/scripts'"
scp "$BIN" "$HOST:$REMOTE/binance_futures_ipc_proxy.next.$STAMP"
scp "$REPO_DIR/scripts/binance_futures_ipc_proxy/start.sh" \
  "$REPO_DIR/scripts/binance_futures_ipc_proxy/stop.sh" "$HOST:$REMOTE/scripts/"
scp "$REPO_DIR/scripts/binance_futures_ipc_proxy/enable-env.patch" \
  "$HOST:$REMOTE/enable-env.patch"
LOCAL_SHA="$(sha256sum "$BIN" | cut -d' ' -f1)"
REMOTE_SHA="$(ssh "$HOST" "sha256sum '$REMOTE/binance_futures_ipc_proxy.next.$STAMP'" | cut -d' ' -f1)"
[[ "$LOCAL_SHA" = "$REMOTE_SHA" ]] || { echo "[ERROR] checksum mismatch" >&2; exit 1; }
ssh "$HOST" "chmod 755 '$REMOTE/scripts/start.sh' '$REMOTE/scripts/stop.sh' && mv '$REMOTE/binance_futures_ipc_proxy.next.$STAMP' '$REMOTE/binance_futures_ipc_proxy'"
echo "[INFO] deployed $REMOTE/binance_futures_ipc_proxy sha256=$LOCAL_SHA"
