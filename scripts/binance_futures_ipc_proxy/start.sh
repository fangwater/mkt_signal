#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BASE_DIR}/binance_futures_ipc_proxy"
NAME="binance_futures_ipc_proxy"

[[ -x "$BIN" ]] || { echo "[ERROR] missing $BIN" >&2; exit 1; }
if command -v pm2 >/dev/null 2>&1; then
  PM2=(pm2)
else
  PM2=(npx pm2)
fi

"${PM2[@]}" delete "$NAME" --namespace "$NAME" >/dev/null 2>&1 || true
RUST_LOG="${RUST_LOG:-info}" "${PM2[@]}" start "$BIN" \
  --name "$NAME" --namespace "$NAME" --cwd "$BASE_DIR" --time
"${PM2[@]}" describe "$NAME" --namespace "$NAME" | sed -n '1,20p'
