#!/usr/bin/env bash
set -euo pipefail

# Supported venues (use --venue in binary)
ALL_VENUES=(
  "binance-um"
  "okex-futures"
  "bybit-futures"
  "bitget-futures"
  "gate-futures"
)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_PATH="${ROOT_DIR}/mkt_pub"
if [[ ! -x "$BIN_PATH" ]]; then
  BIN_PATH="${ROOT_DIR}/target/release/mkt_pub"
fi

if [[ ! -x "$BIN_PATH" ]]; then
  echo "[ERROR] mkt_pub binary not found. Build first with: cargo build --release"
  exit 1
fi

if [[ $# -eq 0 ]]; then
  VENUES=("${ALL_VENUES[@]}")
else
  VENUES=("$@")
fi

# Validate venues
for v in "${VENUES[@]}"; do
  if ! printf '%s\0' "${ALL_VENUES[@]}" | grep -Fxqz "$v"; then
    echo "[ERROR] Unknown venue: $v"
    echo "Supported venues: ${ALL_VENUES[*]}"
    exit 1
  fi
done

start_one() {
  local venue="$1"
  local name="mkt_pub_${venue}"

  echo "[INFO] Restarting ${name}"
  npx pm2 delete "$name" >/dev/null 2>&1 || true

  npx pm2 start "$BIN_PATH" \
    --name "$name" \
    -- \
    --venue "$venue"
}

for venue in "${VENUES[@]}"; do
  start_one "$venue"
  sleep 1
done

echo ""
echo "[INFO] Started venues: ${VENUES[*]}"
echo "Logs: npx pm2 logs mkt_pub_<venue>"
echo "Status: npx pm2 status"
