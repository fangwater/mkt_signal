#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${1:-$HOME/crypto_mkt}"

SRC_CONFIG="$ROOT_DIR/config"
DEST_CONFIG="$TARGET_DIR/config"

if [[ ! -d "$SRC_CONFIG" ]]; then
  echo "[ERROR] Source config directory not found: $SRC_CONFIG" >&2
  exit 1
fi

echo "[INFO] Copying configs from $SRC_CONFIG to $DEST_CONFIG"
mkdir -p "$DEST_CONFIG"
rsync -a --delete "$SRC_CONFIG/" "$DEST_CONFIG/"

echo "[INFO] Config update complete."
