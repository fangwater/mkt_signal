#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$ROOT_DIR/config/mkt_api_key.toml"

export RUST_LOG="${RUST_LOG:-debug}"

if [[ -f "$CONFIG_FILE" ]]; then
  BINANCE_API_KEY=$(grep -m1 "BINANCE_API_KEY" "$CONFIG_FILE" | cut -d"'" -f2 || true)
  BINANCE_API_SECRET=$(grep -m1 "BINANCE_API_SECRET" "$CONFIG_FILE" | cut -d"'" -f2 || true)
  if [[ -n "${BINANCE_API_KEY:-}" ]]; then
    export BINANCE_API_KEY
  else
    echo "[WARN] 未在 $CONFIG_FILE 中找到 BINANCE_API_KEY"
  fi
  if [[ -n "${BINANCE_API_SECRET:-}" ]]; then
    export BINANCE_API_SECRET
  else
    echo "[WARN] 未在 $CONFIG_FILE 中找到 BINANCE_API_SECRET"
  fi
else
  echo "[WARN] 未找到配置文件 $CONFIG_FILE，无法设置 Binance API 凭据"
fi
