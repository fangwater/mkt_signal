#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "[WARN] start_fr_signal.sh 已废弃，请使用 start_trade_signal.sh"
exec "${SCRIPT_DIR}/start_trade_signal.sh" "$@"
