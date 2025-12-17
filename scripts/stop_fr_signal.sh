#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "[WARN] stop_fr_signal.sh 已废弃，请使用 stop_trade_signal.sh"
exec "${SCRIPT_DIR}/stop_trade_signal.sh" "$@"
