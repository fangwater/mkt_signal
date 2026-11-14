#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
[[ -f "$BASE_DIR/env.sh" ]] && source "$BASE_DIR/env.sh"
source "$SCRIPT_DIR/process_match_lib.sh"
PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PROC_NAME="${PMDAEMON_NAME:-exec_volatility_pub_binance}"
timeout -k 1s 45s "$PMDAEMON_BIN" delete "$PROC_NAME" >/dev/null 2>&1 || true
mapfile -t leaked < <(safe_find_running_pids exec_volatility_pub "$BASE_DIR" 'config/exec_volatility_pub.toml' || true)
[[ ${#leaked[@]} -eq 0 ]] || kill "${leaked[@]}" >/dev/null 2>&1 || true
echo "[INFO] stopped $PROC_NAME"
